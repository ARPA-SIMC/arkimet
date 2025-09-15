#include "arki/core/lock.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/tests.h"
#include "arki/segment.h"
#include "arki/segment/defs.h"
#include "arki/segment/scan.h"
#include "arki/types/source/blob.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "dir.h"
#include "tests.h"
#include <fcntl.h>
#include <sstream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

std::filesystem::path relpath("testfile.grib");

class TestInternals : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_segment_dir_internals");

template <class Data, class FixtureData>
class Tests : public SegmentTests<Data, FixtureData>
{
    using SegmentTests<Data, FixtureData>::SegmentTests;
    typedef typename SegmentTests<Data, FixtureData>::Fixture Fixture;
    void register_tests() override;
};

Tests<segment::data::dir::Data, GRIBData> test1("arki_segment_data_dir_grib");
Tests<segment::data::dir::Data, BUFRData> test2("arki_segment_data_dir_bufr");
Tests<segment::data::dir::Data, ODIMData> test3("arki_segment_data_dir_odim");
Tests<segment::data::dir::Data, VM2Data> test4("arki_segment_data_dir_vm2");
Tests<segment::data::dir::Data, NCData> test5("arki_segment_data_dir_nc");
Tests<segment::data::dir::Data, JPEGData> test6("arki_segment_data_dir_jpeg");

/**
 * Create a writer
 * return the data::Writer so that we manage the writer lifetime, but also
 * the underlying implementation so we can test it.
 */
std::shared_ptr<segment::data::dir::Writer> make_w()
{
    auto session = std::make_shared<segment::scan::Session>(
        std::filesystem::current_path());
    auto segment =
        session->segment_from_relpath_and_format(relpath, DataFormat::GRIB);
    segment::WriterConfig writer_config;
    auto data = std::make_shared<segment::data::dir::Data>(segment);
    return std::make_shared<segment::data::dir::Writer>(writer_config, data);
}

void TestInternals::register_tests()
{
    // Scan a well-known sample
    add_method("scanner", [] {
        auto session = std::make_shared<segment::scan::Session>(
            std::filesystem::current_path());
        auto segment = session->segment_from_relpath("inbound/fixture.odimh5");
        segment::data::dir::Scanner scanner(*segment);
        scanner.list_files();
        wassert(actual(scanner.on_disk.size()) == 3u);
        wassert(actual(scanner.max_sequence) == 2u);

        auto reader = segment->reader(make_shared<core::lock::NullReadLock>());

        metadata::Collection mds;
        scanner.scan(reader, mds.inserter_func());
        wassert(actual(mds.size()) == 3u);

        // Check the source info
        wassert(actual(mds[0].source().cloneType())
                    .is_source_blob(DataFormat::ODIMH5,
                                    std::filesystem::current_path(),
                                    "inbound/fixture.odimh5", 0, 49057));
    });

    // Check behaviour of an empty directory (#279)
    add_method("empty_dir", [] {
        if (std::filesystem::is_directory(relpath))
            sys::rmtree_ifexists(relpath);
        else
            std::filesystem::remove(relpath);
        std::filesystem::create_directories(relpath);

        // It can be read as an empty segment
        {
            metadata::TestCollection mdc1;
            wassert(mdc1.scan_from_file(relpath, false));
            wassert(actual(mdc1.size()) == 0u);
        }

        // Verify what are the results of check
        {
            auto session = std::make_shared<segment::scan::Session>(
                std::filesystem::current_path());
            auto segment = session->segment_from_relpath_and_format(
                relpath, DataFormat::GRIB);
            auto data    = std::make_shared<segment::data::dir::Data>(segment);
            auto checker = data->checker();
            wassert(actual(checker->data().size()) == 0u);
            wassert_false(checker->data().exists_on_disk());
            wassert_false(checker->data().is_empty());
        }
    });

    // Test handling of a sequence file out of sync
    add_method("sequence_out_of_sync", [] {
        metadata::TestCollection mdc("inbound/test.grib1");

        if (std::filesystem::is_directory(relpath))
            sys::rmtree_ifexists(relpath);
        else
            std::filesystem::remove(relpath);

        // Create a dir segment
        auto session = std::make_shared<segment::scan::Session>(
            std::filesystem::current_path());
        auto segment = session->segment_from_relpath(relpath);
        auto data    = std::make_shared<segment::data::dir::Data>(segment);
        data->create_segment(mdc);

        // Set its sequence file to 0
        {
            segment::SequenceFile seq(relpath);
            seq.open();
            seq.write_sequence(0u);
        }

        // Scanner still finds all files
        {
            segment::data::dir::Scanner scanner(*segment);
            scanner.list_files();
            wassert(actual(scanner.on_disk.size()) == 3u);
            wassert(actual(scanner.max_sequence) == 2u);
        }

        // Checker reports the segment as dirty
        auto data_checker = data->checker();
        std::vector<std::string> reporter_output;
        auto check_result = wcallchecked(data_checker->check(
            [&](const std::string& s) { reporter_output.emplace_back(s); },
            mdc));
        wassert(
            actual(reporter_output) ==
            std::vector<std::string>{
                "sequence file has value 0 but found files until sequence 2"});
        wassert(actual(check_result) ==
                segment::State(segment::SEGMENT_UNALIGNED));

        // Repack the segment
        auto pending = data_checker->repack(mdc);
        pending.commit();

        // Check is now clean
        reporter_output.clear();
        check_result = wcallchecked(data_checker->check(
            [&](const std::string& s) { reporter_output.emplace_back(s); },
            mdc));
        wassert(actual(reporter_output) == std::vector<std::string>{});
        wassert(actual(check_result) == segment::State());

        // The sequence file has been fixed
        {
            segment::SequenceFile seq(relpath);
            seq.open();
            wassert(actual(seq.read_sequence()) == 2u);
        }

        // Scanner still finds all files
        {
            segment::data::dir::Scanner scanner(*segment);
            scanner.list_files();
            wassert(actual(scanner.on_disk.size()) == 3u);
            wassert(actual(scanner.max_sequence) == 2u);
        }
#if 0
        auto data_reader =
            data->reader(make_shared<core::lock::NullReadLock>());
        metadata::TestCollection scanned;
        data_reader->scan_data(scanned.inserter_func());
        wassert(actual(scanned.size()) == mdc.size());
#endif
    });
}

template <class Data, class FixtureData>
void Tests<Data, FixtureData>::register_tests()
{
    SegmentTests<Data, FixtureData>::register_tests();

    this->add_method("create_last_sequence", [](Fixture& f) {
        auto data    = f.create();
        auto checker = data->checker();
        segment::SequenceFile seq(checker->segment().abspath());
        seq.open();
        wassert(actual(seq.read_sequence()) == 2u);
    });

    this->add_method("append_last_sequence", [](Fixture& f) {
        delete_if_exists(relpath);
        wassert(actual_file(relpath).not_exists());

        // Append 3 items
        metadata::TestCollection mdc("inbound/test.grib1");
        {
            auto w(make_w());
            w->append(mdc[0]);
            w->append(mdc[1]);
            w->append(mdc[2]);
            w->commit();
        }

        segment::SequenceFile seq(relpath);
        seq.open();
        wassert(actual(seq.read_sequence()) == 2u);
    });

    // Try to append some data
    this->add_method("append", [](Fixture& f) {
        if (std::filesystem::is_directory(relpath))
            sys::rmtree_ifexists(relpath);
        else
            std::filesystem::remove(relpath);
        metadata::TestCollection mdc("inbound/test.grib1");
        wassert(actual_file(relpath).not_exists());
        {
            auto w(make_w());

            // It should exist but be empty
            // wassert(actual(fname).fileexists());
            // wassert(actual(sys::size(fname)) == 0u);

            // Try a successful transaction
            {
                Metadata& md = mdc[0];

                // Make a snapshot of everything before appending
                unique_ptr<types::Source> orig_source(md.source().clone());
                size_t data_size = md.data_size();

                // Start the append transaction, the file is written
                const types::source::Blob& new_source = w->append(md);
                wassert(actual((size_t)new_source.offset) == 0u);
                wassert(actual(sys::size(w->segment().abspath() /
                                         "000000.grib")) == data_size);
                wassert(actual_type(md.source()) == *orig_source);

                // Commit
                w->commit();

                // After commit, metadata is updated
                wassert(actual_type(md.source())
                            .is_source_blob(DataFormat::GRIB,
                                            std::filesystem::current_path(),
                                            w->segment().relpath(), 0,
                                            data_size));
            }

            // Then fail one
            {
                Metadata& md = mdc[1];

                // Make a snapshot of everything before appending
                unique_ptr<types::Source> orig_source(md.source().clone());
                size_t data_size = md.data_size();

                // Start the append transaction, the file is written
                const types::source::Blob& new_source = w->append(md);
                wassert(actual((size_t)new_source.offset) == 1u);
                wassert(actual(sys::size(w->segment().abspath() /
                                         "000001.grib")) == data_size);
                wassert(actual_type(md.source()) == *orig_source);

                // Rollback
                w->rollback();

                // After rollback, the file has been deleted
                wassert(actual(std::filesystem::exists(w->segment().abspath() /
                                                       "000001.grib"))
                            .isfalse());
                wassert(actual_type(md.source()) == *orig_source);
            }

            // Then succeed again
            {
                Metadata& md = mdc[2];

                // Make a snapshot of everything before appending
                unique_ptr<types::Source> orig_source(md.source().clone());
                size_t data_size = md.data_size();

                // Start the append transaction, the file is written
                // Rolling back a transaction does leave a gap in the sequence
                const types::source::Blob& new_source = w->append(md);
                wassert(actual((size_t)new_source.offset) == 2u);
                wassert(actual(sys::size(w->segment().abspath() /
                                         "000002.grib")) == data_size);
                wassert(actual_type(md.source()) == *orig_source);

                // Commit
                w->commit();

                // After commit, metadata is updated
                wassert(actual_type(md.source())
                            .is_source_blob(DataFormat::GRIB,
                                            std::filesystem::current_path(),
                                            w->segment().relpath(), 2,
                                            data_size));
            }
        }

        // Data writer goes out of scope, file is closed and flushed
        metadata::TestCollection mdc1;

        // Scan the file we created
        wassert(mdc1.scan_from_file(relpath, false));

        // Check that it only contains the 1st and 3rd data
        wassert(actual(mdc1.size()) == 2u);
        wassert(actual(mdc1[0]).is_similar(mdc[0]));
        wassert(actual(mdc1[1]).is_similar(mdc[2]));
    });
}

} // namespace
#include "tests.tcc"
