#include "dir.h"
#include "tests.h"
#include "arki/metadata/tests.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <fcntl.h>
#include <sstream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

const char* relpath = "testfile.grib";

class TestInternals : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_segment_dir_internals");

template<class Segment, class Data>
class Tests : public SegmentTests<Segment, Data>
{
    using SegmentTests<Segment, Data>::SegmentTests;
    typedef typename SegmentTests<Segment, Data>::Fixture Fixture;
    void register_tests() override;
};

Tests<segment::dir::Segment, GRIBData> test1("arki_segment_dir_grib");
Tests<segment::dir::Segment, BUFRData> test2("arki_segment_dir_bufr");
Tests<segment::dir::Segment, ODIMData> test3("arki_segment_dir_odim");
Tests<segment::dir::Segment, VM2Data>  test4("arki_segment_dir_vm2");
Tests<segment::dir::Segment, NCData>  test5("arki_segment_dir_nc");
Tests<segment::dir::Segment, JPEGData>  test6("arki_segment_dir_jpeg");

/**
 * Create a writer
 * return the data::Writer so that we manage the writer lifetime, but also
 * the underlying implementation so we can test it.
 */
std::shared_ptr<segment::dir::Writer> make_w()
{
    segment::WriterConfig writer_config;
    string abspath = sys::abspath(relpath);
    return std::shared_ptr<segment::dir::Writer>(new segment::dir::Writer(writer_config, "grib", sys::getcwd(), relpath, abspath));
}


void TestInternals::register_tests() {

// Scan a well-known sample
add_method("scanner", [] {
    segment::dir::Scanner scanner("odimh5", "inbound/fixture.odimh5");
    scanner.list_files();
    wassert(actual(scanner.on_disk.size()) == 3u);
    wassert(actual(scanner.max_sequence) == 2u);

    auto reader = Segment::detect_reader("odimh5", std::filesystem::current_path(), "inbound/fixture.odimh5", sys::abspath("inbound/fixture.odimh5"), make_shared<core::lock::Null>());

    metadata::Collection mds;
    scanner.scan(reader, mds.inserter_func());
    wassert(actual(mds.size()) == 3u);

    // Check the source info
    wassert(actual(mds[0].source().cloneType()).is_source_blob("odimh5", std::filesystem::current_path(), "inbound/fixture.odimh5", 0, 49057));
});

}


template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

this->add_method("create_last_sequence", [](Fixture& f) {
    std::shared_ptr<segment::Checker> checker = f.create();
    segment::SequenceFile seq(checker->segment().abspath);
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
    if (sys::isdir(relpath))
        sys::rmtree_ifexists(relpath);
    else
        sys::unlink_ifexists(relpath);
    metadata::TestCollection mdc("inbound/test.grib1");
    wassert(actual_file(relpath).not_exists());
    {
        auto w(make_w());

        // It should exist but be empty
        //wassert(actual(fname).fileexists());
        //wassert(actual(sys::size(fname)) == 0u);

        // Try a successful transaction
        {
            Metadata& md = mdc[0];

            // Make a snapshot of everything before appending
            unique_ptr<types::Source> orig_source(md.source().clone());
            size_t data_size = md.data_size();

            // Start the append transaction, the file is written
            const types::source::Blob& new_source = w->append(md);
            wassert(actual((size_t)new_source.offset) == 0u);
            wassert(actual(sys::size(str::joinpath(w->segment().abspath, "000000.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Commit
            w->commit();

            // After commit, metadata is updated
            wassert(actual_type(md.source()).is_source_blob("grib", sys::getcwd(), w->segment().relpath, 0, data_size));
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
            wassert(actual(sys::size(str::joinpath(w->segment().abspath, "000001.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Rollback
            w->rollback();

            // After rollback, the file has been deleted
            wassert(actual(sys::exists(str::joinpath(w->segment().abspath, "000001.grib"))).isfalse());
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
            wassert(actual(sys::size(str::joinpath(w->segment().abspath, "000002.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Commit
            w->commit();

            // After commit, metadata is updated
            wassert(actual_type(md.source()).is_source_blob("grib", sys::getcwd(), w->segment().relpath, 2, data_size));
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

// Check behaviour of an empty directory (#279)
this->add_method("empty_dir", [](Fixture& f) {
    if (sys::isdir(relpath))
        sys::rmtree_ifexists(relpath);
    else
        sys::unlink_ifexists(relpath);

    sys::makedirs(relpath);

    // It can be read as an empty segment
    {
        metadata::TestCollection mdc1;
        wassert(mdc1.scan_from_file(relpath, false));
        wassert(actual(mdc1.size()) == 0u);
    }

    // Verify what are the results of check
    {
        auto checker = Segment::detect_checker(f.td.format, ".", relpath, sys::abspath(relpath));
        wassert(actual(checker->size()) == 0u);
        wassert_false(checker->exists_on_disk());
        wassert_false(checker->is_empty());
    }
});

}

}
#include "tests.tcc"
