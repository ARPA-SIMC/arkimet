#include "arki/core/file.h"
#include "arki/dataset.h"
#include "arki/dataset/session.h"
#include "arki/dataset/tests.h"
#include "arki/matcher/parser.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/data.h"
#include "arki/nag.h"
#include "arki/query.h"
#include "arki/query/progress.h"
#include "arki/scan.h"
#include "arki/scan/validator.h"
#include "arki/stream.h"
#include "arki/summary.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

template <class Data> struct FixtureReader : public DatasetTest
{
    using DatasetTest::DatasetTest;

    Data td;
    const vector<std::string> matchers = {
        "reftime:=2007-07-08", "reftime:=2007-07-07", "reftime:=2007-10-09"};

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            step = daily
            unique = product, area, reftime
        )");
        if (td.format == DataFormat::VM2)
            cfg->set("smallfiles", "true");
        import_all_packed(td.mds);
    }

    bool smallfiles() const
    {
        return cfg->value_bool("smallfiles") ||
               (td.format == DataFormat::VM2 && cfg->value("type") == "simple");
    }
};

template <class Data>
class TestsReader : public FixtureTestCase<FixtureReader<Data>>
{
    using FixtureTestCase<FixtureReader<Data>>::FixtureTestCase;

    void register_tests() override;
};

TestsReader<GRIBData> test_reader_grib_simple("arki_dataset_reader_grib_simple",
                                              "type=simple");
TestsReader<GRIBData> test_reader_grib_iseg("arki_dataset_reader_grib_iseg",
                                            "type=iseg\nformat=grib\n");
TestsReader<BUFRData> test_reader_bufr_simple("arki_dataset_reader_bufr_simple",
                                              "type=simple");
TestsReader<BUFRData> test_reader_bufr_iseg("arki_dataset_reader_bufr_iseg",
                                            "type=iseg\nformat=bufr\n");
TestsReader<VM2Data> test_reader_vm2_simple("arki_dataset_reader_vm2_simple",
                                            "type=simple");
TestsReader<VM2Data> test_reader_vm2_iseg("arki_dataset_reader_vm2_iseg",
                                          "type=iseg\nformat=vm2\n");
TestsReader<ODIMData> test_reader_odim_simple("arki_dataset_reader_odim_simple",
                                              "type=simple");
TestsReader<ODIMData> test_reader_odim_iseg("arki_dataset_reader_odim_iseg",
                                            "type=iseg\nformat=odimh5\n");
TestsReader<NCData> test_reader_nc_simple("arki_dataset_reader_nc_simple",
                                          "type=simple");
TestsReader<NCData> test_reader_nc_iseg("arki_dataset_reader_nc_iseg",
                                        "type=iseg\nformat=nc\n");
TestsReader<JPEGData> test_reader_jpeg_simple("arki_dataset_reader_jpeg_simple",
                                              "type=simple");
TestsReader<JPEGData> test_reader_jpeg_iseg("arki_dataset_reader_jpeg_iseg",
                                            "type=iseg\nformat=jpeg\n");

template <class Data> void TestsReader<Data>::register_tests()
{

    typedef FixtureReader<Data> Fixture;

    this->add_method("querydata", [](Fixture& f) {
        matcher::Parser parser;
        auto ds(f.config().create_reader());

        acct::plain_data_read_count.reset();

        // Query everything, we should get 3 results
        metadata::Collection mdc(*ds, Matcher());
        wassert(actual(mdc.size()) == 3u);

        // No data should have been read in this query
        wassert(actual(acct::plain_data_read_count.val()) == 0u);

        for (unsigned i = 0; i < 3; ++i)
        {
            using namespace arki::types;
            Matcher matcher = parser.parse(f.matchers[i]);

            // Check that what we imported can be queried
            metadata::Collection mdc(*ds, query::Data(matcher, true));
            wassert(actual(mdc.size()) == 1u);

            // Check that the result matches what was imported
            const source::Blob& s1 = f.td.mds[i].sourceBlob();
            const source::Blob& s2 = mdc[0].sourceBlob();
            wassert(actual(s2.format) == s1.format);
            wassert(actual(s2.size) == s1.size);
            wassert(actual(mdc[0]).is_similar(f.td.mds[i]));

            wassert(actual(matcher(mdc[0])).istrue());

            // Check that the data can be loaded
            const auto& data = mdc[0].get_data().read();
            wassert(actual(data.size()) == s1.size);
        }

        // 3 data items should have been read
        if (f.smallfiles())
            wassert(actual(acct::plain_data_read_count.val()) == 0u);
        else
            wassert(actual(acct::plain_data_read_count.val()) == 3u);
    });

    this->add_method("querysummary", [](Fixture& f) {
        auto ds(f.config().create_reader());

        // Query summary of everything
        {
            Summary s;
            ds->query_summary(Matcher(), s);
            wassert(actual(s.count()) == 3u);
        }

        for (unsigned i = 0; i < 3; ++i)
        {
            using namespace arki::types;

            // Query summary of single items
            Summary s;
            ds->query_summary(f.matchers[i], s);

            wassert(actual(s.count()) == 1u);

            const source::Blob& s1 = f.td.mds[i].sourceBlob();
            wassert(actual(s.size()) == s1.size);
        }
    });

    this->add_method("querybytes", [](Fixture& f) {
        matcher::Parser parser;
        auto ds(f.config().create_reader());

        for (unsigned i = 0; i < 3; ++i)
        {
            using namespace arki::types;
            Matcher matcher = parser.parse(f.matchers[i]);

            // Query into a file
            query::Bytes bq;
            bq.setData(matcher);
            auto out = std::make_shared<sys::File>(
                "testdata", O_WRONLY | O_CREAT | O_TRUNC);
            auto strm = StreamOutput::create(out);
            ds->query_bytes(bq, *strm);
            out->close();

            // Rescan the file
            metadata::TestCollection tmp;
            wassert(tmp.scan_from_file("testdata", f.td.mds[i].source().format,
                                       false));

            // Ensure that what we rescanned is what was imported
            wassert(actual(tmp.size()) == 1u);
            wassert(actual(tmp[0]).is_similar(f.td.mds[i]));

            // UItem<source::Blob> s1 =
            // input_data[i].source.upcast<source::Blob>();
            // wassert(actual(os.str().size()) == s1->size);
        }
    });

    this->add_method("query_data", [](Fixture& f) {
        matcher::Parser parser;
        // Test querying with data only
        auto reader(f.config().create_reader());
        Matcher matcher = parser.parse(f.matchers[1]);

        std::vector<uint8_t> buf;
        auto out = StreamOutput::create(buf);
        query::Bytes bq;
        bq.setData(matcher);
        reader->query_bytes(bq, *out);

        std::vector<uint8_t> data = f.td.mds[1].get_data().read();
        // Add a newline in case of VM2, because get_data() gives us the minimal
        // VM2 without newline
        if (f.td.format == DataFormat::VM2)
            data.emplace_back('\n');
        wassert(actual(buf) == data);
    });

    this->add_method("query_inline", [](Fixture& f) {
        // Test querying with inline data
        using namespace arki::types;
        matcher::Parser parser;
        Matcher matcher = parser.parse(f.matchers[0]);

        auto reader(f.config().create_reader());

        metadata::Collection mdc(*reader, query::Data(matcher, true));
        wassert(actual(mdc.size()) == 1u);

        // Check that the source record that comes out is ok
        // wassert(actual_type(mdc[0].source()).is_source_inline("grib1",
        // 7218));

        // Check that data is accessible
        const auto& buf = mdc[0].get_data().read();
        wassert(actual(buf.size()) == f.td.mds[0].sourceBlob().size);

        mdc.clear();
        mdc.add(*reader, f.matchers[1]);
        wassert(actual(mdc.size()) == 1u);

        mdc.clear();
        mdc.add(*reader, f.matchers[2]);
        wassert(actual(mdc.size()) == 1u);
    });

    this->add_method("querybytes_integrity", [](Fixture& f) {
        auto ds(f.config().create_reader());

        // Query everything
        query::Bytes bq;
        bq.setData(Matcher());
        auto out  = std::make_shared<sys::File>("tempdata",
                                                O_WRONLY | O_CREAT | O_TRUNC);
        auto strm = StreamOutput::create(out);
        ds->query_bytes(bq, *strm);
        out->close();

        // Check that what we got matches the total size of what we imported
        size_t total_size = 0;
        for (unsigned i = 0; i < 3; ++i)
            total_size += f.td.mds[i].sourceBlob().size;
        // We use >= and not == because some data sources add extra information
        // to data, like line endings for VM2
        wassert(actual(sys::size(out->path())) >= total_size);

        // Check that they can be scanned again
        // Read chunks from tempdata and scan them individually, to allow
        // scanning formats like ODIM that do not support concatenation
        unsigned padding = 0;
        if (f.td.format == DataFormat::VM2)
            padding = 1;
        std::vector<uint8_t> buf1(f.td.mds[1].sourceBlob().size + padding);
        std::vector<uint8_t> buf2(f.td.mds[0].sourceBlob().size + padding);
        std::vector<uint8_t> buf3(f.td.mds[2].sourceBlob().size + padding);

        sys::File in("tempdata", O_RDONLY);
        wassert(actual(in.pread(buf1.data(), buf1.size(), 0)) == buf1.size());
        wassert(actual(in.pread(buf2.data(), buf2.size(), buf1.size())) ==
                buf2.size());
        wassert(actual(in.pread(buf3.data(), buf3.size(),
                                buf1.size() + buf2.size())) == buf3.size());
        in.close();

        const auto& validator = scan::Scanner::get_validator(f.td.format);
        wassert(validator.validate_buf(buf1.data(), buf1.size()));
        wassert(validator.validate_buf(buf2.data(), buf2.size()));
        wassert(validator.validate_buf(buf3.data(), buf3.size()));

        auto scanner = scan::Scanner::get_scanner(f.td.format);
        wassert(scanner->scan_data(buf1));
        wassert(scanner->scan_data(buf2));
        wassert(scanner->scan_data(buf3));

        sys::unlink("tempdata");
    });

    this->add_method("postprocess", [](Fixture& f) {
        auto ds(f.config().create_reader());
        matcher::Parser parser;
        Matcher matcher = parser.parse(f.matchers[0]);

        // Do a simple export first, to get the exact metadata that would come
        // out
        metadata::Collection mdc(*ds, query::Data(matcher, true));
        wassert(actual(mdc.size()) == 1u);

        // Then do a postprocessed query_bytes

        query::Bytes bq;
        bq.setPostprocess(matcher, "testcountbytes");
        auto strm = StreamOutput::create(std::make_shared<Stderr>());
        ds->query_bytes(bq, *strm);

        // Verify that the data that was output was exactly as long as the
        // encoded metadata and its data
        string out = sys::read_file("testcountbytes.out");
        auto copy(mdc[0].clone());
        copy->makeInline();
        char buf[32];
        snprintf(buf, 32, "%zu\n",
                 copy->encodeBinary().size() + copy->data_size());
        wassert(actual(out) == buf);
    });

    this->add_method("locked", [](Fixture& f) {
        // Lock a dataset for writing
        auto wds(f.config().create_writer());
        Pending p = wds->test_writelock();

        // Try to read from it, it should still work with WAL
        auto rds(f.config().create_reader());
        query::Bytes bq;
        bq.setData(Matcher());
        auto out = StreamOutput::create_discard();
        rds->query_bytes(bq, *out);
    });

    this->add_method("interrupted_read", [](Fixture& f) {
        auto orig_data = f.td.mds[1].get_data().read();

        unsigned count = 0;
        auto reader    = f.dataset_config()->create_reader();
        reader->query_data(query::Data(Matcher(), true),
                           [&](std::shared_ptr<Metadata> md) {
                               auto data = md->get_data().read();
                               wassert(actual(data) == orig_data);
                               ++count;
                               return false;
                           });

        wassert(actual(count) == 1u);
    });

    this->add_method("read_missing_segment", [](Fixture& f) {
        // Delete a segment, leaving it in the index
        auto segment = f.segment_session()->segment_from_relpath_and_format(
            f.import_results[0].sourceBlob().filename, f.td.format);
        f.segment_session()->segment_data_checker(segment)->remove();

        unsigned count_ok = 0;
        auto reader       = f.dataset_config()->create_reader();
        {
            nag::CollectHandler nag_messages(false, false);
            nag_messages.install();
            reader->query_data(query::Data(Matcher(), true),
                               [&](std::shared_ptr<Metadata> md) {
                                   md->get_data().read();
                                   ++count_ok;
                                   return true;
                               });
            wassert(actual(nag_messages.collected.size()) == 1u);
            wassert(actual(nag_messages.collected[0])
                        .matches("W:.+2007/07-08\\.\\w+: segment data is not "
                                 "available"));
            nag_messages.collected.clear();
        }
        wassert(actual(count_ok) == 2u);
    });

    this->add_method("issue116", [](Fixture& f) {
        unsigned count = 0;
        auto reader    = f.dataset_config()->create_reader();
        reader->query_data("reftime:==13:00",
                           [&](std::shared_ptr<Metadata>) noexcept {
                               ++count;
                               return true;
                           });
        wassert(actual(count) == 1u);
    });

    this->add_method("issue213_manyquery", [](Fixture& f) {
        matcher::Parser parser;
        query::Data dq(parser.parse("reftime:==13:00"), true);
        auto reader = f.dataset_config()->create_reader();
        metadata::Collection coll;
        for (unsigned i = 0; i < 2000; ++i)
            reader->query_data(dq, coll.inserter_func());
    });

    this->add_method("issue213_manyds", [](Fixture& f) {
        matcher::Parser parser;
        query::Data dq(parser.parse("reftime:==13:00"), true);
        metadata::Collection coll;
        for (unsigned i = 0; i < 2000; ++i)
        {
            auto reader = f.dataset_config()->create_reader();
            reader->query_data(dq, coll.inserter_func());
        }
    });

    this->add_method("issue215", [](Fixture& f) {
        unsigned count = 0;
        auto reader    = f.dataset_config()->create_reader();
        reader->query_data("reftime:;area:GRIB: or VM2:",
                           [&](std::shared_ptr<Metadata>) noexcept {
                               ++count;
                               return true;
                           });
        wassert(actual(count) == 3u);
    });

    this->add_method("progress", [](Fixture& f) {
        auto reader = f.dataset_config()->create_reader();

        struct TestProgress : public query::Progress
        {
            using query::Progress::count;
            using query::Progress::bytes;
            unsigned start_called  = 0;
            unsigned update_called = 0;
            unsigned done_called   = 0;

            void start(size_t expected_count = 0,
                       size_t expected_bytes = 0) override
            {
                query::Progress::start(expected_count, expected_bytes);
                ++start_called;
            }
            void update(size_t count, size_t bytes) override
            {
                query::Progress::update(count, bytes);
                ++update_called;
            }
            void done() override
            {
                query::Progress::done();
                ++done_called;
            }
        };
        auto progress = make_shared<TestProgress>();

        query::Data dq;
        dq.progress  = progress;
        size_t count = 0;
        reader->query_data(dq, [&](std::shared_ptr<Metadata>) noexcept {
            ++count;
            return true;
        });
        wassert(actual(count) == 3u);
        wassert(actual(progress->count) == 3u);
        wassert(actual(progress->bytes) > 90u);
        wassert(actual(progress->start_called) == 1u);
        wassert(actual(progress->update_called) == 3u);
        wassert(actual(progress->done_called) == 1u);

        progress = make_shared<TestProgress>();
        query::Bytes bq;
        bq.progress = progress;
        auto out    = StreamOutput::create_discard();
        reader->query_bytes(bq, *out);
        wassert(actual(progress->count) == 3u);
        wassert(actual(progress->bytes) > 90u);
        wassert(actual(progress->start_called) == 1u);
        wassert(actual(progress->update_called) == 3u);
        wassert(actual(progress->done_called) == 1u);

        struct TestProgressThrowing : public TestProgress
        {
            void update(size_t count, size_t bytes) override
            {
                TestProgress::update(count, bytes);
                throw std::runtime_error("Expected error");
            }
        };

        auto progress1 = make_shared<TestProgressThrowing>();
        dq.progress    = progress1;
        count          = 0;
        auto e         = wassert_throws(
            std::runtime_error,
            reader->query_data(dq, [&](std::shared_ptr<Metadata>) noexcept {
                ++count;
                return true;
            }));
        wassert(actual(e.what()) = "Expected error");

        progress1   = make_shared<TestProgressThrowing>();
        bq.progress = progress1;
        e = wassert_throws(std::runtime_error, reader->query_bytes(bq, *out));
        wassert(actual(e.what()) = "Expected error");
    });
}

} // namespace
