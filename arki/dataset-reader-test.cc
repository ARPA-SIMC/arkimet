#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/session.h"
#include "arki/core/file.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/summary.h"
#include "arki/scan.h"
#include "arki/scan/validator.h"
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

template<class Data>
struct FixtureReader : public DatasetTest
{
    using DatasetTest::DatasetTest;

    Data td;
    const vector<std::string> matchers = { "reftime:=2007-07-08", "reftime:=2007-07-07", "reftime:=2007-10-09" };

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            step = daily
            unique = product, area, reftime
        )");
        if (td.format == "vm2")
            cfg.set("smallfiles", "true");
        import_all_packed(td.mds);
    }

    bool smallfiles() const
    {
        return cfg.value_bool("smallfiles") || (td.format == "vm2" && cfg.value("type") == "simple");
    }
};


template<class Data>
class TestsReader : public FixtureTestCase<FixtureReader<Data>>
{
    using FixtureTestCase<FixtureReader<Data>>::FixtureTestCase;

    void register_tests() override;
};

TestsReader<GRIBData> test_reader_grib_ondisk2("arki_dataset_reader_grib_ondisk2", "type=ondisk2\n");
TestsReader<GRIBData> test_reader_grib_simple_plain("arki_dataset_reader_grib_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<GRIBData> test_reader_grib_simple_sqlite("arki_dataset_reader_grib_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsReader<GRIBData> test_reader_grib_iseg("arki_dataset_reader_grib_iseg", "type=iseg\nformat=grib\n");
TestsReader<BUFRData> test_reader_bufr_ondisk2("arki_dataset_reader_bufr_ondisk2", "type=ondisk2\n");
TestsReader<BUFRData> test_reader_bufr_simple_plain("arki_dataset_reader_bufr_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<BUFRData> test_reader_bufr_simple_sqlite("arki_dataset_reader_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsReader<BUFRData> test_reader_bufr_iseg("arki_dataset_reader_bufr_iseg", "type=iseg\nformat=bufr\n");
TestsReader<VM2Data> test_reader_vm2_ondisk2("arki_dataset_reader_vm2_ondisk2", "type=ondisk2\n");
TestsReader<VM2Data> test_reader_vm2_simple_plain("arki_dataset_reader_vm2_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<VM2Data> test_reader_vm2_simple_sqlite("arki_dataset_reader_vm2_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsReader<VM2Data> test_reader_vm2_iseg("arki_dataset_reader_vm2_iseg", "type=iseg\nformat=vm2\n");
TestsReader<ODIMData> test_reader_odim_ondisk2("arki_dataset_reader_odim_ondisk2", "type=ondisk2\n");
TestsReader<ODIMData> test_reader_odim_simple_plain("arki_dataset_reader_odim_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<ODIMData> test_reader_odim_simple_sqlite("arki_dataset_reader_odim_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsReader<ODIMData> test_reader_odim_iseg("arki_dataset_reader_odim_iseg", "type=iseg\nformat=odimh5\n");

template<class Data>
void TestsReader<Data>::register_tests() {

typedef FixtureReader<Data> Fixture;

this->add_method("querydata", [](Fixture& f) {
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
        Matcher matcher = Matcher::parse(f.matchers[i]);

        // Check that what we imported can be queried
        metadata::Collection mdc(*ds, dataset::DataQuery(matcher, true));
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
        Matcher matcher = Matcher::parse(f.matchers[i]);

        // Query summary of single items
        Summary s;
        ds->query_summary(matcher, s);

        wassert(actual(s.count()) == 1u);

        const source::Blob& s1 = f.td.mds[i].sourceBlob();
        wassert(actual(s.size()) == s1.size);
    }
});

this->add_method("querybytes", [](Fixture& f) {
    auto ds(f.config().create_reader());

    for (unsigned i = 0; i < 3; ++i)
    {
        using namespace arki::types;
        Matcher matcher = Matcher::parse(f.matchers[i]);

        // Query into a file
        dataset::ByteQuery bq;
        bq.setData(matcher);
        sys::File out("testdata", O_WRONLY | O_CREAT | O_TRUNC);
        ds->query_bytes(bq, out);
        out.close();

        // Rescan the file
        metadata::TestCollection tmp;
        wassert(tmp.scan_from_file("testdata", f.td.mds[i].source().format, false));

        // Ensure that what we rescanned is what was imported
        wassert(actual(tmp.size()) == 1u);
        wassert(actual(tmp[0]).is_similar(f.td.mds[i]));

        //UItem<source::Blob> s1 = input_data[i].source.upcast<source::Blob>();
        //wassert(actual(os.str().size()) == s1->size);
    }
});

this->add_method("query_data", [](Fixture& f) {
    // Test querying with data only
    auto reader(f.config().create_reader());
    Matcher matcher = Matcher::parse(f.matchers[1]);

    sys::File out(sys::File::mkstemp("test"));
    dataset::ByteQuery bq;
    bq.setData(matcher);
    reader->query_bytes(bq, out);
    out.close();

    string queried = sys::read_file(out.name());
    std::vector<uint8_t> data = f.td.mds[1].get_data().read();
    string strdata(data.begin(), data.end());
    wassert(actual(queried.substr(0, 8)) == strdata.substr(0, 8));
});

this->add_method("query_inline", [](Fixture& f) {
    // Test querying with inline data
    using namespace arki::types;
    Matcher matcher = Matcher::parse(f.matchers[0]);

    auto reader(f.config().create_reader());

    metadata::Collection mdc(*reader, dataset::DataQuery(matcher, true));
    wassert(actual(mdc.size()) == 1u);

    // Check that the source record that comes out is ok
    //wassert(actual_type(mdc[0].source()).is_source_inline("grib1", 7218));

    // Check that data is accessible
    const auto& buf = mdc[0].get_data().read();
    wassert(actual(buf.size()) == f.td.mds[0].sourceBlob().size);

    mdc.clear();
    mdc.add(*reader, Matcher::parse(f.matchers[1]));
    wassert(actual(mdc.size()) == 1u);

    mdc.clear();
    mdc.add(*reader, Matcher::parse(f.matchers[2]));
    wassert(actual(mdc.size()) == 1u);
});

this->add_method("querybytes_integrity", [](Fixture& f) {
    auto ds(f.config().create_reader());

    // Query everything
    dataset::ByteQuery bq;
    bq.setData(Matcher());
    sys::File out("tempdata", O_WRONLY | O_CREAT | O_TRUNC);
    ds->query_bytes(bq, out);
    out.close();

    // Check that what we got matches the total size of what we imported
    size_t total_size = 0;
    for (unsigned i = 0; i < 3; ++i)
        total_size += f.td.mds[i].sourceBlob().size;
    // We use >= and not == because some data sources add extra information
    // to data, like line endings for VM2
    wassert(actual(sys::size(out.name())) >= total_size);

    // Check that they can be scanned again
    // Read chunks from tempdata and scan them individually, to allow scanning
    // formats like ODIM that do not support concatenation
    unsigned padding = 0;
    if (f.td.format == "vm2")
        padding = 1;
    std::vector<uint8_t> buf1(f.td.mds[1].sourceBlob().size + padding);
    std::vector<uint8_t> buf2(f.td.mds[0].sourceBlob().size + padding);
    std::vector<uint8_t> buf3(f.td.mds[2].sourceBlob().size + padding);

    sys::File in("tempdata", O_RDONLY);
    wassert(actual(in.pread(buf1.data(), buf1.size(), 0)) == buf1.size());
    wassert(actual(in.pread(buf2.data(), buf2.size(), buf1.size())) == buf2.size());
    wassert(actual(in.pread(buf3.data(), buf3.size(), buf1.size() + buf2.size())) == buf3.size());
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
    Matcher matcher = Matcher::parse(f.matchers[0]);

    // Do a simple export first, to get the exact metadata that would come
    // out
    metadata::Collection mdc(*ds, dataset::DataQuery(matcher, true));
    wassert(actual(mdc.size()) == 1u);

    // Then do a postprocessed query_bytes

    dataset::ByteQuery bq;
    bq.setPostprocess(matcher, "testcountbytes");
    Stderr outfd;
    ds->query_bytes(bq, outfd);

    // Verify that the data that was output was exactly as long as the
    // encoded metadata and its data
    string out = sys::read_file("testcountbytes.out");
    unique_ptr<Metadata> copy(mdc[0].clone());
    copy->makeInline();
    char buf[32];
    snprintf(buf, 32, "%zd\n", copy->encodeBinary().size() + copy->data_size());
    wassert(actual(out) == buf);
});

this->add_method("locked", [](Fixture& f) {
    // Lock a dataset for writing
    auto wds(f.config().create_writer());
    Pending p = wds->test_writelock();

    // Try to read from it, it should still work with WAL
    auto rds(f.config().create_reader());
    dataset::ByteQuery bq;
    bq.setData(Matcher());
    sys::File out("/dev/null", O_WRONLY);
    rds->query_bytes(bq, out);
});

this->add_method("interrupted_read", [](Fixture& f) {
    auto orig_data = f.td.mds[1].get_data().read();

    unsigned count = 0;
    auto reader = f.dataset_config()->create_reader();
    reader->query_data(dataset::DataQuery("", true), [&](std::shared_ptr<Metadata> md) {
        auto data = md->get_data().read();
        wassert(actual(data) == orig_data);
        ++count;
        return false;
    });

    wassert(actual(count) == 1u);
});

this->add_method("read_missing_segment", [](Fixture& f) {
    // Delete a segment, leaving it in the index
    f.session->segment_checker(f.td.format, f.local_config()->path, f.import_results[0].sourceBlob().filename)->remove();

    unsigned count_ok = 0;
    unsigned count_err = 0;
    auto reader = f.dataset_config()->create_reader();
    reader->query_data(dataset::DataQuery("", true), [&](std::shared_ptr<Metadata> md) {
        try {
            md->get_data().read();
            ++count_ok;
        } catch (std::runtime_error& e) {
            wassert(actual(e.what()).contains("the segment has disappeared"));
            ++count_err;
        }
        return true;
    });

    if (f.smallfiles())
    {
        wassert(actual(count_ok) == 3u);
        wassert(actual(count_err) == 0u);
    } else {
        wassert(actual(count_ok) == 2u);
        wassert(actual(count_err) == 1u);
    }
});

this->add_method("issue116", [](Fixture& f) {
    unsigned count = 0;
    auto reader = f.dataset_config()->create_reader();
    reader->query_data(dataset::DataQuery("reftime:==13:00"), [&](std::shared_ptr<Metadata> md) { ++count; return true; });
    wassert(actual(count) == 1u);
});

this->add_method("issue213_manyquery", [](Fixture& f) {
    auto reader = f.dataset_config()->create_reader();
    metadata::Collection coll;
    for (unsigned i = 0; i < 2000; ++i)
        reader->query_data(dataset::DataQuery("reftime:==13:00", true), coll.inserter_func());
});

this->add_method("issue213_manyds", [](Fixture& f) {
    metadata::Collection coll;
    for (unsigned i = 0; i < 2000; ++i)
    {
        auto reader = f.dataset_config()->create_reader();
        reader->query_data(dataset::DataQuery("reftime:==13:00", true), coll.inserter_func());
    }
});

#if 0
// TODO: with_data is currently ignored by all datasets except http
this->add_method("read_data_missing_segment", [](Fixture& f) {
    // Delete a segment, leaving it in the index
    f.segments().remove(f.import_results[0].sourceBlob().filename);

    unsigned count_ok = 0;
    unsigned count_err = 0;
    auto reader = f.dataset_config()->create_reader();
    reader->query_data(dataset::DataQuery(Matcher(), true), [&](unique_ptr<Metadata> md) {
        try {
            md->getData();
            ++count_ok;
        } catch (std::runtime_error& e) {
            wassert(actual(e.what()).contains("the segment has disappeared"));
            ++count_err;
        }
        return true;
    });

    wassert(actual(count_ok) == 3u);
    wassert(actual(count_err) == 0u);
});
#endif

}

}

