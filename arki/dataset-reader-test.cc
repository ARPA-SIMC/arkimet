#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/metadata/collection.h"
#include "arki/summary.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/configfile.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <sys/fcntl.h>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

namespace {

template<class Data>
struct FixtureReader : public DatasetTest
{
    using DatasetTest::DatasetTest;

    Data td;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            step = daily
            unique = product, area, reftime
        )");
        if (td.format == "vm2")
            cfg.setValue("smallfiles", "true");
        import_all_packed(td);
    }

    bool smallfiles() const
    {
        return ConfigFile::boolValue(cfg.value("smallfiles")) ||
            (td.format == "vm2" && cfg.value("type") == "simple");
    }
};


template<class Data>
class TestsReader : public FixtureTestCase<FixtureReader<Data>>
{
    using FixtureTestCase<FixtureReader<Data>>::FixtureTestCase;

    void register_tests() override;
};

TestsReader<testdata::GRIBData> test_reader_grib_ondisk2("arki_dataset_reader_grib_ondisk2", "type=ondisk2\n");
TestsReader<testdata::GRIBData> test_reader_grib_ondisk2_sharded("arki_dataset_reader_grib_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsReader<testdata::GRIBData> test_reader_grib_simple_plain("arki_dataset_reader_grib_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<testdata::GRIBData> test_reader_grib_simple_plain_sharded("arki_dataset_reader_grib_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsReader<testdata::GRIBData> test_reader_grib_simple_sqlite("arki_dataset_reader_grib_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsReader<testdata::GRIBData> test_reader_grib_iseg("arki_dataset_reader_grib_iseg", "type=iseg\nformat=grib\n");
TestsReader<testdata::BUFRData> test_reader_bufr_ondisk2("arki_dataset_reader_bufr_ondisk2", "type=ondisk2\n");
TestsReader<testdata::BUFRData> test_reader_bufr_ondisk2_sharded("arki_dataset_reader_bufr_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsReader<testdata::BUFRData> test_reader_bufr_simple_plain("arki_dataset_reader_bufr_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<testdata::BUFRData> test_reader_bufr_simple_plain_sharded("arki_dataset_reader_bufr_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsReader<testdata::BUFRData> test_reader_bufr_simple_sqlite("arki_dataset_reader_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsReader<testdata::BUFRData> test_reader_bufr_iseg("arki_dataset_reader_bufr_iseg", "type=iseg\nformat=bufr\n");
TestsReader<testdata::VM2Data> test_reader_vm2_ondisk2("arki_dataset_reader_vm2_ondisk2", "type=ondisk2\n");
TestsReader<testdata::VM2Data> test_reader_vm2_ondisk2_sharded("arki_dataset_reader_vm2_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsReader<testdata::VM2Data> test_reader_vm2_simple_plain("arki_dataset_reader_vm2_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<testdata::VM2Data> test_reader_vm2_simple_plain_sharded("arki_dataset_reader_vm2_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsReader<testdata::VM2Data> test_reader_vm2_simple_sqlite("arki_dataset_reader_vm2_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsReader<testdata::VM2Data> test_reader_vm2_iseg("arki_dataset_reader_vm2_iseg", "type=iseg\nformat=vm2\n");
TestsReader<testdata::ODIMData> test_reader_odim_ondisk2("arki_dataset_reader_odim_ondisk2", "type=ondisk2\n");
TestsReader<testdata::ODIMData> test_reader_odim_ondisk2_sharded("arki_dataset_reader_odim_ondisk2", "type=ondisk2\nshard=yearly\n");
TestsReader<testdata::ODIMData> test_reader_odim_simple_plain("arki_dataset_reader_odim_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<testdata::ODIMData> test_reader_odim_simple_plain_sharded("arki_dataset_reader_odilym_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsReader<testdata::ODIMData> test_reader_odim_simple_sqlite("arki_dataset_reader_odim_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsReader<testdata::ODIMData> test_reader_odim_iseg("arki_dataset_reader_odim_iseg", "type=iseg\nformat=odimh5\n");

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

        // Check that what we imported can be queried
        metadata::Collection mdc(*ds, f.td.test_data[i].matcher);
        wassert(actual(mdc.size()) == 1u);

        // Check that the result matches what was imported
        const source::Blob& s1 = f.td.test_data[i].md.sourceBlob();
        const source::Blob& s2 = mdc[0].sourceBlob();
        wassert(actual(s2.format) == s1.format);
        wassert(actual(s2.size) == s1.size);
        wassert(actual(mdc[0]).is_similar(f.td.test_data[i].md));

        wassert(actual(f.td.test_data[i].matcher(mdc[0])).istrue());

        // Check that the data can be loaded
        const auto& data = mdc[0].getData();
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
        ds->query_summary(f.td.test_data[i].matcher, s);

        wassert(actual(s.count()) == 1u);

        const source::Blob& s1 = f.td.test_data[i].md.sourceBlob();
        wassert(actual(s.size()) == s1.size);
    }
});

this->add_method("querybytes", [](Fixture& f) {
    auto ds(f.config().create_reader());

    for (unsigned i = 0; i < 3; ++i)
    {
        using namespace arki::types;

        // Query into a file
        dataset::ByteQuery bq;
        bq.setData(f.td.test_data[i].matcher);
        sys::File out("testdata", O_WRONLY | O_CREAT | O_TRUNC);
        ds->query_bytes(bq, out);
        out.close();

        // Rescan the file
        metadata::Collection tmp;
        wassert(actual(scan::scan("testdata", tmp.inserter_func(), f.td.test_data[i].md.source().format)).istrue());

        // Ensure that what we rescanned is what was imported
        wassert(actual(tmp.size()) == 1u);
        wassert(actual(tmp[0]).is_similar(f.td.test_data[i].md));

        //UItem<source::Blob> s1 = input_data[i].source.upcast<source::Blob>();
        //wassert(actual(os.str().size()) == s1->size);
    }
});

this->add_method("query_data", [](Fixture& f) {
    // Test querying with data only
    f.clean_and_import();
    auto reader(f.config().create_reader());

    sys::File out(sys::File::mkstemp("test"));
    dataset::ByteQuery bq;
    bq.setData(Matcher::parse("origin:GRIB1,200"));
    reader->query_bytes(bq, out);

    string res = sys::read_file(out.name());
    ensure_equals(res.substr(0, 4), "GRIB");
});

this->add_method("query_inline", [](Fixture& f) {
    // Test querying with inline data
    using namespace arki::types;

    f.clean_and_import();
    auto reader(f.config().create_reader());

    metadata::Collection mdc(*reader, Matcher::parse("origin:GRIB1,200"));
    ensure_equals(mdc.size(), 1u);

    // Check that the source record that comes out is ok
    //wassert(actual_type(mdc[0].source()).is_source_inline("grib1", 7218));

    // Check that data is accessible
    const auto& buf = mdc[0].getData();
    ensure_equals(buf.size(), 7218u);

    mdc.clear();
    mdc.add(*reader, Matcher::parse("origin:GRIB1,80"));
    ensure_equals(mdc.size(), 1u);

    mdc.clear();
    mdc.add(*reader, Matcher::parse("origin:GRIB1,98"));
    ensure_equals(mdc.size(), 1u);
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
        total_size += f.td.test_data[i].md.sourceBlob().size;
    // We use >= and not == because some data sources add extra information
    // to data, like line endings for VM2
    wassert(actual(sys::size(out.name())) >= total_size);

    // Check that they can be scanned again
    metadata::Collection mdc;
    wassert(actual(scan::scan("tempdata", mdc.inserter_func(), f.td.format)).istrue());

    sys::unlink("tempdata");
});

this->add_method("postprocess", [](Fixture& f) {
    auto ds(f.config().create_reader());

    // Do a simple export first, to get the exact metadata that would come
    // out
    metadata::Collection mdc(*ds, f.td.test_data[0].matcher);
    wassert(actual(mdc.size()) == 1u);

    // Then do a postprocessed query_bytes

    dataset::ByteQuery bq;
    bq.setPostprocess(f.td.test_data[0].matcher, "testcountbytes");
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

}

}

