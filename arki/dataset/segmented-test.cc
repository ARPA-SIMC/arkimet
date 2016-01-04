#include "tests.h"
#include "segmented.h"
#include "maintenance.h"
#include "arki/types/source/blob.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/scan/any.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/types/area.h"
#include "arki/types/product.h"
#include "arki/types/value.h"
#include "arki/utils/sys.h"
#include <fcntl.h>
#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::dataset;
using namespace arki::utils;

struct Fixture : public DatasetTest
{
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            step = daily
            unique = product, area, reftime
        )");
    }
};

template<class Data>
struct FixtureReader : public Fixture
{
    using Fixture::Fixture;

    Data td;

    void test_setup()
    {
        Fixture::test_setup();
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

TestsReader<testdata::GRIBData> test_reader_grib_ondisk2("arki_dataset_segmented_reader_grib_ondisk2", "type=ondisk2\n");
TestsReader<testdata::GRIBData> test_reader_grib_simple_plain("arki_dataset_segmented_reader_grib_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<testdata::GRIBData> test_reader_grib_simple_sqlite("arki_dataset_segmented_reader_grib_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsReader<testdata::BUFRData> test_reader_bufr_ondisk2("arki_dataset_segmented_reader_bufr_ondisk2", "type=ondisk2\n");
TestsReader<testdata::BUFRData> test_reader_bufr_simple_plain("arki_dataset_segmented_reader_bufr_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<testdata::BUFRData> test_reader_bufr_simple_sqlite("arki_dataset_segmented_reader_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsReader<testdata::VM2Data> test_reader_vm2_ondisk2("arki_dataset_segmented_reader_vm2_ondisk2", "type=ondisk2\n");
TestsReader<testdata::VM2Data> test_reader_vm2_simple_plain("arki_dataset_segmented_reader_vm2_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<testdata::VM2Data> test_reader_vm2_simple_sqlite("arki_dataset_segmented_reader_vm2_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsReader<testdata::ODIMData> test_reader_odim_ondisk2("arki_dataset_segmented_reader_odim_ondisk2", "type=ondisk2\n");
TestsReader<testdata::ODIMData> test_reader_odim_simple_plain("arki_dataset_segmented_reader_odim_simple_plain", "type=simple\nindex_type=plain\n");
TestsReader<testdata::ODIMData> test_reader_odim_simple_sqlite("arki_dataset_segmented_reader_odim_simple_sqlite", "type=simple\nindex_type=sqlite");



class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test_ondisk2("arki_dataset_segmented_ondisk2", "type=ondisk2\n");
Tests test_simple_plain("arki_dataset_segmented_simple_plain", "type=simple\nindex_type=plain\n");
Tests test_simple_sqlite("arki_dataset_segmented_simple_sqlite", "type=simple\nindex_type=sqlite");

template<class Data>
void TestsReader<Data>::register_tests() {

typedef FixtureReader<Data> Fixture;

this->add_method("querydata", [](Fixture& f) {
    auto ds(f.makeReader());

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
    auto ds(f.makeReader());

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
    auto ds(f.makeReader());

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
    std::unique_ptr<Reader> reader(f.makeReader());

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
    std::unique_ptr<Reader> reader(f.makeReader());

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
    auto ds(f.makeReader());

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
    auto ds(f.makeReader());

    // Do a simple export first, to get the exact metadata that would come
    // out
    metadata::Collection mdc(*ds, f.td.test_data[0].matcher);
    wassert(actual(mdc.size()) == 1u);

    // Then do a postprocessed query_bytes

    dataset::ByteQuery bq;
    bq.setPostprocess(f.td.test_data[0].matcher, "testcountbytes");
    ds->query_bytes(bq, 2);

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
    auto wds(f.makeWriter());
    Pending p = wds->test_writelock();

    // Try to read from it, it should still work with WAL
    auto rds(f.makeReader());
    dataset::ByteQuery bq;
    bq.setData(Matcher());
    sys::File out("/dev/null", O_WRONLY);
    rds->query_bytes(bq, out);
});

}

void Tests::register_tests() {

add_method("import", [](Fixture& f) {
    testdata::GRIBData td;
    auto ds(dataset::Writer::create(f.cfg));

    for (unsigned i = 0; i < 3; ++i)
    {
        Metadata md = td.test_data[i].md;
        wassert(actual(ds->acquire(md)) == dataset::Writer::ACQ_OK);
        wassert(actual_file(str::joinpath(f.ds_root, td.test_data[i].destfile)).exists());
        wassert(actual_type(md.source()).is_source_blob("grib1", f.ds_root, td.test_data[i].destfile));
    }
});

add_method("compressed", [](Fixture& f) {
    // Test moving into archive data that have been compressed
    ConfigFile cfg = f.cfg;
    cfg.setValue("archive age", days_since(2007, 9, 1));

    // Import and compress all the files
    f.clean_and_import(&cfg);
    scan::compress("testds/2007/07-07.grib1");
    scan::compress("testds/2007/07-08.grib1");
    scan::compress("testds/2007/10-09.grib1");
    sys::unlink_ifexists("testds/2007/07-07.grib1");
    sys::unlink_ifexists("testds/2007/07-08.grib1");
    sys::unlink_ifexists("testds/2007/10-09.grib1");

    // Test that querying returns all items
    {
        std::unique_ptr<Reader> reader(f.makeReader(&cfg));
        unsigned count = count_results(*reader, Matcher());
        ensure_equals(count, 3u);
    }

    // Check if files to archive are detected
    {
        auto writer(f.makeLocalChecker(&cfg));

        MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_OK] = 1;
        expected.by_type[DatasetTest::COUNTED_ARCHIVE_AGE] = 2;
        wassert(actual(writer.get()).maintenance(expected));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto writer(f.makeLocalChecker(&cfg));
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib1");
        e.archived.emplace_back("testds", "2007/07-08.grib1");
        wassert(actual(writer.get()).repack(e, true));
    }

    // Check that the files have been moved to the archive
    ensure(!sys::exists("testds/.archive/last/2007/07-07.grib1"));
    ensure(sys::exists("testds/.archive/last/2007/07-07.grib1.gz"));
    ensure(sys::exists("testds/.archive/last/2007/07-07.grib1.gz.idx"));
    ensure(sys::exists("testds/.archive/last/2007/07-07.grib1.metadata"));
    ensure(sys::exists("testds/.archive/last/2007/07-07.grib1.summary"));
    ensure(!sys::exists("testds/.archive/last/2007/07-08.grib1"));
    ensure(sys::exists("testds/.archive/last/2007/07-08.grib1.gz"));
    ensure(sys::exists("testds/.archive/last/2007/07-08.grib1.gz.idx"));
    ensure(sys::exists("testds/.archive/last/2007/07-08.grib1.metadata"));
    ensure(sys::exists("testds/.archive/last/2007/07-08.grib1.summary"));
    ensure(!sys::exists("testds/2007/07-07.grib1"));
    ensure(!sys::exists("testds/2007/07-07.grib1.gz"));
    ensure(!sys::exists("testds/2007/07-07.grib1.gz.idx"));
    ensure(!sys::exists("testds/2007/07-07.grib1.metadata"));
    ensure(!sys::exists("testds/2007/07-07.grib1.summary"));
    ensure(!sys::exists("testds/2007/07-08.grib1"));
    ensure(!sys::exists("testds/2007/07-08.grib1.gz"));
    ensure(!sys::exists("testds/2007/07-08.grib1.gz.idx"));
    ensure(!sys::exists("testds/2007/07-08.grib1.metadata"));
    ensure(!sys::exists("testds/2007/07-08.grib1.summary"));

    // Maintenance should now show a normal situation
    {
        auto writer(f.makeLocalChecker(&cfg));
        ReporterExpected e;
        e.report.emplace_back("testds", "check", "1 file ok");
        e.report.emplace_back("testds.archives.last", "check", "2 files ok");
        wassert(actual(writer.get()).check(e, false));
    }

    // Perform full maintenance and check that things are still ok afterwards
    {
        auto writer(f.makeLocalChecker(&cfg));
        wassert(actual(writer.get()).check_clean(true, true));

        ReporterExpected e;
        e.report.emplace_back("testds", "check", "1 file ok");
        e.report.emplace_back("testds.archives.last", "check", "2 files ok");
        wassert(actual(writer.get()).check(e, false));
    }

    // Test that querying returns all items
    {
        std::unique_ptr<Reader> reader(f.makeReader(&cfg));
        ensure_equals(count_results(*reader, Matcher()), 3u);
    }
});
add_method("query_archived", [](Fixture& f) {
    // Test querying with archived data
    using namespace arki::types;

    ConfigFile cfg = f.cfg;
    cfg.setValue("archive age", days_since(2007, 9, 1));
    f.clean_and_import(&cfg);
    {
        auto writer(f.makeLocalChecker(&cfg));
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib1");
        e.archived.emplace_back("testds", "2007/07-08.grib1");
        wassert(actual(writer.get()).repack(e, true));
    }

    std::unique_ptr<Reader> reader(f.makeReader(&cfg));
    metadata::Collection mdc(*reader, Matcher::parse(""));
    wassert(actual(mdc.size()) == 3u);

    mdc.clear();
    mdc.add(*reader, Matcher::parse("origin:GRIB1,200"));
    wassert(actual(mdc.size()) == 1u);

    // Check that the source record that comes out is ok
    //wassert(actual_type(mdc[0].source()).is_source_inline("grib1", 7218));

    // Check that data is accessible
    const auto& buf = mdc[0].getData();
    wassert(actual(buf.size()) == 7218u);

    mdc.clear();
    mdc.add(*reader, Matcher::parse("reftime:=2007-07-08"));
    wassert(actual(mdc.size()) == 1u);
    wassert(actual(mdc[0].data_size()) == 7218u);

    mdc.clear();
    mdc.add(*reader, Matcher::parse("origin:GRIB1,80"));
    wassert(actual(mdc.size()) == 1u);

    mdc.clear();
    mdc.add(*reader, Matcher::parse("origin:GRIB1,98"));
    wassert(actual(mdc.size()) == 1u);

    // Query bytes
    sys::File out(sys::File::mkstemp("test"));
    dataset::ByteQuery bq;
    bq.setData(Matcher());
    reader->query_bytes(bq, out);
    string res = sys::read_file(out.name());
    wassert(actual(res.size()) == 44412u);

    out.lseek(0); out.ftruncate(0);
    bq.matcher = Matcher::parse("origin:GRIB1,200");
    reader->query_bytes(bq, out);
    res = sys::read_file(out.name());
    wassert(actual(res.size()) == 7218u);

    out.lseek(0); out.ftruncate(0);
    bq.matcher = Matcher::parse("reftime:=2007-07-08");
    reader->query_bytes(bq, out);
    res = sys::read_file(out.name());
    wassert(actual(res.size()) == 7218u);

    /* TODO
        case BQ_POSTPROCESS:
        case BQ_REP_METADATA:
        case BQ_REP_SUMMARY:
    virtual void query_summary(const Matcher& matcher, Summary& summary);
    */

    // Query summary
    Summary s;
    reader->query_summary(Matcher::parse(""), s);
    wassert(actual(s.count()) == 3u);
    wassert(actual(s.size()) == 44412u);

    s.clear();
    reader->query_summary(Matcher::parse("origin:GRIB1,200"), s);
    wassert(actual(s.count()) == 1u);
    wassert(actual(s.size()) == 7218u);

    s.clear();
    reader->query_summary(Matcher::parse("reftime:=2007-07-08"), s);
    wassert(actual(s.count()) == 1u);
    wassert(actual(s.size()) == 7218u);
});
add_method("empty_dirs", [](Fixture& f) {
    // Tolerate empty dirs
    // Start with an empty dir
    system("rm -rf testds");
    system("mkdir testds");

    std::unique_ptr<Reader> reader(f.makeReader());

    metadata::Collection mdc(*reader, Matcher());
    ensure(mdc.empty());

    Summary s;
    reader->query_summary(Matcher::parse(""), s);
    ensure_equals(s.count(), 0u);

    sys::File out(sys::File::mkstemp("test"));
    dataset::ByteQuery bq;
    bq.setData(Matcher::parse(""));
    reader->query_bytes(bq, out);
    out.close();
    wassert(actual(sys::size(out.name())) == 0u);
});
add_method("query_lots", [](Fixture& f) {
    // Test querying with lots of data, to trigger on disk metadata buffering
    using namespace arki::types;

    f.clean();
    {
        std::unique_ptr<Writer> writer(f.makeWriter());

        // Reverse order of import, so we can check if things get sorted properly when querying
        for (int month = 12; month >= 1; --month)
            for (int day = 28; day >= 1; --day)
                for (int hour = 21; hour >= 0; hour -= 3)
                    for (int station = 2; station >= 1; --station)
                        for (int varid = 3; varid >= 1; --varid)
                        {
                            int value = month + day + hour + station + varid;
                            Metadata md;
                            char buf[40];
                            int len = snprintf(buf, 40, "2013%02d%02d%02d00,%d,%d,%d,,,000000000",
                                    month, day, hour, station, varid, value);
                            md.set_source_inline("vm2", vector<uint8_t>(buf, buf+len));
                            md.add_note("Generated from memory");
                            md.set(Reftime::createPosition(Time(2013, month, day, hour, 0, 0)));
                            md.set(Area::createVM2(station));
                            md.set(Product::createVM2(varid));
                            snprintf(buf, 40, "%d,,,000000000", value);
                            md.set(types::Value::create(buf));
                            Writer::AcquireResult res = writer->acquire(md);
                            wassert(actual(res) == Writer::ACQ_OK);
                        }
    }

    utils::files::removeDontpackFlagfile(f.cfg.value("path"));

    // Query all the dataset and make sure the results are in the right order
    struct CheckSortOrder : public metadata::Eater
    {
        uint64_t last_value;
        unsigned seen;
        CheckSortOrder() : last_value(0), seen(0) {}
        virtual uint64_t make_key(const Metadata& md) const = 0;
        bool eat(unique_ptr<Metadata>&& md) override
        {
            uint64_t value = make_key(*md);
            wassert(actual(value) >= last_value);
            last_value = value;
            ++seen;
            return true;
        }
    };

    struct CheckReftimeSortOrder : public CheckSortOrder
    {
        virtual uint64_t make_key(const Metadata& md) const
        {
            const reftime::Position* rt = md.get<types::reftime::Position>();
            return rt->time.vals[1] * 10000 + rt->time.vals[2] * 100 + rt->time.vals[3];
        }
    };

    struct CheckAllSortOrder : public CheckSortOrder
    {
        virtual uint64_t make_key(const Metadata& md) const
        {
            const reftime::Position* rt = md.get<types::reftime::Position>();
            const area::VM2* area = dynamic_cast<const area::VM2*>(md.get(TYPE_AREA));
            const product::VM2* prod = dynamic_cast<const product::VM2*>(md.get(TYPE_PRODUCT));
            uint64_t dt = rt->time.vals[1] * 10000 + rt->time.vals[2] * 100 + rt->time.vals[3];
            return dt * 100 + area->station_id() * 10 + prod->variable_id();
        }
    };

    {
        std::unique_ptr<Reader> reader(f.makeReader());
        CheckReftimeSortOrder cso;
        reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) { return cso.eat(move(md)); });
        wassert(actual(cso.seen) == 16128u);
    }

    {
        std::unique_ptr<Reader> reader(f.makeReader());
        CheckAllSortOrder cso;
        dataset::DataQuery dq(Matcher::parse(""));
        dq.sorter = sort::Compare::parse("reftime,area,product");
        reader->query_data(dq, [&](unique_ptr<Metadata> md) { return cso.eat(move(md)); });
        wassert(actual(cso.seen) == 16128u);
    }
});

}
}
