#include "tests.h"
#include "segmented.h"
#include "indexed.h"
#include "maintenance.h"
#include "arki/dataset/time.h"
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
using arki::core::Time;

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

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test_ondisk2("arki_dataset_segmented_ondisk2", "type=ondisk2\n");
Tests test_simple_plain("arki_dataset_segmented_simple_plain", "type=simple\nindex_type=plain\n");
Tests test_simple_sqlite("arki_dataset_segmented_simple_sqlite", "type=simple\nindex_type=sqlite");

void Tests::register_tests() {

add_method("compressed", [](Fixture& f) {
    // Import and compress all the files
    f.clean_and_import();
    f.test_reread_config();
    // Test moving into archive data that have been compressed
    f.cfg.setValue("archive age", days_since(2007, 9, 1));

    scan::compress("testds/2007/07-07.grib");
    scan::compress("testds/2007/07-08.grib");
    scan::compress("testds/2007/10-09.grib");
    sys::unlink_ifexists("testds/2007/07-07.grib");
    sys::unlink_ifexists("testds/2007/07-08.grib");
    sys::unlink_ifexists("testds/2007/10-09.grib");

    // Test that querying returns all items
    {
        auto reader(f.config().create_reader());
        unsigned count = count_results(*reader, Matcher());
        ensure_equals(count, 3u);
    }

    // Check if files to archive are detected
    {
        auto writer(f.makeSegmentedChecker());

        MaintenanceResults expected(false, 3);
        expected.by_type[DatasetTest::COUNTED_OK] = 1;
        expected.by_type[DatasetTest::COUNTED_ARCHIVE_AGE] = 2;
        wassert(actual(writer.get()).maintenance(expected));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib");
        e.archived.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(writer.get()).repack(e, true));
    }

    // Check that the files have been moved to the archive
    ensure(!sys::exists("testds/.archive/last/2007/07-07.grib"));
    ensure(sys::exists("testds/.archive/last/2007/07-07.grib.gz"));
    ensure(sys::exists("testds/.archive/last/2007/07-07.grib.gz.idx"));
    ensure(sys::exists("testds/.archive/last/2007/07-07.grib.metadata"));
    ensure(sys::exists("testds/.archive/last/2007/07-07.grib.summary"));
    ensure(!sys::exists("testds/.archive/last/2007/07-08.grib"));
    ensure(sys::exists("testds/.archive/last/2007/07-08.grib.gz"));
    ensure(sys::exists("testds/.archive/last/2007/07-08.grib.gz.idx"));
    ensure(sys::exists("testds/.archive/last/2007/07-08.grib.metadata"));
    ensure(sys::exists("testds/.archive/last/2007/07-08.grib.summary"));
    ensure(!sys::exists("testds/2007/07-07.grib"));
    ensure(!sys::exists("testds/2007/07-07.grib.gz"));
    ensure(!sys::exists("testds/2007/07-07.grib.gz.idx"));
    ensure(!sys::exists("testds/2007/07-07.grib.metadata"));
    ensure(!sys::exists("testds/2007/07-07.grib.summary"));
    ensure(!sys::exists("testds/2007/07-08.grib"));
    ensure(!sys::exists("testds/2007/07-08.grib.gz"));
    ensure(!sys::exists("testds/2007/07-08.grib.gz.idx"));
    ensure(!sys::exists("testds/2007/07-08.grib.metadata"));
    ensure(!sys::exists("testds/2007/07-08.grib.summary"));

    // Maintenance should now show a normal situation
    {
        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "check", "1 file ok");
        e.report.emplace_back("testds.archives.last", "check", "2 files ok");
        wassert(actual(writer.get()).check(e, false));
    }

    // Perform full maintenance and check that things are still ok afterwards
    {
        auto writer(f.makeSegmentedChecker());
        wassert(actual(writer.get()).check_clean(true, true));

        ReporterExpected e;
        e.report.emplace_back("testds", "check", "1 file ok");
        e.report.emplace_back("testds.archives.last", "check", "2 files ok");
        wassert(actual(writer.get()).check(e, false));
    }

    // Test that querying returns all items
    {
        auto reader = f.config().create_reader();
        ensure_equals(count_results(*reader, Matcher()), 3u);
    }
});

add_method("query_archived", [](Fixture& f) {
    // Test querying with archived data
    using namespace arki::types;
    f.clean_and_import();
    f.test_reread_config();
    f.cfg.setValue("archive age", days_since(2007, 9, 1));
    {
        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib");
        e.archived.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(writer.get()).repack(e, true));
    }

    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, Matcher::parse(""));
    wassert(actual(mdc.size()) == 3u);

    mdc.clear();
    mdc.add(*reader, Matcher::parse("origin:GRIB1,200"));
    wassert(actual(mdc.size()) == 1u);

    // Check that the source record that comes out is ok
    //wassert(actual_type(mdc[0].source()).is_source_inline("grib", 7218));

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
    f.clean();

    auto reader = f.config().create_reader();

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
        auto writer = f.config().create_writer();

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
    struct CheckSortOrder
    {
        uint64_t last_value;
        unsigned seen;
        CheckSortOrder() : last_value(0), seen(0) {}
        virtual uint64_t make_key(const Metadata& md) const = 0;
        bool eat(unique_ptr<Metadata>&& md)
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
            return rt->time.mo * 10000 + rt->time.da * 100 + rt->time.ho;
        }
    };

    struct CheckAllSortOrder : public CheckSortOrder
    {
        virtual uint64_t make_key(const Metadata& md) const
        {
            const reftime::Position* rt = md.get<types::reftime::Position>();
            const area::VM2* area = dynamic_cast<const area::VM2*>(md.get(TYPE_AREA));
            const product::VM2* prod = dynamic_cast<const product::VM2*>(md.get(TYPE_PRODUCT));
            uint64_t dt = rt->time.mo * 10000 + rt->time.da * 100 + rt->time.ho;
            return dt * 100 + area->station_id() * 10 + prod->variable_id();
        }
    };

    {
        auto reader = f.config().create_reader();
        CheckReftimeSortOrder cso;
        reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) { return cso.eat(move(md)); });
        wassert(actual(cso.seen) == 16128u);
    }

    {
        auto reader = f.config().create_reader();
        CheckAllSortOrder cso;
        dataset::DataQuery dq(Matcher::parse(""));
        dq.sorter = sort::Compare::parse("reftime,area,product");
        reader->query_data(dq, [&](unique_ptr<Metadata> md) { return cso.eat(move(md)); });
        wassert(actual(cso.seen) == 16128u);
    }
});

// Ensure that the segment is not archived if it can still accept new data
// after the archive cutoff (regardless of the data currently in the
// segment)
add_method("archive_age", [](Fixture& f) {
    f.cfg.setValue("step", "yearly");

    // Import a file with a known reftime
    // Reftime: 2007-07-08T13:00:00Z
    metadata::Collection mds("inbound/test.grib1");
    dataset::Writer::AcquireResult res = wcallchecked(f.makeSegmentedWriter()->acquire(mds[0]));
    wassert(actual(res) == dataset::Writer::ACQ_OK);
    wassert(actual(f.makeSegmentedChecker().get()).check_clean(true));

    f.test_reread_config();

    // TZ=UTC date --date="2008-01-01 00:00:00" +%s
    time_t start2008 = 1199145600;

    f.cfg.setValue("archive age", "1");

    {
        auto o = dataset::SessionTime::local_override(start2008);
        wassert(actual(f.makeSegmentedChecker().get()).repack_clean(false));
    }

    {
        auto o = dataset::SessionTime::local_override(start2008 + 86400);
        ReporterExpected e;
        e.archived.emplace_back("testds", "20/2007.grib");
        wassert(actual(f.makeSegmentedChecker().get()).repack(e, false));
    }
});

// Ensure that the segment is not deleted if it can still accept new data
// after the archive cutoff (regardless of the data currently in the
// segment)
add_method("delete_age", [](Fixture& f) {
    f.cfg.setValue("step", "yearly");

    // Import a file with a known reftime
    // Reftime: 2007-07-08T13:00:00Z
    metadata::Collection mds("inbound/test.grib1");
    dataset::Writer::AcquireResult res = wcallchecked(f.makeSegmentedWriter()->acquire(mds[0]));
    wassert(actual(res) == dataset::Writer::ACQ_OK);
    wassert(actual(f.makeSegmentedChecker().get()).check_clean(true));

    // TZ=UTC date --date="2008-01-01 00:00:00" +%s
    time_t start2008 = 1199145600;

    f.test_reread_config();
    f.cfg.setValue("delete age", "1");

    {
        auto o = dataset::SessionTime::local_override(start2008);
        wassert(actual(f.makeSegmentedChecker().get()).repack_clean(false));
    }

    {
        auto o = dataset::SessionTime::local_override(start2008 + 86400);
        ReporterExpected e;
        e.deleted.emplace_back("testds", "20/2007.grib");
        wassert(actual(f.makeSegmentedChecker().get()).repack(e, false));
    }
});

add_method("unarchive_segment", [](Fixture& f) {
    // Import a file with a known reftime
    // Reftime: 2007-07-08T13:00:00Z
    // Reftime: 2007-07-07T00:00:00Z
    // Reftime: 2007-10-09T00:00:00Z
    f.clean_and_import();
    f.test_reread_config();

    // TZ=UTC date --date="2007-07-09 00:00:00" +%s
    time_t now = 1183939200;
    f.cfg.setValue("archive age", "1");

    // Archive one segment
    {
        auto o = dataset::SessionTime::local_override(now);
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib");
        wassert(actual(f.makeSegmentedChecker().get()).repack(e, true));
    }

    // Check that segments are where we expect them
    ensure(sys::exists("testds/.archive/last/2007/07-07.grib"));
    ensure(sys::exists("testds/.archive/last/2007/07-07.grib.metadata"));
    ensure(sys::exists("testds/.archive/last/2007/07-07.grib.summary"));
    ensure(!sys::exists("testds/.archive/last/2007/07-08.grib"));
    ensure(!sys::exists("testds/.archive/last/2007/07-08.grib.metadata"));
    ensure(!sys::exists("testds/.archive/last/2007/07-08.grib.summary"));
    ensure(!sys::exists("testds/.archive/last/2007/10-09.grib"));
    ensure(!sys::exists("testds/.archive/last/2007/10-09.grib.metadata"));
    ensure(!sys::exists("testds/.archive/last/2007/10-09.grib.summary"));
    ensure(!sys::exists("testds/2007/07-07.grib"));
    ensure(sys::exists("testds/2007/07-08.grib"));
    ensure(sys::exists("testds/2007/10-09.grib"));

    {
        f.makeSegmentedChecker()->unarchive_segment("2007/07-07.grib");
    }

    ensure(!sys::exists("testds/.archive/last/2007/07-07.grib"));
    ensure(!sys::exists("testds/.archive/last/2007/07-07.grib.metadata"));
    ensure(!sys::exists("testds/.archive/last/2007/07-07.grib.summary"));
    ensure(!sys::exists("testds/.archive/last/2007/07-08.grib"));
    ensure(!sys::exists("testds/.archive/last/2007/07-08.grib.metadata"));
    ensure(!sys::exists("testds/.archive/last/2007/07-08.grib.summary"));
    ensure(!sys::exists("testds/.archive/last/2007/10-09.grib"));
    ensure(!sys::exists("testds/.archive/last/2007/10-09.grib.metadata"));
    ensure(!sys::exists("testds/.archive/last/2007/10-09.grib.summary"));
    ensure(sys::exists("testds/2007/07-07.grib"));
    ensure(sys::exists("testds/2007/07-08.grib"));
    ensure(sys::exists("testds/2007/10-09.grib"));
});



}
}
