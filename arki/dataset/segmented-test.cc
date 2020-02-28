#include "tests.h"
#include "arki/segment/tests.h"
#include "segmented.h"
#include "indexed.h"
#include "maintenance.h"
#include "arki/exceptions.h"
#include "arki/core/file.h"
#include "arki/dataset/time.h"
#include "arki/dataset/query.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/types/area.h"
#include "arki/types/product.h"
#include "arki/types/value.h"
#include "arki/metadata/sort.h"
#include "arki/utils/sys.h"
#include <sys/resource.h>
#include <time.h>
#include <algorithm>

namespace {
using namespace std;
using namespace arki;
using namespace arki::core;
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
Tests test_iseg("arki_dataset_segmented_iseg", "type=iseg\nformat=grib");

void Tests::register_tests() {

add_method("gz", [](Fixture& f) {
    // Import and compress all the files
    f.clean_and_import();

    // Test moving into archive data that have been compressed
    f.cfg.set("archive age", days_since(2007, 9, 1));
    f.cfg.set("gz group size", 0);
    f.test_reread_config();

    // Compress segments
    {
        dataset::CheckerConfig cfg;
        cfg.online = cfg.offline = true; cfg.readonly = false;
        auto checker(f.makeSegmentedChecker());
        checker->compress(cfg, 0);
    }

    // Test that querying returns all items
    wassert(f.query_results({1, 0, 2}));

    // Check if files to archive are detected
    {
        auto state = f.scan_state();
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("testds:2007/07-07.grib").state) == segment::State(segment::SEGMENT_ARCHIVE_AGE));
        wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::State(segment::SEGMENT_ARCHIVE_AGE));
        wassert(actual(state.get("testds:2007/10-09.grib").state) == segment::State(segment::SEGMENT_OK));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto checker(f.makeSegmentedChecker());
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib");
        e.archived.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(checker.get()).repack(e, true));
    }

    // Check that the files have been moved to the archive
    wassert(actual_segment("testds/.archive/last/2007/07-07.grib").exists({".gz", ".metadata", ".summary"}));
    wassert(actual_segment("testds/.archive/last/2007/07-08.grib").exists({".gz", ".metadata", ".summary"}));
    wassert(actual_segment("testds/.archive/last/2007/10-09.grib").not_exists());
    wassert(actual_segment("testds/2007/07-07.grib").not_exists());
    wassert(actual_segment("testds/2007/07-08.grib").not_exists());
    wassert(f.online_segment_exists("2007/10-09.grib", {".gz"}));

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
    wassert(f.query_results({1, 0, 2}));
});

add_method("gzidx", [](Fixture& f) {
    // Import and compress all the files
    f.clean_and_import();
    // Do an extra round of import to have at least the two data per segment needed to trigger indexing
    metadata::TestCollection mds("inbound/fixture.grib1");
    for (auto& md: mds)
    {
        Time t = md->get<types::reftime::Position>()->time;
        t.se = 30;
        md->set(types::reftime::Position(t));
    }
    wassert(f.import(mds));

    // Test moving into archive data that have been compressed
    f.cfg.set("archive age", days_since(2007, 9, 1));
    f.cfg.set("gz group size", 1);
    f.test_reread_config();

    // Compress segments
    {
        dataset::CheckerConfig cfg;
        cfg.online = cfg.offline = true; cfg.readonly = false;
        auto checker(f.makeSegmentedChecker());
        checker->compress(cfg, 1);
    }

    // Test that querying returns all items
    wassert(f.query_results({1, 4, 0, 3, 2, 5}));

    // Check if files to archive are detected
    {
        auto state = f.scan_state();
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("testds:2007/07-07.grib").state) == segment::State(segment::SEGMENT_ARCHIVE_AGE));
        wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::State(segment::SEGMENT_ARCHIVE_AGE));
        wassert(actual(state.get("testds:2007/10-09.grib").state) == segment::State(segment::SEGMENT_OK));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto checker(f.makeSegmentedChecker());
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib");
        e.archived.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(checker.get()).repack(e, true));
    }

    // Check that the files have been moved to the archive
    wassert(actual_segment("testds/.archive/last/2007/07-07.grib").exists({".gz", ".gz.idx", ".metadata", ".summary"}));
    wassert(actual_segment("testds/.archive/last/2007/07-08.grib").exists({".gz", ".gz.idx", ".metadata", ".summary"}));
    wassert(actual_segment("testds/.archive/last/2007/10-09.grib").not_exists());
    wassert(actual_segment("testds/2007/07-07.grib").not_exists());
    wassert(actual_segment("testds/2007/07-08.grib").not_exists());
    wassert(f.online_segment_exists("2007/10-09.grib", {".gz", ".gz.idx"}));

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
    wassert(f.query_results({1, 4, 0, 3, 2, 5}));
});

add_method("tarred", [](Fixture& f) {
    // Import and compress all the files
    f.clean_and_import();

    // Test moving into archive data that have been tarred
    f.cfg.set("archive age", days_since(2007, 9, 1));
    f.test_reread_config();

    // tar segments
    {
        auto checker(f.makeSegmentedChecker());
        checker->segments_tracked([&](segmented::CheckerSegment& seg) {
            seg.tar();
        });
    }

    // Test that querying returns all items
    wassert(f.query_results({1, 0, 2}));

    // Check if files to archive are detected
    {
        auto state = f.scan_state();
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("testds:2007/07-07.grib").state) == segment::State(segment::SEGMENT_ARCHIVE_AGE));
        wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::State(segment::SEGMENT_ARCHIVE_AGE));
        wassert(actual(state.get("testds:2007/10-09.grib").state) == segment::State(segment::SEGMENT_OK));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto checker(f.makeSegmentedChecker());
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib");
        e.archived.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(checker.get()).repack(e, true));
    }

    // Check that the files have been moved to the archive
    wassert(actual_segment("testds/.archive/last/2007/07-07.grib").exists({".tar", ".metadata", ".summary"}));
    wassert(actual_segment("testds/.archive/last/2007/07-08.grib").exists({".tar", ".metadata", ".summary"}));
    wassert(actual_segment("testds/.archive/last/2007/10-09.grib").not_exists());
    wassert(actual_segment("testds/2007/07-07.grib").not_exists());
    wassert(actual_segment("testds/2007/07-08.grib").not_exists());
    wassert(f.online_segment_exists("2007/10-09.grib", {".tar"}));

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
    wassert(f.query_results({1, 0, 2}));
});

add_method("zipped", [](Fixture& f) {
    skip_unless_libzip();
    skip_unless_libarchive();

    // Import and compress all the files
    f.clean_and_import();

    // Test moving into archive data that have been tarred
    f.cfg.set("archive age", days_since(2007, 9, 1));
    f.test_reread_config();

    // tar segments
    {
        auto checker(f.makeSegmentedChecker());
        checker->segments_tracked([&](segmented::CheckerSegment& seg) {
            seg.zip();
        });
    }

    // Test that querying returns all items
    wassert(f.query_results({1, 0, 2}));

    // Check if files to archive are detected
    {
        auto state = f.scan_state();
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("testds:2007/07-07.grib").state) == segment::State(segment::SEGMENT_ARCHIVE_AGE));
        wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::State(segment::SEGMENT_ARCHIVE_AGE));
        wassert(actual(state.get("testds:2007/10-09.grib").state) == segment::State(segment::SEGMENT_OK));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto checker(f.makeSegmentedChecker());
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib");
        e.archived.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(checker.get()).repack(e, true));
    }

    // Check that the files have been moved to the archive
    wassert(actual_segment("testds/.archive/last/2007/07-07.grib").exists({".zip", ".metadata", ".summary"}));
    wassert(actual_segment("testds/.archive/last/2007/07-08.grib").exists({".zip", ".metadata", ".summary"}));
    wassert(actual_segment("testds/.archive/last/2007/10-09.grib").not_exists());
    wassert(actual_segment("testds/2007/07-07.grib").not_exists());
    wassert(actual_segment("testds/2007/07-08.grib").not_exists());
    wassert(f.online_segment_exists("2007/10-09.grib", {".zip"}));

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
    wassert(f.query_results({1, 0, 2}));
});

add_method("query_archived", [](Fixture& f) {
    // Test querying with archived data
    using namespace arki::types;
    f.clean_and_import();
    f.cfg.set("archive age", days_since(2007, 9, 1));
    f.test_reread_config();
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
    mdc.add(*reader, dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), true));
    wassert(actual(mdc.size()) == 1u);

    // Check that the source record that comes out is ok
    //wassert(actual_type(mdc[0].source()).is_source_inline("grib", 7218));

    // Check that data is accessible
    const auto& buf = mdc[0].get_data().read();
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
    wassert_true(mdc.empty());

    Summary s;
    reader->query_summary(Matcher::parse(""), s);
    wassert(actual(s.count()) == 0u);

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

    f.reset_test("step=daily\nformat=vm2\nunique=product,area,reftime\n");

    f.clean();
    {
        metadata::Collection mds;
        for (int month = 12; month >= 1; --month)
            for (int day = 28; day >= 1; --day)
                for (int hour = 21; hour >= 0; hour -= 3)
                    for (int station = 2; station >= 1; --station)
                        for (int varid = 3; varid >= 1; --varid)
                        {
                            int value = month + day + hour + station + varid;
                            std::unique_ptr<Metadata> md(new Metadata);
                            char buf[40];
                            int len = snprintf(buf, 40, "2013%02d%02d%02d00,%d,%d,%d,,,000000000",
                                    month, day, hour, station, varid, value);
                            md->set_source_inline("vm2", metadata::DataManager::get().to_data("vm2", vector<uint8_t>(buf, buf+len)));
                            md->add_note("Generated from memory");
                            md->set(Reftime::createPosition(Time(2013, month, day, hour, 0, 0)));
                            md->set(Area::createVM2(station));
                            md->set(Product::createVM2(varid));
                            snprintf(buf, 40, "%d,,,000000000", value);
                            md->set(types::Value::create(buf));
                            mds.acquire(std::move(md));
                        }

        wassert(f.import(mds));
    }

    utils::files::removeDontpackFlagfile(f.cfg.value("path"));

    // Query all the dataset and make sure the results are in the right order
    struct CheckSortOrder
    {
        uint64_t last_value;
        unsigned seen;
        CheckSortOrder() : last_value(0), seen(0) {}
        virtual uint64_t make_key(const Metadata& md) const = 0;
        bool eat(std::shared_ptr<Metadata> md)
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
        reader->query_data(Matcher(), [&](std::shared_ptr<Metadata> md) { return cso.eat(md); });
        wassert(actual(cso.seen) == 16128u);
    }

    {
        auto reader = f.config().create_reader();
        CheckAllSortOrder cso;
        dataset::DataQuery dq(Matcher::parse(""));
        dq.sorter = metadata::sort::Compare::parse("reftime,area,product");
        reader->query_data(dq, [&](std::shared_ptr<Metadata> md) { return cso.eat(md); });
        wassert(actual(cso.seen) == 16128u);
    }
});

// Ensure that the segment is not archived if it can still accept new data
// after the archive cutoff (regardless of the data currently in the
// segment)
add_method("archive_age", [](Fixture& f) {
    f.cfg.set("step", "yearly");
    f.test_reread_config();

    // Import a file with a known reftime
    // Reftime: 2007-07-08T13:00:00Z
    metadata::TestCollection mds("inbound/test.grib1");
    wassert(actual(*f.makeSegmentedWriter()).import(mds[0]));
    wassert(actual(f.makeSegmentedChecker().get()).check_clean(true));

    // TZ=UTC date --date="2008-01-01 00:00:00" +%s
    time_t start2008 = 1199145600;
    f.cfg.set("archive age", "1");
    f.test_reread_config();


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
    f.cfg.set("step", "yearly");
    f.test_reread_config();

    // Import a file with a known reftime
    // Reftime: 2007-07-08T13:00:00Z
    metadata::TestCollection mds("inbound/test.grib1");
    wassert(actual(*f.makeSegmentedWriter()).import(mds[0]));
    wassert(actual(f.makeSegmentedChecker().get()).check_clean(true));

    // TZ=UTC date --date="2008-01-01 00:00:00" +%s
    time_t start2008 = 1199145600;
    f.cfg.set("delete age", "1");
    f.test_reread_config();

    {
        auto o = dataset::SessionTime::local_override(start2008);
        wassert(actual(f.makeSegmentedChecker().get()).repack_clean(false));
    }

    {
        auto o = dataset::SessionTime::local_override(start2008 + 86400);
        ReporterExpected e;
        e.deleted.emplace_back("testds", "20/2007.grib");
        wassert(actual(f.makeSegmentedChecker().get()).repack(e, false));

        // Query

    }
});

add_method("unarchive_segment", [](Fixture& f) {
    // Import a file with a known reftime
    // Reftime: 2007-07-08T13:00:00Z
    // Reftime: 2007-07-07T00:00:00Z
    // Reftime: 2007-10-09T00:00:00Z
    f.clean_and_import();

    // TZ=UTC date --date="2007-07-09 00:00:00" +%s
    time_t now = 1183939200;
    f.cfg.set("archive age", "1");

    f.test_reread_config();

    // Archive one segment
    {
        auto o = dataset::SessionTime::local_override(now);
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib");
        wassert(actual(f.makeSegmentedChecker().get()).repack(e, true));
    }

    // Check that segments are where we expect them
    wassert(actual_file("testds/.archive/last/2007/07-07.grib").exists());
    wassert(actual_file("testds/.archive/last/2007/07-07.grib.metadata").exists());
    wassert(actual_file("testds/.archive/last/2007/07-07.grib.summary").exists());
    wassert(actual_file("testds/.archive/last/2007/07-08.grib").not_exists());
    wassert(actual_file("testds/.archive/last/2007/07-08.grib.metadata").not_exists());
    wassert(actual_file("testds/.archive/last/2007/07-08.grib.summary").not_exists());
    wassert(actual_file("testds/.archive/last/2007/10-09.grib").not_exists());
    wassert(actual_file("testds/.archive/last/2007/10-09.grib.metadata").not_exists());
    wassert(actual_file("testds/.archive/last/2007/10-09.grib.summary").not_exists());
    wassert(actual_file("testds/2007/07-07.grib").not_exists());
    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/10-09.grib").exists());

    {
        f.makeSegmentedChecker()->segment("2007/07-07.grib")->unarchive();
    }

    wassert(actual_file("testds/.archive/last/2007/07-07.grib").not_exists());
    wassert(actual_file("testds/.archive/last/2007/07-07.grib.metadata").not_exists());
    wassert(actual_file("testds/.archive/last/2007/07-07.grib.summary").not_exists());
    wassert(actual_file("testds/.archive/last/2007/07-08.grib").not_exists());
    wassert(actual_file("testds/.archive/last/2007/07-08.grib.metadata").not_exists());
    wassert(actual_file("testds/.archive/last/2007/07-08.grib.summary").not_exists());
    wassert(actual_file("testds/.archive/last/2007/10-09.grib").not_exists());
    wassert(actual_file("testds/.archive/last/2007/10-09.grib.metadata").not_exists());
    wassert(actual_file("testds/.archive/last/2007/10-09.grib.summary").not_exists());
    wassert(actual_file("testds/2007/07-07.grib").exists());
    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/10-09.grib").exists());
});

add_method("unarchive_segment_lastonly", [](Fixture& f) {
    // Import a file with a known reftime
    // Reftime: 2007-07-08T13:00:00Z
    // Reftime: 2007-07-07T00:00:00Z
    // Reftime: 2007-10-09T00:00:00Z
    f.clean_and_import();

    // TZ=UTC date --date="2007-07-09 00:00:00" +%s
    time_t now = 1183939200;
    f.cfg.set("archive age", "1");

    f.test_reread_config();

    // Archive one segment
    {
        auto o = dataset::SessionTime::local_override(now);
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib");
        wassert(actual(f.makeSegmentedChecker().get()).repack(e, true));
    }

    // Check that segments are where we expect them
    sys::rename("testds/.archive/last/2007", "testds/.archive/testds-2007");
    wassert(actual_file("testds/.archive/testds-2007/07-07.grib").exists());
    wassert(actual_file("testds/.archive/testds-2007/07-07.grib.metadata").exists());
    wassert(actual_file("testds/.archive/testds-2007/07-07.grib.summary").exists());
    wassert(actual_file("testds/.archive/testds-2007/07-08.grib").not_exists());
    wassert(actual_file("testds/.archive/testds-2007/07-08.grib.metadata").not_exists());
    wassert(actual_file("testds/.archive/testds-2007/07-08.grib.summary").not_exists());
    wassert(actual_file("testds/.archive/testds-2007/10-09.grib").not_exists());
    wassert(actual_file("testds/.archive/testds-2007/10-09.grib.metadata").not_exists());
    wassert(actual_file("testds/.archive/testds-2007/10-09.grib.summary").not_exists());
    wassert(actual_file("testds/2007/07-07.grib").not_exists());
    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/10-09.grib").exists());

    try {
        f.makeSegmentedChecker()->segment("../test-ds/2007/07-07.grib")->unarchive();
        wassert(throw std::runtime_error("unarchive_segment should have thrown at this point"));
    } catch (std::exception& e) {
        wassert(actual(e.what()).contains("segment is not in last/ archive"));
    }

    wassert(actual_file("testds/.archive/testds-2007/07-07.grib").exists());
    wassert(actual_file("testds/.archive/testds-2007/07-07.grib.metadata").exists());
    wassert(actual_file("testds/.archive/testds-2007/07-07.grib.summary").exists());
    wassert(actual_file("testds/.archive/testds-2007/07-08.grib").not_exists());
    wassert(actual_file("testds/.archive/testds-2007/07-08.grib.metadata").not_exists());
    wassert(actual_file("testds/.archive/testds-2007/07-08.grib.summary").not_exists());
    wassert(actual_file("testds/.archive/testds-2007/10-09.grib").not_exists());
    wassert(actual_file("testds/.archive/testds-2007/10-09.grib.metadata").not_exists());
    wassert(actual_file("testds/.archive/testds-2007/10-09.grib.summary").not_exists());
    wassert(actual_file("testds/2007/07-07.grib").not_exists());
    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/10-09.grib").exists());
});

}


struct Issue103Fixture : public DatasetTest
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

class Tests103 : public FixtureTestCase<Issue103Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests103 test103_ondisk2("arki_dataset_segmented_issue103_ondisk2", "type=ondisk2\n");
Tests103 test103_ondisk2_dir("arki_dataset_segmented_issue103_ondisk2_dir", "type=ondisk2\n", DatasetTest::TEST_FORCE_DIR);
Tests103 test103_simple_plain("arki_dataset_segmented_issue103_simple_plain", "type=simple\nindex_type=plain\n");
Tests103 test103_simple_plain_dir("arki_dataset_segmented_issue103_simple_plain_dir", "type=simple\nindex_type=plain\n", DatasetTest::TEST_FORCE_DIR);
Tests103 test103_simple_sqlite("arki_dataset_segmented_issue103_simple_sqlite", "type=simple\nindex_type=sqlite\n");
Tests103 test103_simple_sqlite_dir("arki_dataset_segmented_issue103_simple_sqlite_dir", "type=simple\nindex_type=sqlite\n", DatasetTest::TEST_FORCE_DIR);
Tests103 test103_iseg("arki_dataset_segmented_issue103_iseg", "type=iseg\nformat=vm2\n");
Tests103 test103_iseg_dir("arki_dataset_segmented_issue103_iseg_dir", "type=iseg\nformat=vm2\n", DatasetTest::TEST_FORCE_DIR);

void Tests103::register_tests() {

// Test datasets with more segments than the number of open files allowed
add_method("issue103", [](Fixture& f) {
    skip_unless_vm2();
    static const unsigned max_files = 100;
    sys::OverrideRlimit(RLIMIT_NOFILE, max_files);

    // TODO: speed up by lowering RLIMIT_NOFILE for the duration of this test

    File sample("inbound/issue103.vm2", O_CREAT | O_WRONLY);
    time_t base = 1507917600;
    for (unsigned i = 0; i < max_files + 1; ++i)
    {
        time_t step = base + i * 86400;
        struct tm t;
        if (gmtime_r(&step, &t) == nullptr)
            throw_system_error("gmtime_r failed");
        char buf[128];
        size_t bufsize = strftime(buf, 128, "%Y%m%d%H%M,1,158,23,,,\n", &t);
        if (bufsize == 0)
            throw_system_error("strftime failed");
        sample.write_all_or_throw(buf, bufsize);
    }
    sample.close();

    // Dispatch
    {
        metadata::TestCollection mds("inbound/issue103.vm2");
        wassert(f.import(mds));
    }

    // Check
    wassert(actual(f.makeSegmentedChecker().get()).check_clean(true));

    // Query without data
    auto reader = f.config().create_reader();
    metadata::Collection mdc;
    wassert(mdc.add(*reader, Matcher::parse("")));
    wassert(actual(mdc.size()) == max_files + 1);
    wassert(actual(mdc[0].sourceBlob().reader).isfalse());

    // Query with data, without storing all the results on a collection
    dataset::DataQuery dq(Matcher::parse(""), true);
    unsigned count = 0;
    wassert(reader->query_data(dq, [&](std::shared_ptr<Metadata> md) {
        wassert(actual(md->sourceBlob().reader).istrue());
        ++count; return true;
    }));
    wassert(actual(count) == max_files + 1);

    // Query with data, sorting, without storing all the results on a collection
    dq.sorter = metadata::sort::Compare::parse("level");
    count = 0;
    wassert(reader->query_data(dq, [&](std::shared_ptr<Metadata> md) {
        wassert(actual(md->sourceBlob().reader).istrue());
        ++count; return true;
    }));
    wassert(actual(count) == max_files + 1);
});

}

class ScanDirTests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_segmented_scan_dir");

void ScanDirTests::register_tests() {

// Test scan_dir on an empty directory
add_method("scan_dir_empty", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);

    {
        std::vector<std::string> res;
        segmented::Checker::scan_dir("dirscanner", [&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
    }
});

// Test scan_dir on a populated directory
add_method("scan_dir_dir1", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);
    sys::write_file("dirscanner/index.sqlite", "");
    mkdir("dirscanner/2007", 0777);
    mkdir("dirscanner/2008", 0777);
    sys::write_file("dirscanner/2008/a.grib", "");
    sys::write_file("dirscanner/2008/b.grib", "");
    sys::write_file("dirscanner/2008/c.grib.gz", "");
    sys::write_file("dirscanner/2008/d.grib.tar", "");
    sys::write_file("dirscanner/2008/e.grib.zip", "");
    mkdir("dirscanner/2008/temp", 0777);
    mkdir("dirscanner/2009", 0777);
    sys::write_file("dirscanner/2009/a.grib", "");
    sys::write_file("dirscanner/2009/b.grib", "");
    mkdir("dirscanner/2009/temp", 0777);
    mkdir("dirscanner/.archive", 0777);
    sys::write_file("dirscanner/.archive/z.grib", "");

    {
        std::vector<std::string> res;
        segmented::Checker::scan_dir("dirscanner", [&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 7u);
        std::sort(res.begin(), res.end());

        wassert(actual(res[0]) == "2008/a.grib");
        wassert(actual(res[1]) == "2008/b.grib");
        wassert(actual(res[2]) == "2008/c.grib");
        wassert(actual(res[3]) == "2008/d.grib");
        wassert(actual(res[4]) == "2008/e.grib");
        wassert(actual(res[5]) == "2009/a.grib");
        wassert(actual(res[6]) == "2009/b.grib");
    }
});

// Test file names interspersed with directory names
add_method("scan_dir_dir2", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);
    sys::write_file("dirscanner/index.sqlite", "");
    mkdir("dirscanner/2008", 0777);
    sys::write_file("dirscanner/2008/a.grib", "");
    sys::write_file("dirscanner/2008/b.grib", "");
    mkdir("dirscanner/2008/a", 0777);
    sys::write_file("dirscanner/2008/a/a.grib", "");
    mkdir("dirscanner/2009", 0777);
    sys::write_file("dirscanner/2009/a.grib", "");
    sys::write_file("dirscanner/2009/b.grib", "");

    {
        std::vector<std::string> res;
        segmented::Checker::scan_dir("dirscanner", [&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 5u);
        std::sort(res.begin(), res.end());

        wassert(actual(res[0]) == "2008/a.grib");
        wassert(actual(res[1]) == "2008/a/a.grib");
        wassert(actual(res[2]) == "2008/b.grib");
        wassert(actual(res[3]) == "2009/a.grib");
        wassert(actual(res[4]) == "2009/b.grib");
    }
});

// odimh5 files are not considered segments
add_method("scan_dir_dir2", [] {
    system("rm -rf dirscanner");
    mkdir("dirscanner", 0777);
    mkdir("dirscanner/2008", 0777);
    sys::write_file("dirscanner/2008/01.odimh5", "");

    {
        std::vector<std::string> res;
        segmented::Checker::scan_dir("dirscanner", [&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
    }

    {
        std::vector<std::string> res;
        segmented::Checker::scan_dir("dirscanner", [&](const std::string& relpath) { res.push_back(relpath); });
        wassert(actual(res.size()) == 0u);
    }
});

}

}
