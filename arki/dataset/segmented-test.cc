#include "tests.h"
#include "arki/segment/data/tests.h"
#include "segmented.h"
#include "maintenance.h"
#include "arki/exceptions.h"
#include "arki/core/file.h"
#include "arki/core/lock.h"
#include "arki/stream.h"
#include "arki/dataset/time.h"
#include "arki/query.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/matcher/parser.h"
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

Tests test_simple_plain("arki_dataset_segmented_simple", "type=simple");
Tests test_iseg("arki_dataset_segmented_iseg", "type=iseg\nformat=grib");

void Tests::register_tests() {

add_method("gz", [](Fixture& f) {
    // Import and compress all the files
    f.clean_and_import();

    // Test moving into archive data that have been compressed
    f.cfg->set("archive age", days_since(2007, 9, 1));
    f.cfg->set("gz group size", 0);
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
        wassert(actual(state.get("testds:2007/07-07.grib").state) == segment::SEGMENT_ARCHIVE_AGE);
        wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::SEGMENT_ARCHIVE_AGE);
        wassert(actual(state.get("testds:2007/10-09.grib").state) == segment::SEGMENT_OK);
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
        Time t = md->get<types::reftime::Position>()->get_Position();
        t.se = 30;
        md->test_set(types::Reftime::createPosition(t));
    }
    wassert(f.import(mds));

    // Test moving into archive data that have been compressed
    f.cfg->set("archive age", days_since(2007, 9, 1));
    f.cfg->set("gz group size", 1);
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
        wassert(actual(state.get("testds:2007/07-07.grib").state) == segment::SEGMENT_ARCHIVE_AGE);
        wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::SEGMENT_ARCHIVE_AGE);
        wassert(actual(state.get("testds:2007/10-09.grib").state) == segment::SEGMENT_OK);
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
    f.cfg->set("archive age", days_since(2007, 9, 1));
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
        wassert(actual(state.get("testds:2007/07-07.grib").state) == segment::SEGMENT_ARCHIVE_AGE);
        wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::SEGMENT_ARCHIVE_AGE);
        wassert(actual(state.get("testds:2007/10-09.grib").state) == segment::SEGMENT_OK);
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
    f.cfg->set("archive age", days_since(2007, 9, 1));
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
        wassert(actual(state.get("testds:2007/07-07.grib").state) == segment::SEGMENT_ARCHIVE_AGE);
        wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::SEGMENT_ARCHIVE_AGE);
        wassert(actual(state.get("testds:2007/10-09.grib").state) == segment::SEGMENT_OK);
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
    matcher::Parser parser;
    f.clean_and_import();
    f.cfg->set("archive age", days_since(2007, 9, 1));
    f.test_reread_config();
    {
        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib");
        e.archived.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(writer.get()).repack(e, true));
    }

    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, "");
    wassert(actual(mdc.size()) == 3u);

    mdc.clear();
    mdc.add(*reader, query::Data(parser.parse("origin:GRIB1,200"), true));
    wassert(actual(mdc.size()) == 1u);

    // Check that the source record that comes out is ok
    //wassert(actual_type(mdc[0].source()).is_source_inline("grib", 7218));

    // Check that data is accessible
    const auto& buf = mdc[0].get_data().read();
    wassert(actual(buf.size()) == 7218u);

    mdc.clear();
    mdc.add(*reader, "reftime:=2007-07-08");
    wassert(actual(mdc.size()) == 1u);
    wassert(actual(mdc[0].data_size()) == 7218u);

    mdc.clear();
    mdc.add(*reader, "origin:GRIB1,80");
    wassert(actual(mdc.size()) == 1u);

    mdc.clear();
    mdc.add(*reader, "origin:GRIB1,98");
    wassert(actual(mdc.size()) == 1u);

    // Query bytes
    {
        std::vector<uint8_t> buf;
        auto out(StreamOutput::create(buf));
        query::Bytes bq;
        bq.setData(Matcher());
        reader->query_bytes(bq, *out);
        wassert(actual(buf.size()) == 44412u);
    }

    {
        std::vector<uint8_t> buf;
        auto out(StreamOutput::create(buf));
        query::Bytes bq;
        bq.setData(parser.parse("origin:GRIB1,200"));
        reader->query_bytes(bq, *out);
        wassert(actual(buf.size()) == 7218u);
    }

    {
        std::vector<uint8_t> buf;
        auto out(StreamOutput::create(buf));
        query::Bytes bq;
        bq.setData(parser.parse("reftime:=2007-07-08"));
        reader->query_bytes(bq, *out);
        wassert(actual(buf.size()) == 7218u);
    }

    /* TODO
        case BQ_POSTPROCESS:
        case BQ_REP_METADATA:
        case BQ_REP_SUMMARY:
    virtual void query_summary(const Matcher& matcher, Summary& summary);
    */

    // Query summary
    Summary s;
    reader->query_summary("", s);
    wassert(actual(s.count()) == 3u);
    wassert(actual(s.size()) == 44412u);

    s.clear();
    reader->query_summary("origin:GRIB1,200", s);
    wassert(actual(s.count()) == 1u);
    wassert(actual(s.size()) == 7218u);

    s.clear();
    reader->query_summary("reftime:=2007-07-08", s);
    wassert(actual(s.count()) == 1u);
    wassert(actual(s.size()) == 7218u);
});
add_method("empty_dirs", [](Fixture& f) {
    matcher::Parser parser;
    // Tolerate empty dirs
    // Start with an empty dir
    f.clean();

    auto reader = f.config().create_reader();

    metadata::Collection mdc(*reader, Matcher());
    wassert_true(mdc.empty());

    Summary s;
    reader->query_summary("", s);
    wassert(actual(s.count()) == 0u);

    std::vector<uint8_t> buf;
    auto out(StreamOutput::create(buf));
    query::Bytes bq;
    bq.setData(parser.parse(""));
    reader->query_bytes(bq, *out);
    wassert(actual(buf.size()) == 0u);
});
add_method("query_lots", [](Fixture& f) {
    // Test querying with lots of data, to trigger on disk metadata buffering
    using namespace arki::types;
    matcher::Parser parser;

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
                            md->set_source_inline(DataFormat::VM2, metadata::DataManager::get().to_data(DataFormat::VM2, vector<uint8_t>(buf, buf+len)));
                            md->add_note("Generated from memory");
                            md->test_set(Reftime::createPosition(Time(2013, month, day, hour, 0, 0)));
                            md->test_set<area::VM2>(station);
                            md->test_set(Product::createVM2(varid));
                            snprintf(buf, 40, "%d,,,000000000", value);
                            md->test_set(types::Value::create(buf));
                            mds.acquire(std::move(md));
                        }

        wassert(f.import(mds));
    }

    utils::files::removeDontpackFlagfile(f.cfg->value("path"));

    // Query all the dataset and make sure the results are in the right order
    struct CheckSortOrder
    {
        uint64_t last_value;
        unsigned seen;
        CheckSortOrder() : last_value(0), seen(0) {}
        virtual ~CheckSortOrder() {}
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
        uint64_t make_key(const Metadata& md) const override
        {
            const reftime::Position* rt = md.get<types::reftime::Position>();
            auto time = rt->get_Position();
            return time.mo * 10000 + time.da * 100 + time.ho;
        }
    };

    struct CheckAllSortOrder : public CheckSortOrder
    {
        uint64_t make_key(const Metadata& md) const override
        {
            const reftime::Position* rt = md.get<types::reftime::Position>();
            auto time = rt->get_Position();
            const area::VM2* area = dynamic_cast<const area::VM2*>(md.get(TYPE_AREA));
            const Product* prod = md.get<Product>();
            uint64_t dt = time.mo * 10000 + time.da * 100 + time.ho;
            unsigned vi;
            prod->get_VM2(vi);
            auto sid = area->get_VM2();
            return dt * 100 + sid * 10 + vi;
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
        query::Data dq(parser.parse(""));
        dq.sorter = metadata::sort::Compare::parse("reftime,area,product");
        reader->query_data(dq, [&](std::shared_ptr<Metadata> md) { return cso.eat(md); });
        wassert(actual(cso.seen) == 16128u);
    }
});

// Ensure that the segment is not archived if it can still accept new data
// after the archive cutoff (regardless of the data currently in the
// segment)
add_method("archive_age", [](Fixture& f) {
    f.cfg->set("step", "yearly");
    f.test_reread_config();

    // Import a file with a known reftime
    // Reftime: 2007-07-08T13:00:00Z
    metadata::TestCollection mds("inbound/test.grib1");
    wassert(actual(*f.makeSegmentedWriter()).acquire_ok(mds.get(0)));
    wassert(actual(f.makeSegmentedChecker().get()).check_clean(true));

    // TZ=UTC date --date="2008-01-01 00:00:00" +%s
    time_t start2008 = 1199145600;
    f.cfg->set("archive age", "1");
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
    f.cfg->set("step", "yearly");
    f.test_reread_config();

    // Import a file with a known reftime
    // Reftime: 2007-07-08T13:00:00Z
    metadata::TestCollection mds("inbound/test.grib1");
    wassert(actual(*f.makeSegmentedWriter()).acquire_ok(mds.get(0)));
    wassert(actual(f.makeSegmentedChecker().get()).check_clean(true));

    // TZ=UTC date --date="2008-01-01 00:00:00" +%s
    time_t start2008 = 1199145600;
    f.cfg->set("delete age", "1");
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
    f.cfg->set("archive age", "1");

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
        auto checker = f.makeSegmentedChecker();
        auto segment = checker->dataset().segment_session->segment_from_relpath("2007/07-07.grib");
        wassert(checker->segment(segment)->unarchive());
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
    f.cfg->set("archive age", "1");

    f.test_reread_config();

    // Archive one segment
    {
        auto o = dataset::SessionTime::local_override(now);
        ReporterExpected e;
        e.archived.emplace_back("testds", "2007/07-07.grib");
        wassert(actual(f.makeSegmentedChecker().get()).repack(e, true));
    }

    // Check that segments are where we expect them
    std::filesystem::rename("testds/.archive/last/2007", "testds/.archive/testds-2007");
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
        auto checker = f.makeSegmentedChecker();
        auto segment = checker->dataset().segment_session->segment_from_relpath("../test-ds/2007/07-07.grib");
        checker->segment(segment)->unarchive();
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

// Test scan_dir on an empty directory
add_method("scan_dir_empty", [](Fixture& f) {
    sys::rmtree_ifexists("testds");
    mkdir("testds", 0777);

    auto checker(f.makeSegmentedChecker());
    std::vector<std::string> res;
    wassert(checker->scan_dir([&](auto segment) { res.push_back(segment->relpath()); }));
    wassert(actual(res.size()) == 0u);
});

// Test scan_dir on a populated directory
add_method("scan_dir_dir1", [](Fixture& f) {
    sys::rmtree_ifexists("testds");
    mkdir("testds", 0777);
    sys::write_file("testds/MANIFEST", "");
    mkdir("testds/2007", 0777);
    mkdir("testds/2008", 0777);
    sys::write_file("testds/2008/a.grib", "");
    sys::write_file("testds/2008/b.grib", "");
    sys::write_file("testds/2008/c.grib.gz", "");
    sys::write_file("testds/2008/d.grib.tar", "");
    sys::write_file("testds/2008/e.grib.zip", "");
    mkdir("testds/2008/temp", 0777);
    mkdir("testds/2009", 0777);
    sys::write_file("testds/2009/a.grib", "");
    sys::write_file("testds/2009/b.grib", "");
    mkdir("testds/2009/temp", 0777);
    mkdir("testds/.archive", 0777);
    sys::write_file("testds/.archive/z.grib", "");

    auto checker(wcallchecked(f.makeSegmentedChecker()));
    std::vector<std::string> res;
    wassert(checker->scan_dir([&](auto segment) { res.push_back(segment->relpath()); }));
    wassert(actual(res.size()) == 7u);
    std::sort(res.begin(), res.end());

    wassert(actual(res[0]) == "2008/a.grib");
    wassert(actual(res[1]) == "2008/b.grib");
    wassert(actual(res[2]) == "2008/c.grib");
    wassert(actual(res[3]) == "2008/d.grib");
    wassert(actual(res[4]) == "2008/e.grib");
    wassert(actual(res[5]) == "2009/a.grib");
    wassert(actual(res[6]) == "2009/b.grib");
});

// Test file names interspersed with directory names
add_method("scan_dir_dir2", [](Fixture& f) {
    sys::rmtree_ifexists("testds");
    mkdir("testds", 0777);
    sys::write_file("testds/MANIFEST", "");
    mkdir("testds/2008", 0777);
    sys::write_file("testds/2008/a.grib", "");
    sys::write_file("testds/2008/b.grib", "");
    mkdir("testds/2008/a", 0777);
    sys::write_file("testds/2008/a/a.grib", "");
    mkdir("testds/2009", 0777);
    sys::write_file("testds/2009/a.grib", "");
    sys::write_file("testds/2009/b.grib", "");

    auto checker(wcallchecked(f.makeSegmentedChecker()));
    std::vector<std::string> res;
    wassert(checker->scan_dir([&](auto segment) { res.push_back(segment->relpath()); }));
    wassert(actual(res.size()) == 5u);
    std::sort(res.begin(), res.end());

    wassert(actual(res[0]) == "2008/a.grib");
    wassert(actual(res[1]) == "2008/a/a.grib");
    wassert(actual(res[2]) == "2008/b.grib");
    wassert(actual(res[3]) == "2009/a.grib");
    wassert(actual(res[4]) == "2009/b.grib");
});

// odimh5 files are not considered segments
add_method("scan_dir_dir2", [](Fixture& f) {
    sys::rmtree_ifexists("testds");
    mkdir("testds", 0777);
    mkdir("testds/2008", 0777);
    sys::write_file("testds/2008/01.odimh5", "");

    auto checker(f.makeSegmentedChecker());
    std::vector<std::string> res;
    wassert(checker->scan_dir([&](auto segment) { res.push_back(segment->relpath()); }));
    wassert(actual(res.size()) == 0u);
});

add_method("no_writes_when_repacking", [](Fixture& f) {
    matcher::Parser parser;
    std::filesystem::path relpath("2007/07-07.grib");
    f.clean_and_import();
    // Make a hole so the segment needs repack
    f.makeSegmentedChecker()->test_make_hole(relpath, 10, 0);

    auto dataset = f.segmented_config();

    // Install hooks
    bool on_check_lock_triggered = false;
    bool on_repack_write_lock_triggered = false;
    auto test_hooks = std::make_shared<segmented::TestHooks>();
    test_hooks->on_check_lock = [&](const Segment& segment) {
        if (segment.relpath() != relpath)
            return;
        on_check_lock_triggered = true;

        // When repack is still checking, we can still query
        auto reader = dataset->create_reader();
        unsigned count = 0;
        reader->query_data(parser.parse("reftime:=2007-07-07"), [&](const auto&) noexcept { ++count; return true; });
        wassert(actual(count) == 1u);

        // When repack is still checking, we cannot append
        auto writer = dataset->create_writer();
        metadata::InboundBatch batch;
        batch.add(f.import_results.get(1));
        wassert_throws(lock::locked_error, writer->acquire_batch(batch));
    };
    test_hooks->on_repack_write_lock = [&](const Segment& segment) {
        if (segment.relpath() != relpath)
            return;
        on_repack_write_lock_triggered = true;

        // When repack is fixing, reading fails
        auto reader = dataset->create_reader();
        wassert_throws(lock::locked_error, reader->query_data(parser.parse("reftime:=2007-07-07"), [&](const auto&) noexcept {return true;}));

        // When repack is fixing, appending fails
        auto writer = dataset->create_writer();
        metadata::InboundBatch batch;
        batch.add(f.import_results.get(1));
        wassert_throws(lock::locked_error, writer->acquire_batch(batch));
    };
    dataset->test_hooks = test_hooks;

    auto checker = dataset->create_checker();
    CheckerConfig checker_config;
    checker_config.readonly = false;
    checker->repack(checker_config);
    wassert_true(on_check_lock_triggered);
    wassert_true(on_repack_write_lock_triggered);
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

Tests103 test103_simple("arki_dataset_segmented_issue103_simple", "type=simple");
Tests103 test103_simple_dir("arki_dataset_segmented_issue103_simple_dir", "type=simple", DatasetTest::TEST_FORCE_DIR);
Tests103 test103_iseg("arki_dataset_segmented_issue103_iseg", "type=iseg\nformat=vm2\n");
Tests103 test103_iseg_dir("arki_dataset_segmented_issue103_iseg_dir", "type=iseg\nformat=vm2\n", DatasetTest::TEST_FORCE_DIR);

void Tests103::register_tests() {

// Test datasets with more segments than the number of open files allowed
add_method("issue103", [](Fixture& f) {
    skip_unless_vm2();
    static const unsigned max_files = 100;
    sys::OverrideRlimit(RLIMIT_NOFILE, max_files);
    matcher::Parser parser;

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
    wassert(mdc.add(*reader, ""));
    wassert(actual(mdc.size()) == max_files + 1);
    wassert(actual(mdc[0].sourceBlob().reader).isfalse());

    // Query with data, without storing all the results on a collection
    query::Data dq(parser.parse(""), true);
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

}
