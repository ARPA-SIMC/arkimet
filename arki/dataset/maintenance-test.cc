#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"
#include "arki/exceptions.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segmented.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/time.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include <cstdlib>
#include <cstdio>
#include <sys/types.h>
#include <utime.h>
#include <unistd.h>

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::utils;
using arki::core::Time;

static const time_t t20070707 = 1183766400;

namespace arki {
namespace dataset {
namespace maintenance_test {

Fixture::Fixture(const std::string& format, const std::string& cfg_instance)
    : DatasetTest(cfg_instance), format(format)
{
    if (format == "grib")
    {
        import_files = { "inbound/mainttest.grib" };
        test_relpath = "2007/07-07.grib";
        test_relpath_wrongstep = "2007/07.grib";
        test_datum_size = 34960;
    }
    else if (format == "bufr")
    {
        import_files = { "inbound/mainttest.bufr" };
        test_relpath = "2007/07-07.bufr";
        test_relpath_wrongstep = "2007/07.bufr";
        test_datum_size = 172;
    }
    else if (format == "vm2")
    {
        import_files = { "inbound/mainttest.vm2" };
        test_relpath = "2007/07-07.vm2";
        test_relpath_wrongstep = "2007/07.vm2";
        test_datum_size = 34;
    }
    else if (format == "odimh5")
    {
        import_files = {
            "inbound/mainttest.h5/00.h5",
            "inbound/mainttest.h5/01.h5",
            "inbound/mainttest.h5/02.h5",
            "inbound/mainttest.h5/03.h5",
        };
        test_relpath = "2007/07-07.odimh5";
        test_relpath_wrongstep = "2007/07.odimh5";
        test_datum_size = 49049;
    }
}

void Fixture::state_is(unsigned segment_count, unsigned test_relpath_state)
{
    auto state = scan_state();
    wassert(actual(state.size()) == segment_count);
    wassert(actual(state.get(test_relpath).state) == test_relpath_state);
}

void Fixture::test_setup()
{
    DatasetTest::test_setup(R"(
        unique=reftime, origin, product, level, timerange, area
        step=daily
    )");

    for (const auto& pathname : import_files)
        import(pathname);
}


MaintenanceTest::~MaintenanceTest()
{
}

void MaintenanceTest::make_hole_start()
{
    fixture->makeSegmentedChecker()->test_make_hole(fixture->test_relpath, 10, 0);
}

void MaintenanceTest::make_hole_middle()
{
    fixture->makeSegmentedChecker()->test_make_hole(fixture->test_relpath, 10, 1);
}

void MaintenanceTest::make_hole_end()
{
    fixture->makeSegmentedChecker()->test_make_hole(fixture->test_relpath, 10, 2);
}

void MaintenanceTest::corrupt_first()
{
    fixture->makeSegmentedChecker()->test_corrupt_data(fixture->test_relpath, 0);
}

void MaintenanceTest::truncate_segment()
{
    fixture->makeSegmentedChecker()->test_truncate_data(fixture->test_relpath, 1);
}

void MaintenanceTest::swap_data()
{
    fixture->makeSegmentedChecker()->test_swap_data(fixture->test_relpath, 0, 1);
}

void MaintenanceTest::remove_index()
{
    fixture->makeSegmentedChecker()->test_remove_index(fixture->test_relpath);
}

void MaintenanceTest::rm_r(const std::string& pathname)
{
    if (sys::isdir(pathname))
        sys::rmtree(pathname);
    else
        sys::unlink(pathname);
}

void MaintenanceTest::touch(const std::string& pathname, time_t ts)
{
    struct utimbuf t = { ts, ts };
    if (::utime(pathname.c_str(), &t) != 0)
        throw_system_error("cannot set mtime/atime of " + pathname);
}

void MaintenanceTest::register_tests_concat()
{
    // Check
    add_method("check_isfile", R"(
        - the segment must be a file
    )", [&](Fixture& f) {
        sys::unlink("testds/" + fixture->test_relpath);
        sys::makedirs("testds/" + fixture->test_relpath);

        wassert(f.state_is(3, SEGMENT_MISSING));
    });
}

void MaintenanceTest::register_tests_dir()
{
    // Check
    add_method("check_isdir", R"(
        - the segment must be a directory [unaligned]
    )", [](Fixture& f) {
        sys::rmtree("testds/" + f.test_relpath);
        sys::write_file("testds/" + f.test_relpath, "");

        wassert(f.state_is(3, SEGMENT_UNALIGNED));
    });

    add_method("check_datasize", R"(
        - the size of each data file must match the data size exactly [corrupted]
    )", [](Fixture& f) {
        {
            sys::File df("testds/" + f.test_relpath + "/000000." + f.format, O_RDWR);
            df.ftruncate(f.test_datum_size + 1);
        }

        wassert(f.state_is(3, SEGMENT_CORRUPTED));
    });
}

void MaintenanceTest::register_tests()
{
    switch (segment_type)
    {
        case SEGMENT_CONCAT: register_tests_concat(); break;
        case SEGMENT_DIR:    register_tests_dir();    break;
        default: throw std::runtime_error("unsupported segment type");
    }

    add_method("clean", [](Fixture& f) {
        wassert(actual(f.makeSegmentedChecker().get()).check_clean());
        wassert(actual(f.makeSegmentedChecker().get()).maintenance_clean(3));
        wassert(actual(f.makeSegmentedChecker().get()).repack_clean());
        wassert(actual(f.makeSegmentedChecker().get()).maintenance_clean(3));
    });

    // Check

    add_method("check_exists", R"(
        - the segment must exist [missing]
    )", [&](Fixture& f) {
        rm_r("testds/" + f.test_relpath);

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get(f.test_relpath).state) == segment::State(SEGMENT_MISSING));
    });

    if (can_delete_data())
        add_method("check_all_removed", R"(
            - segments that only contain data that has been removed are
              identified as fully deleted [deleted]
        )", [&](Fixture& f) {
            {
                metadata::Collection mds(*f.config().create_reader(), Matcher());
                auto writer = f.config().create_writer();
                for (auto& md: mds)
                    writer->remove(*md);
            }

            wassert(f.state_is(3, SEGMENT_DELETED));
        });

    add_method("check_dataexists", R"(
        - all data known by the index for this segment must be present on disk [unaligned]
    )", [&](Fixture& f) {
        truncate_segment();

        wassert(f.state_is(3, SEGMENT_UNALIGNED));
    });

    if (can_detect_overlap())
        add_method("check_data_overlap", R"(
            - no pair of (offset, size) data spans from the index can overlap [unaligned]
        )", [&](Fixture& f) {
            if (segment_type == SEGMENT_DIR)
                f.makeSegmentedChecker()->test_make_overlap(f.test_relpath, 1, 1);
            else
                f.makeSegmentedChecker()->test_make_overlap(f.test_relpath, f.test_datum_size / 2, 1);

            // TODO: should it be CORRUPTED?
            wassert(f.state_is(3, SEGMENT_UNALIGNED));
        });

    add_method("check_hole_start", R"(
        - data must start at the beginning of the segment [dirty]
    )", [&](Fixture& f) {
        make_hole_start();

        wassert(f.state_is(3, SEGMENT_DIRTY));
    });

    add_method("check_hole_middle", R"(
        - there must be no gaps between data in the segment [dirty]
    )", [&](Fixture& f) {
        make_hole_middle();

        wassert(f.state_is(3, SEGMENT_DIRTY));
    });

    add_method("check_hole_end", R"(
        - data must end at the end of the segment [dirty]
    )", [&](Fixture& f) {
        make_hole_end();

        wassert(f.state_is(3, SEGMENT_DIRTY));
    });

    add_method("check_archive_age", R"(
        - find segments that can only contain data older than `archive age` days [archive_age]
    )", [&](Fixture& f) {
        f.cfg.setValue("archive age", "1");
        f.test_reread_config();

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400 - 1);
            wassert(f.state_is(3, SEGMENT_OK));
        }

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);
            wassert(f.state_is(3, SEGMENT_ARCHIVE_AGE));
        }
    });

    add_method("check_delete_age", R"(
        - find segments that can only contain data older than `delete age` days [delete_age]
    )", [&](Fixture& f) {
        f.cfg.setValue("delete age", "1");
        f.test_reread_config();

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400 - 1);
            wassert(f.state_is(3, SEGMENT_OK));
        }

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);
            wassert(f.state_is(3, SEGMENT_DELETE_AGE));
        }
    });

    add_method("check_metadata_reftimes_must_fit_segment", R"(
    - the span of reference times in each segment must fit inside the interval
      implied by the segment file name (FIXME: should this be disabled for
      archives, to deal with datasets that had a change of step in their lifetime?) [corrupted]
    )", [&](Fixture& f) {
        Metadata md = f.import_results[0];
        md.set("reftime", "2007-07-06 00:00:00");
        checker()->test_change_metadata(f.test_relpath, md, 0);

        wassert(f.state_is(3, SEGMENT_CORRUPTED));
    });

    if (can_detect_segments_out_of_step())
        add_method("check_segment_name_must_fit_step", R"(
        - the segment name must represent an interval matching the dataset step
          (FIXME: should this be disabled for archives, to deal with datasets that had
          a change of step in their lifetime?) [corrupted]
        )", [&](Fixture& f) {
            checker()->test_rename(f.test_relpath, f.test_relpath_wrongstep);

            auto state = f.scan_state();
            wassert(actual(state.size()) == 3u);
            wassert(actual(state.get(f.test_relpath_wrongstep).state) == SEGMENT_CORRUPTED);
        });

    add_method("check_isordered", R"(
        - data on disk must match the order of data used by queries [dirty]
    )", [&](Fixture& f) {
        swap_data();

        wassert(f.state_is(3, SEGMENT_DIRTY));
    });


    // Optional thorough check
    add_method("tcheck_corrupted_data", R"(
        - format-specific consistency checks on the content of each file must pass [unaligned]
    )", [&](Fixture& f) {
        corrupt_first();

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr, false);
        wassert(actual(state.size()) == 3u);
        // TODO: should it be CORRUPTED?
        wassert(actual(state.get(f.test_relpath).state) == segment::State(SEGMENT_UNALIGNED));
    });


    // Fix
    add_method("fix_dirty", R"(
        - [dirty] segments are not touched
    )", [&](Fixture& f) {
        make_hole_end();

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "check", "2 files ok");
        wassert(actual(writer.get()).check(e, true));

        wassert(f.state_is(3, SEGMENT_DIRTY));
    });

    add_method("fix_unaligned", R"(
        - [unaligned] segments are imported in-place
    )", [&](Fixture& f) {
        make_unaligned();

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "check", "2 files ok");
        e.rescanned.emplace_back("testds", f.test_relpath);
        wassert(actual(writer.get()).check(e, true));

        wassert(actual(writer.get()).maintenance_clean(3));
    });

    add_method("fix_missing", R"(
        - [missing] segments are removed from the index
    )", [&](Fixture& f) {
        rm_r("testds/" + f.test_relpath);

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "check", "2 files ok");
        e.deindexed.emplace_back("testds", f.test_relpath);
        wassert(actual(writer.get()).check(e, true));

        wassert(actual(writer.get()).maintenance_clean(2));
    });

    add_method("fix_deleted", R"(
        - [deleted] segments are removed from the index
    )", [&](Fixture& f) {
        rm_r("testds/" + f.test_relpath);

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "check", "2 files ok");
        e.deindexed.emplace_back("testds", f.test_relpath);
        wassert(actual(writer.get()).check(e, true));

        wassert(actual(writer.get()).maintenance_clean(2));
    });

    add_method("fix_corrupted", R"(
        - [corrupted] segments can only be fixed by manual intervention. They
          are reported and left untouched
    )", [&](Fixture& f) {
        Metadata md = f.import_results[0];
        md.set("reftime", "2007-07-06 00:00:00");
        checker()->test_change_metadata(f.test_relpath, md, 0);

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "check", "2 files ok");
        wassert(actual(writer.get()).check(e, true));

        auto state = f.scan_state();
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get(f.test_relpath).state) == segment::State(SEGMENT_CORRUPTED));
    });

    add_method("fix_archive_age", R"(
        - [archive age] segments are not touched
    )", [&](Fixture& f) {
        f.cfg.setValue("archive age", "1");
        f.test_reread_config();

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);

            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            wassert(actual(writer.get()).check(e, true));

            wassert(f.state_is(3, SEGMENT_ARCHIVE_AGE));
        }
    });

    add_method("fix_delete_age", R"(
        - [delete age] segments are not touched
    )", [&](Fixture& f) {
        f.cfg.setValue("delete age", "1");
        f.test_reread_config();

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);

            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            wassert(actual(writer.get()).check(e, true));

            wassert(f.state_is(3, SEGMENT_DELETE_AGE));
        }
    });

    // Repack
    add_method("repack_dirty", R"(
        - [dirty] segments are rewritten to be without holes and have data in the right order.
          In concat segments, this is done to guarantee linear disk access when
          data are queried in the default sorting order. In dir segments, this
          is done to avoid sequence numbers growing indefinitely for datasets
          with frequent appends and removes.
    )", [&](Fixture& f) {
        make_hole_middle();
        // swap_data(); // FIXME: swap_data currently isn't detected as dirty on simple datasets

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "repack", "2 files ok");
        e.repacked.emplace_back("testds", f.test_relpath);
        wassert(actual(writer.get()).repack(e, true));

        wassert(actual(writer.get()).maintenance_clean(3));
    });

    // repack_unaligned is implemented in dataset-specific tests

    add_method("repack_missing", R"(
        - [missing] segments are removed from the index
    )", [&](Fixture& f) {
        rm_r("testds/" + f.test_relpath);

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "repack", "2 files ok");
        e.deindexed.emplace_back("testds", f.test_relpath);
        wassert(actual(writer.get()).repack(e, true));

        wassert(actual(writer.get()).maintenance_clean(2));
    });

    add_method("repack_deleted", R"(
        - [deleted] segments are removed from the index
    )", [&](Fixture& f) {
        rm_r("testds/" + f.test_relpath);

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "repack", "2 files ok");
        e.deindexed.emplace_back("testds", f.test_relpath);
        wassert(actual(writer.get()).repack(e, true));

        wassert(actual(writer.get()).maintenance_clean(2));
    });

    add_method("repack_corrupted", R"(
        - [corrupted] segments are not untouched
    )", [&](Fixture& f) {
        Metadata md = f.import_results[0];
        md.set("reftime", "2007-07-06 00:00:00");
        checker()->test_change_metadata(f.test_relpath, md, 0);

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "repack", "2 files ok");
        wassert(actual(writer.get()).repack(e, true));

        wassert(f.state_is(3, SEGMENT_CORRUPTED));
    });


    add_method("repack_archive_age", R"(
        - [archive age] segments are repacked if needed, then moved to .archive/last
    )", [&](Fixture& f) {
        make_hole_middle();
        f.cfg.setValue("archive age", "1");
        f.test_reread_config();

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);

            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.repacked.emplace_back("testds", f.test_relpath);
            e.archived.emplace_back("testds", f.test_relpath);
            wassert(actual(writer.get()).repack(e, true));

            wassert(actual(writer.get()).maintenance_clean(2));
        }
    });

    add_method("repack_delete_age", R"(
        - [delete age] segments are deleted
    )", [&](Fixture& f) {
        make_hole_middle();
        f.cfg.setValue("delete age", "1");
        f.test_reread_config();

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);

            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.deleted.emplace_back("testds", f.test_relpath);
            wassert(actual(writer.get()).repack(e, true));

            wassert(actual(writer.get()).maintenance_clean(2));
        }
    });
}

}
}
}

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            unique=reftime, origin, product, level, timerange, area
            step=daily
        )");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override
    {
        add_method("scan_hugefile", [](Fixture& f) {
            // Test accuracy of maintenance scan, on a dataset with a data file larger than 2**31
            if (f.cfg.value("type") == "simple") return; // TODO: we need to avoid the SLOOOOW rescan done by simple on the data file
            f.clean();

            // Simulate 2007/07-07.grib to be 6G already
            system("mkdir -p testds/2007");
            system("touch testds/2007/07-07.grib");
            // Truncate the last grib out of a file
            if (truncate("testds/2007/07-07.grib", 6000000000LLU) < 0)
                throw_system_error("truncating testds/2007/07-07.grib");

            f.import();

            {
                auto writer(f.config().create_checker());
                ReporterExpected e;
                e.report.emplace_back("testds", "check", "2 files ok");
                e.repacked.emplace_back("testds", "2007/07-07.grib");
                wassert(actual(writer.get()).check(e, false));
            }

#if 0
            stringstream s;

        // Rescanning a 6G+ file with grib_api is SLOW!

            // Perform full maintenance and check that things are still ok afterwards
            writer.check(s, true, false);
            ensure_equals(s.str(),
                "testdir: rescanned 2007/07.grib\n"
                "testdir: 1 file rescanned, 7736 bytes reclaimed cleaning the index.\n");
            c.clear();
            writer.maintenance(c);
            ensure_equals(c.count(COUNTED_OK), 1u);
            ensure_equals(c.count(COUNTED_DIRTY), 1u);
            ensure_equals(c.remaining(), "");
            ensure(not c.isClean());

            // Perform packing and check that things are still ok afterwards
            s.str(std::string());
            writer.repack(s, true);
            ensure_equals(s.str(), 
                "testdir: packed 2007/07.grib (34960 saved)\n"
                "testdir: 1 file packed, 2576 bytes reclaimed on the index, 37536 total bytes freed.\n");
            c.clear();

            // Maintenance and pack are ok now
            writer.maintenance(c, false);
            ensure_equals(c.count(COUNTED_OK), 2u);
            ensure_equals(c.remaining(), "");
            ensure(c.isClean());
                s.str(std::string());
                writer.repack(s, true);
                ensure_equals(s.str(), string()); // Nothing should have happened
                c.clear();

            // Ensure that we have the summary cache
            ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
            ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
            ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
#endif
        });
        add_method("repack_timestamps", [](Fixture& f) {
            // Ensure that if repacking changes the data file timestamp, it reindexes it properly
            f.clean_and_import();

            // Ensure the archive appears clean
            wassert(actual(f.makeSegmentedChecker().get()).maintenance_clean(3));

            // Change timestamp and rescan the file
            {
                struct utimbuf oldts = { 199926000, 199926000 };
                wassert(actual(utime("testds/2007/07-08.grib", &oldts)) == 0);

                auto writer(f.makeSegmentedChecker());
                writer->rescanSegment("2007/07-08.grib");
            }

            // Ensure that the archive is still clean
            wassert(actual(f.makeSegmentedChecker().get()).maintenance_clean(3));

            // Repack the file
            {
                auto writer(f.makeSegmentedChecker());
                wassert(actual(writer->repackSegment("2007/07-08.grib")) == 0u);
            }

            // Ensure that the archive is still clean
            wassert(actual(f.makeSegmentedChecker().get()).maintenance_clean(3));
        });
        add_method("repack_delete", [](Fixture& f) {
            // Test accuracy of maintenance scan, on a dataset with one file to both repack and delete
            f.cfg.setValue("step", "yearly");

            testdata::GRIBData data;
            f.import_all(data);
            f.test_reread_config();

            // Data are from 07, 08, 10 2007
            Time treshold(2008, 1, 1);
            Time now = Time::create_now();
            long long int duration = Time::duration(treshold, now);
            f.cfg.setValue("delete age", duration/(3600*24));

            {
                auto writer(f.makeSegmentedChecker());
                arki::tests::MaintenanceResults expected(false, 1);
                // A repack is still needed because the data is not sorted by reftime
                expected.by_type[DatasetTest::COUNTED_DIRTY] = 1;
                // And the same file is also old enough to be deleted
                expected.by_type[DatasetTest::COUNTED_DELETE_AGE] = 1;
                wassert(actual(writer.get()).maintenance(expected));
            }

            // Perform packing and check that things are still ok afterwards
            {
                auto writer(f.makeSegmentedChecker());
                ReporterExpected e;
                e.deleted.emplace_back("testds", "20/2007.grib");
                wassert(actual(writer.get()).repack(e, true));
            }
            wassert(actual(f.makeSegmentedChecker().get()).maintenance_clean(0));

            // Perform full maintenance and check that things are still ok afterwards
            {
                auto checker(f.makeSegmentedChecker());
                wassert(actual(checker.get()).check_clean(true, true));
            }
        });
        add_method("scan_repack_archive", [](Fixture& f) {
            // Test accuracy of maintenance scan, on a dataset with one file to both repack and archive
            f.cfg.setValue("step", "yearly");

            testdata::GRIBData data;
            f.import_all(data);
            f.test_reread_config();

            // Data are from 07, 08, 10 2007
            Time treshold(2008, 1, 1);
            Time now = Time::create_now();
            long long int duration = Time::duration(treshold, now);
            f.cfg.setValue("archive age", duration/(3600*24));

            {
                auto writer(f.makeSegmentedChecker());
                MaintenanceResults expected(false, 1);
                // A repack is still needed because the data is not sorted by reftime
                expected.by_type[DatasetTest::COUNTED_DIRTY] = 1;
                // And the same file is also old enough to be deleted
                expected.by_type[DatasetTest::COUNTED_ARCHIVE_AGE] = 1;
                wassert(actual(writer.get()).maintenance(expected));
            }

            // Perform packing and check that things are still ok afterwards
            {
                auto writer(f.makeSegmentedChecker());

                ReporterExpected e;
                e.repacked.emplace_back("testds", "20/2007.grib");
                e.archived.emplace_back("testds", "20/2007.grib");
                wassert(actual(writer.get()).repack(e, true));
            }
        });
    }
};

Tests test_ondisk2("arki_dataset_maintenance_ondisk2", "type=ondisk2\n");
Tests test_simple_plain("arki_dataset_maintenance_simple_plain", "type=simple\nindex_type=plain\n");
Tests test_simple_sqlite("arki_dataset_maintenance_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests test_ondisk2_dir("arki_dataset_maintenance_ondisk2_dirs", "type=ondisk2\nsegments=dir\n");
Tests test_simple_plain_dir("arki_dataset_maintenance_simple_plain_dirs", "type=simple\nindex_type=plain\nsegments=dir\n");
Tests test_simple_sqlite_dir("arki_dataset_maintenance_simple_sqlite_dirs", "type=simple\nindex_type=sqlite\nsegments=dir\n");
Tests test_iseg("arki_dataset_maintenance_iseg", "type=iseg\nformat=grib\n");
Tests test_iseg_dir("arki_dataset_maintenance_iseg_dir", "type=iseg\nformat=grib\nsegments=dir\n");

}
