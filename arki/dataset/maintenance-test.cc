#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"
#include "arki/exceptions.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segmented.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/time.h"
#include "arki/reader.h"
#include "arki/reader/registry.h"
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

void MaintenanceTest::delete_one_in_segment()
{
    metadata::Collection mds(*fixture->config().create_reader(), Matcher());
    auto writer = fixture->config().create_writer();
    writer->remove(mds[0]);
}

void MaintenanceTest::delete_all_in_segment()
{
    metadata::Collection mds(*fixture->config().create_reader(), Matcher());
    auto writer = fixture->config().create_writer();
    writer->remove(mds[0]);
    writer->remove(mds[1]);
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

void MaintenanceTest::make_hugefile()
{
    // Pretend that the test segment is 6G already
    {
        sys::File fd("testds/" + fixture->test_relpath, O_RDWR);
        sys::PreserveFileTimes pt(fd);
        fd.ftruncate(6000000000LLU);
    }

    // Import a new datum, that will get appended to it
    {
        Metadata md = fixture->import_results[0];
        md.set("reftime", "2007-07-07 06:00:00");
        md.makeInline();

        auto writer(fixture->makeSegmentedWriter());
        wassert(actual(writer->acquire(md)) == dataset::Writer::ACQ_OK);

        wassert(actual(md.sourceBlob().offset) == 6000000000LLU);
    }
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
        wassert(f.query_results({1, 3, 0, 2}));
    });

    add_method("check_hugefile", [&](Fixture& f) {
        make_hugefile();
        wassert(f.state_is(3, SEGMENT_DIRTY));
        wassert(f.query_results({1, -1, 3, 0, 2}));
    });

    add_method("fix_hugefile", [&](Fixture& f) {
        make_hugefile();
        wassert(f.state_is(3, SEGMENT_DIRTY));

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "check", "2 files ok");
        wassert(actual(writer.get()).check(e, true));

        wassert(f.state_is(3, SEGMENT_DIRTY));
        wassert(f.query_results({1, -1, 3, 0, 2}));
    });

    add_method("repack_hugefile", [&](Fixture& f) {
        make_hugefile();
        wassert(f.state_is(3, SEGMENT_DIRTY));

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.repacked.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).repack(e, true));
        }

        wassert(f.all_clean(3));
        wassert(f.query_results({1, -1, 3, 0, 2}));
    });

    add_method("repack_timestamps", [&](Fixture& f) {
        // Set the segment to a past timestamp and rescan it, making it as if
        // it were imported a long time ago
        {
            touch("testds/" + f.test_relpath, 199926000);

            f.makeSegmentedChecker()->rescanSegment(f.test_relpath);
        }

        // Ensure that the archive is still clean
        wassert(f.all_clean(3));

        // Do a repack, it should change the timestamp
        {
            auto checker(f.makeSegmentedChecker());
            wassert(actual(checker->segment(f.test_relpath)->repack()) == 0u);
        }

        // Ensure that the archive is still clean
        wassert(f.all_clean(3));
        wassert(f.query_results({1, 3, 0, 2}));
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

        wassert(f.state_is(3, SEGMENT_MISSING));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    add_method("check_datasize", R"(
        - the size of each data file must match the data size exactly [corrupted]
    )", [](Fixture& f) {
        {
            sys::File df("testds/" + f.test_relpath + "/000000." + f.format, O_RDWR);
            df.ftruncate(f.test_datum_size + 1);
        }

        wassert(f.state_is(3, SEGMENT_CORRUPTED));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    add_method("check_ignore_dir_timestamp", R"(
       - the modification time of a directory segment can vary unpredictably,
         so it is ignored. The modification time of the sequence file is used
         instead.
    )", [&](Fixture& f) {
        touch("testds/" + f.test_relpath, time(NULL) + 86400);
        wassert(f.all_clean(3));
        wassert(f.query_results({1, 3, 0, 2}));
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
        wassert(f.all_clean(3));
        wassert(actual(f.makeSegmentedChecker().get()).repack_clean());
        wassert(f.all_clean(3));

        // Check that maintenance does not accidentally create an archive
        wassert(actual_file("testds/.archive").not_exists());
        wassert(f.query_results({1, 3, 0, 2}));
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
        wassert(f.query_results({1, 3, 0, 2}));
    });

    if (can_delete_data())
    {
        add_method("check_one_removed", R"(
            - segments that contain some data that has been removed are
              identified as to be repacked [dirty]
        )", [&](Fixture& f) {
            delete_one_in_segment();
            wassert(f.state_is(3, SEGMENT_DIRTY));
            wassert(f.query_results({3, 0, 2}));
        });

        add_method("check_all_removed", R"(
            - segments that only contain data that has been removed are
              identified as fully deleted [deleted]
        )", [&](Fixture& f) {
            delete_all_in_segment();
            wassert(f.state_is(3, SEGMENT_DELETED));
            wassert(f.query_results({0, 2}));
        });

        add_method("fix_one_removed", [&](Fixture& f) {
            delete_one_in_segment();

            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            wassert(actual(writer.get()).check(e, true));

            auto state = f.scan_state();
            wassert(actual(state.size()) == 3u);
            wassert(actual(state.get(f.test_relpath).state) == segment::State(SEGMENT_DIRTY));

            wassert(f.query_results({3, 0, 2}));
        });

        add_method("fix_deleted", R"(
            - [deleted] segments are left untouched
        )", [&](Fixture& f) {
            delete_all_in_segment();

            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            wassert(actual(writer.get()).check(e, true));

            auto state = f.scan_state();
            wassert(actual(state.size()) == 3u);
            wassert(actual(state.get(f.test_relpath).state) == segment::State(SEGMENT_DELETED));

            wassert(f.query_results({0, 2}));
        });

        add_method("repack_deleted", R"(
            - [deleted] segments are removed from disk
        )", [&](Fixture& f) {
            delete_all_in_segment();

            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.deleted.emplace_back("testds", f.test_relpath);
            wassert(actual(writer.get()).repack(e, true));

            wassert(f.ensure_localds_clean(2, 2));
            wassert(f.query_results({0, 2}));
        });

        add_method("repack_one_removed", [&](Fixture& f) {
            delete_one_in_segment();

            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.repacked.emplace_back("testds", f.test_relpath);
            wassert(actual(writer.get()).repack(e, true));

            wassert(f.ensure_localds_clean(3, 3));
            wassert(f.query_results({3, 0, 2}));
        });
    }

    add_method("check_dataexists", R"(
        - all data known by the index for this segment must be present on disk [unaligned]
    )", [&](Fixture& f) {
        truncate_segment();

        wassert(f.state_is(3, SEGMENT_UNALIGNED));
        wassert(f.query_results({1, 3, 0, 2}));
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
            wassert(f.query_results({1, 3, 0, 2}));
        });

    add_method("check_hole_start", R"(
        - data must start at the beginning of the segment [dirty]
    )", [&](Fixture& f) {
        make_hole_start();

        wassert(f.state_is(3, SEGMENT_DIRTY));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    add_method("check_hole_middle", R"(
        - there must be no gaps between data in the segment [dirty]
    )", [&](Fixture& f) {
        make_hole_middle();

        wassert(f.state_is(3, SEGMENT_DIRTY));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    add_method("check_hole_end", R"(
        - data must end at the end of the segment [dirty]
    )", [&](Fixture& f) {
        make_hole_end();

        wassert(f.state_is(3, SEGMENT_DIRTY));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    add_method("check_archive_age", R"(
        - find segments that can only contain data older than `archive age` days [archive_age]
    )", [&](Fixture& f) {
        f.cfg.setValue("archive age", "1");
        f.test_reread_config();

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400 - 1);
            wassert(f.state_is(3, SEGMENT_OK));
            wassert(f.query_results({1, 3, 0, 2}));
        }

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);
            wassert(f.state_is(3, SEGMENT_ARCHIVE_AGE));
            wassert(f.query_results({1, 3, 0, 2}));
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
            wassert(f.query_results({1, 3, 0, 2}));
        }

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);
            wassert(f.state_is(3, SEGMENT_DELETE_AGE));
            wassert(f.query_results({1, 3, 0, 2}));
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
        wassert(f.query_results({-1, 3, 0, 2}));
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

            // We are breaking the invariant that segments sorted by file name
            // are in the same sequence of segments sorted by time of file
            // contents, but the invariant that data queried is sorted by
            // reftime must still stand
            wassert(f.query_results({1, 3, 0, 2}));
        });

    add_method("check_isordered", R"(
        - data on disk must match the order of data used by queries [dirty]
    )", [&](Fixture& f) {
        swap_data();

        wassert(f.state_is(3, SEGMENT_DIRTY));
        wassert(f.query_results({1, 3, 0, 2}));
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
        wassert(f.query_results({1, 3, 0, 2}));
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
        wassert(f.query_results({1, 3, 0, 2}));
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

        wassert(f.ensure_localds_clean(3, 4));
        wassert(f.query_results({1, 3, 0, 2}));
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

        wassert(f.ensure_localds_clean(2, 2));
        wassert(f.query_results({0, 2}));
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
        wassert(f.query_results({-1, 3, 0, 2}));
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
        wassert(f.query_results({1, 3, 0, 2}));
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
        wassert(f.query_results({1, 3, 0, 2}));
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

        wassert(f.ensure_localds_clean(3, 4));
        wassert(f.query_results({1, 3, 0, 2}));
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

        wassert(f.ensure_localds_clean(2, 2));
        wassert(f.query_results({0, 2}));
    });

    add_method("repack_corrupted", R"(
        - [corrupted] segments are not touched
    )", [&](Fixture& f) {
        Metadata md = f.import_results[0];
        md.set("reftime", "2007-07-06 00:00:00");
        checker()->test_change_metadata(f.test_relpath, md, 0);

        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "repack", "2 files ok");
        wassert(actual(writer.get()).repack(e, true));

        wassert(f.state_is(3, SEGMENT_CORRUPTED));
        wassert(f.query_results({-1, 3, 0, 2}));
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

            wassert(f.ensure_localds_clean(2, 4));
        }

        // Check that the files have been moved to the archive
        wassert(actual_file("testds/" + f.test_relpath).not_exists());
        wassert(actual_file("testds/.archive/last/" + f.test_relpath).exists());
        wassert(actual_file("testds/.archive/last/" + f.test_relpath + ".metadata").exists());
        wassert(actual_file("testds/.archive/last/" + f.test_relpath + ".summary").exists());

        wassert(f.query_results({1, 3, 0, 2}));
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

            wassert(f.ensure_localds_clean(2, 2));
        }

        wassert(f.query_results({0, 2}));
    });

    add_method("repack_delete", R"(
        - [delete age] [dirty] a segment that needs to be both repacked and
          deleted, gets deleted without repacking
    )", [&](Fixture& f) {
        make_hole_middle();
        f.cfg.setValue("delete age", "1");
        f.test_reread_config();

        auto o = SessionTime::local_override(t20070707 + 2 * 86400);

        // State knows of a segment both to repack and to delete
        {
            auto state = f.scan_state();
            wassert(actual(state.get(f.test_relpath).state) == segment::State(SEGMENT_DIRTY | SEGMENT_DELETE_AGE));
            wassert(actual(state.count(SEGMENT_OK)) == 2u);
            wassert(actual(state.size()) == 3u);
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.deleted.emplace_back("testds", f.test_relpath);
            wassert(actual(writer.get()).repack(e, true));
        }
        wassert(f.all_clean(2));
        wassert(f.query_results({0, 2}));
    });

    add_method("repack_archive", R"(
        - [archive age] [dirty] a segment that needs to be both repacked and
          archived, gets repacked before archiving
    )", [&](Fixture& f) {
        make_hole_middle();
        f.cfg.setValue("archive age", "1");
        f.test_reread_config();

        auto o = SessionTime::local_override(t20070707 + 2 * 86400);

        // State knows of a segment both to repack and to delete
        {
            auto state = f.scan_state();
            wassert(actual(state.get(f.test_relpath).state) == segment::State(SEGMENT_DIRTY | SEGMENT_ARCHIVE_AGE));
            wassert(actual(state.count(SEGMENT_OK)) == 2u);
            wassert(actual(state.size()) == 3u);
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto writer(f.makeSegmentedChecker());
            ReporterExpected e;
            e.repacked.emplace_back("testds", f.test_relpath);
            e.archived.emplace_back("testds", f.test_relpath);
            wassert(actual(writer.get()).repack(e, true));
        }
        wassert(f.all_clean(2));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    add_method("leaks_repack", [&](Fixture& f) {
        // Repack a segment and check that it doesn't cause a file to stay open
        // in the reader registry
        auto checker = f.makeSegmentedChecker();

        unsigned orig = arki::Reader::test_count_cached();
        checker->segment(f.test_relpath)->repack();
        wassert(actual(arki::Reader::test_count_cached()) == orig);
    });
}

}
}
}
