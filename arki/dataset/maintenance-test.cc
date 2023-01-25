#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"
#include "arki/dataset/query.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segmented.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/time.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/segment/seqfile.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/exceptions.h"
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

Fixture::Fixture(const std::string& format, const std::string& cfg_instance, DatasetTest::TestVariant variant)
    : DatasetTest(cfg_instance, variant), format(format)
{
    if (format == "grib")
    {
        skip_unless_grib();
        import_files = { "inbound/mainttest.grib" };
        test_relpath = "2007/07-07.grib";
        test_relpath_wrongstep = "2007/07.grib";
        test_datum_size = 34960;
    }
    else if (format == "bufr")
    {
        skip_unless_bufr();
        import_files = { "inbound/mainttest.bufr" };
        test_relpath = "2007/07-07.bufr";
        test_relpath_wrongstep = "2007/07.bufr";
        test_datum_size = 172;
    }
    else if (format == "vm2")
    {
        skip_unless_vm2();
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
    wassert(actual(state.get("testds:" + test_relpath).state) == test_relpath_state);
}

void Fixture::accurate_state_is(unsigned segment_count, unsigned test_relpath_state)
{
    auto state = scan_state(false);
    wassert(actual(state.size()) == segment_count);
    wassert(actual(state.get("testds:" + test_relpath).state) == test_relpath_state);
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

void Fixture::make_hole_start()
{
    makeSegmentedChecker()->test_make_hole(test_relpath, 10, 0);
}

void Fixture::make_hole_middle()
{
    makeSegmentedChecker()->test_make_hole(test_relpath, 10, 1);
}

void Fixture::make_hole_end()
{
    makeSegmentedChecker()->test_make_hole(test_relpath, 10, 2);
}

void Fixture::corrupt_first()
{
    makeSegmentedChecker()->test_corrupt_data(test_relpath, 0);
}

void Fixture::truncate_segment()
{
    makeSegmentedChecker()->test_truncate_data(test_relpath, 1);
}

void Fixture::swap_data()
{
    makeSegmentedChecker()->test_swap_data(test_relpath, 0, 1);
}

void Fixture::make_hugefile()
{
    skip_unless_filesystem_has_holes(".");

    // Pretend that the test segment is 6G already
    {
        utils::files::PreserveFileTimes pt("testds/" + test_relpath);
        sys::File fd("testds/" + test_relpath, O_RDWR);
        fd.ftruncate(6000000000LLU);
    }

    // Import a new datum, that will get appended to it
    {
        std::shared_ptr<Metadata> md(import_results[0]->clone());
        md->test_set("reftime", "2007-07-07 06:00:00");
        md->makeInline();

        auto writer(makeSegmentedWriter());
        wassert(actual(*writer).import(*md));

        wassert(actual(md->sourceBlob().offset) == 6000000000LLU);
    }
}

void Fixture::delete_one_in_segment()
{
    metadata::Collection mds(*config().create_reader(), Matcher());
    metadata::Collection to_delete;
    to_delete.push_back(mds.get(0));
    auto checker = config().create_checker();
    checker->remove(to_delete);
}

void Fixture::delete_all_in_segment()
{
    metadata::Collection mds(*config().create_reader(), Matcher());
    metadata::Collection to_delete;
    to_delete.push_back(mds.get(0));
    to_delete.push_back(mds.get(1));
    auto checker = config().create_checker();
    checker->remove(to_delete);
}

void Fixture::reset_seqfile()
{
    // FIXME: unlink instead?
    segment::SequenceFile sf("testds/" + test_relpath);
    sf.open();
    sf.write_sequence(0u);
}

void Fixture::remove_segment()
{
    std::string pathname = "testds/" + test_relpath;
    delete_if_exists(pathname);
}


void FixtureConcat::make_overlap()
{
    makeSegmentedChecker()->test_make_overlap(test_relpath, test_datum_size / 2, 1);
}

void FixtureDir::make_overlap()
{
    makeSegmentedChecker()->test_make_overlap(test_relpath, 1, 1);
}

void FixtureZip::test_setup()
{
    skip_unless_libzip();
    Fixture::test_setup();
    auto checker = makeSegmentedChecker();
    dataset::CheckerConfig opts;
    opts.readonly = false;
    opts.online = true;
    opts.offline = false;
    checker->zip(opts);
}

void FixtureZip::remove_segment()
{
    std::string pathname = "testds/" + test_relpath + ".zip";
    delete_if_exists(pathname);
}

void FixtureZip::make_overlap()
{
    throw TestSkipped("make_overlap not yet implemented in zip segments");
}


template<typename Fixture>
CheckTest<Fixture>::~CheckTest()
{
}

template<typename Fixture>
FixTest<Fixture>::~FixTest()
{
}

template<typename Fixture>
RepackTest<Fixture>::~RepackTest()
{
}


template<typename Fixture>
void CheckTest<Fixture>::register_tests_concat()
{
    // Check
    this->add_method("check_isfile", R"(
        - the segment must be a file
    )", [&](Fixture& f) {
        f.remove_segment();
        sys::makedirs("testds/" + f.test_relpath);

        wassert(f.state_is(3, segment::SEGMENT_MISSING));
        auto e = wassert_throws(std::runtime_error, f.query_results({1, 3, 0, 2}));
        wassert(actual(e.what()).contains("does not exist in directory segment"));
    });

    this->add_method("check_hugefile", [&](Fixture& f) {
        f.make_hugefile();
        wassert(f.state_is(3, segment::SEGMENT_DIRTY));
        wassert(f.query_results({1, -1, 3, 0, 2}));
    });
}

template<typename Fixture>
void CheckTest<Fixture>::register_tests_dir()
{
    this->add_method("check_isdir", R"(
        - the segment must be a directory [unaligned]
    )", [](Fixture& f) {
        wassert(actual_file("testds/" + f.test_relpath).exists());
        wassert_true(sys::isdir("testds/" + f.test_relpath));
    });

    this->add_method("check_datasize", R"(
        - the size of each data file must match the data size exactly [corrupted]
    )", [](Fixture& f) {
        {
            sys::File df("testds/" + f.test_relpath + "/000000." + f.format, O_RDWR);
            df.ftruncate(f.test_datum_size + 1);
        }

        wassert(f.state_is(3, segment::SEGMENT_CORRUPTED));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("check_ignore_dir_timestamp", R"(
       - the modification time of a directory segment can vary unpredictably,
         so it is ignored. The modification time of the sequence file is used
         instead.
    )", [&](Fixture& f) {
        sys::touch("testds/" + f.test_relpath, time(NULL) + 86400);
        wassert(f.all_clean(3));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("check_files_above_sequence", R"(
       - if arkimet is interrupted during rollback of an append operation on a
         dir dataset, there are files whose name has a higher sequence number
         than the sequence file. This will break further appends, and needs to
         be detected and fixed. [unaligned]
    )", [&](Fixture& f) {
        f.reset_seqfile();
        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
        wassert(f.query_results({1, 3, 0, 2}));
    });
}


template<typename Fixture>
void CheckTest<Fixture>::register_tests_zip()
{
    // Check
    this->add_method("check_iszip", R"(
        - the segment must be a zip file [unaligned]
    )", [](Fixture& f) {
        wassert(actual_file("testds/" + f.test_relpath + ".zip").exists());
        wassert(actual_file("testds/" + f.test_relpath).not_exists());
    });
}


template<typename TestFixture>
void CheckTest<TestFixture>::register_tests()
{
    switch (Fixture::segment_type())
    {
        case SEGMENT_CONCAT: register_tests_concat(); break;
        case SEGMENT_DIR:    register_tests_dir();    break;
        case SEGMENT_ZIP:    register_tests_zip();    break;
        default: throw std::runtime_error("unsupported segment type");
    }

    // TODO: this is not really a check test, and should be moved elsewhere
    this->add_method("clean", [](Fixture& f) {
        wassert(actual(f.makeSegmentedChecker().get()).check_clean());
        wassert(f.all_clean(3));
        wassert(actual(f.makeSegmentedChecker().get()).repack_clean());
        wassert(f.all_clean(3));

        // Check that maintenance does not accidentally create an archive
        wassert(actual_file("testds/.archive").not_exists());
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("check_exists", R"(
        - the segment must exist [missing]
    )", [&](Fixture& f) {
        f.remove_segment();

        wassert(f.state_is(3, segment::SEGMENT_MISSING));
        auto e = wassert_throws(std::runtime_error, f.query_results({1, 3, 0, 2}));
        wassert(actual(e.what()).contains("the segment has disappeared"));
    });

    this->add_method("check_unknown_empty", R"(
        - an empty segment not known by the index must be considered deleted [deleted]
    )", [&](Fixture& f) {
        {
            auto checker = f.makeSegmentedChecker();
            checker->test_truncate_data(f.test_relpath, 0);
            checker->test_delete_from_index(f.test_relpath);
        }
        wassert(f.state_is(3, segment::SEGMENT_DELETED));
        wassert(f.query_results({0, 2}));
    });

    this->add_method("empty_dir_segment", R"(
	- a directory segment without a .sequence file is not considered a
	  segment, only a spurious empty directory
    )", [&](Fixture& f) {
        // See #279
        sys::makedirs(f.test_relpath);

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "3 files ok");
            wassert(actual(checker.get()).repack(e, true));
        }
    });

    if (can_delete_data() && TestFixture::segment_can_delete_data())
    {
        this->add_method("check_new", R"(
            - data files not known by a valid index are considered data files whose
              entire content has been removed [deleted]
        )", [&](Fixture& f) {
            f.makeSegmentedChecker()->test_delete_from_index(f.test_relpath);
            wassert(f.state_is(3, segment::SEGMENT_DELETED));
        });

        this->add_method("check_one_removed", R"(
            - segments that contain some data that has been removed are
              identified as to be repacked [dirty]
        )", [&](Fixture& f) {
            f.delete_one_in_segment();
            wassert(f.state_is(3, segment::SEGMENT_DIRTY));
            wassert(f.query_results({3, 0, 2}));
        });

        this->add_method("check_all_removed", R"(
            - segments that only contain data that has been removed are
              identified as fully deleted [deleted]
        )", [&](Fixture& f) {
            f.delete_all_in_segment();
            wassert(f.state_is(3, segment::SEGMENT_DELETED));
            wassert(f.query_results({0, 2}));
        });
    }

    this->add_method("check_dataexists", R"(
        - all data known by the index for this segment must be present on disk [corrupted]
    )", [&](Fixture& f) {
        f.truncate_segment();

        wassert(f.state_is(3, segment::SEGMENT_CORRUPTED));
        wassert_throws(std::runtime_error, f.query_results({1, 3, 0, 2}));
    });

    if (can_detect_overlap())
        this->add_method("check_data_overlap", R"(
            - no pair of (offset, size) data spans from the index can overlap [corrupted]
        )", [&](Fixture& f) {
            f.make_overlap();
            wassert(f.state_is(3, segment::SEGMENT_CORRUPTED));
            wassert(f.query_results({1, 3, 0, 2}));
        });

    this->add_method("check_hole_start", R"(
        - data must start at the beginning of the segment [dirty]
    )", [&](Fixture& f) {
        f.make_hole_start();

        wassert(f.state_is(3, segment::SEGMENT_DIRTY));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("check_hole_middle", R"(
        - there must be no gaps between data in the segment [dirty]
    )", [&](Fixture& f) {
        f.make_hole_middle();

        wassert(f.state_is(3, segment::SEGMENT_DIRTY));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("check_hole_end", R"(
        - data must end at the end of the segment [dirty]
    )", [&](Fixture& f) {
        f.make_hole_end();

        wassert(f.state_is(3, segment::SEGMENT_DIRTY));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("check_archive_age", R"(
        - find segments that can only contain data older than `archive age` days [archive_age]
    )", [&](Fixture& f) {
        f.cfg->set("archive age", "1");
        f.test_reread_config();

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400 - 1);
            wassert(f.state_is(3, segment::SEGMENT_OK));
            wassert(f.query_results({1, 3, 0, 2}));
        }

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);
            wassert(f.state_is(3, segment::SEGMENT_ARCHIVE_AGE));
            wassert(f.query_results({1, 3, 0, 2}));
        }
    });

    this->add_method("check_delete_age", R"(
        - find segments that can only contain data older than `delete age` days [delete_age]
    )", [&](Fixture& f) {
        f.cfg->set("delete age", "1");
        f.test_reread_config();

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400 - 1);
            wassert(f.state_is(3, segment::SEGMENT_OK));
            wassert(f.query_results({1, 3, 0, 2}));
        }

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);
            wassert(f.state_is(3, segment::SEGMENT_DELETE_AGE));
            wassert(f.query_results({1, 3, 0, 2}));
        }
    });

    this->add_method("check_metadata_reftimes_must_fit_segment", R"(
    - the span of reference times in each segment must fit inside the interval
      implied by the segment file name (FIXME: should this be disabled for
      archives, to deal with datasets that had a change of step in their lifetime?) [corrupted]
    )", [&](Fixture& f) {
        std::shared_ptr<Metadata> md(f.import_results[1]->clone());
        md->test_set("reftime", "2007-07-06 00:00:00");
        md = f.makeSegmentedChecker()->test_change_metadata(f.test_relpath, md, 0);

        wassert(f.state_is(3, segment::SEGMENT_CORRUPTED));
        wassert(f.query_results({-1, 3, 0, 2}));
    });

    if (can_detect_segments_out_of_step())
        this->add_method("check_segment_name_must_fit_step", R"(
        - the segment name must represent an interval matching the dataset step
          (FIXME: should this be disabled for archives, to deal with datasets that had
          a change of step in their lifetime?) [corrupted]
        )", [&](Fixture& f) {
            f.makeSegmentedChecker()->test_rename(f.test_relpath, f.test_relpath_wrongstep);

            auto state = f.scan_state();
            wassert(actual(state.size()) == 3u);
            wassert(actual(state.get("testds:" + f.test_relpath_wrongstep).state) == segment::SEGMENT_CORRUPTED);

            // We are breaking the invariant that segments sorted by file name
            // are in the same sequence of segments sorted by time of file
            // contents, but the invariant that data queried is sorted by
            // reftime must still stand
            wassert(f.query_results({1, 3, 0, 2}));
        });

    this->add_method("check_isordered", R"(
        - data on disk must match the order of data used by queries [dirty]
    )", [&](Fixture& f) {
        f.swap_data();

        wassert(f.state_is(3, segment::SEGMENT_DIRTY));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("check_unaligned", R"(
        - segments not known by the index, but when the index is either
          missing, older than the file, or marked as needing checking, are marked
          for reindexing instead of deletion [unaligned]
    )", [&](Fixture& f) {
        f.makeSegmentedChecker()->test_invalidate_in_index(f.test_relpath);

        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));
    });

    // Optional thorough check
    this->add_method("check_corrupted_data", R"(
        - format-specific consistency checks on the content of each file must pass [corrupted]
    )", [&](Fixture& f) {
        f.corrupt_first();

        wassert(f.accurate_state_is(3, segment::SEGMENT_CORRUPTED));
        wassert(f.query_results({1, 3, 0, 2}));
    });
}

template<typename Fixture>
void FixTest<Fixture>::register_tests_concat()
{
    this->add_method("fix_hugefile", [&](Fixture& f) {
        f.make_hugefile();
        wassert(f.state_is(3, segment::SEGMENT_DIRTY));

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            wassert(actual(checker.get()).check(e, true));
        }

        wassert(f.state_is(3, segment::SEGMENT_DIRTY));
        wassert(f.query_results({1, -1, 3, 0, 2}));
    });
}

template<typename Fixture>
void FixTest<Fixture>::register_tests_dir()
{
    this->add_method("fix_files_above_sequence", R"(
       - [unaligned] fix low sequence file value by setting it to the highest
         sequence number found.
    )", [&](Fixture& f) {
        f.reset_seqfile();

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            e.rescanned.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).check(e, true));
        }

        wassert(f.ensure_localds_clean(3, 4));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("fix_truncated_file_above_sequence", R"(
       - [unaligned] fix low sequence file value by setting it to the highest
         sequence number found, with one file truncated / partly written.
    )", [&](Fixture& f) {
        f.makeSegmentedChecker()->test_invalidate_in_index(f.test_relpath);
        f.reset_seqfile();
        {
            sys::File df("testds/" + f.test_relpath + "/000000." + f.format, O_RDWR);
            df.ftruncate(f.test_datum_size / 2);
        }

        wassert(f.state_is(3, segment::SEGMENT_UNALIGNED));

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            e.rescanned.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).check(e, true));
        }

        wassert(f.state_is(3, segment::SEGMENT_DIRTY));

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.repacked.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).repack(e, true));
        }

        wassert(f.ensure_localds_clean(3, 3));
        wassert(f.query_results({3, 0, 2}));
    });
}

template<typename TestFixture>
void FixTest<TestFixture>::register_tests()
{
    switch (Fixture::segment_type())
    {
        case SEGMENT_CONCAT: register_tests_concat(); break;
        case SEGMENT_DIR:    register_tests_dir();    break;
        case SEGMENT_ZIP:    break;
        default: throw std::runtime_error("unsupported segment type");
    }

    if (can_delete_data() && TestFixture::segment_can_delete_data())
    {
        this->add_method("fix_one_removed", [&](Fixture& f) {
            f.delete_one_in_segment();

            {
                auto checker(f.makeSegmentedChecker());
                ReporterExpected e;
                wassert(actual(checker.get()).check(e, true));
            }

            wassert(f.state_is(3, segment::SEGMENT_DIRTY));
            wassert(f.query_results({3, 0, 2}));
        });

        this->add_method("fix_deleted", R"(
            - [deleted] segments are left untouched
        )", [&](Fixture& f) {
            f.delete_all_in_segment();

            {
                auto checker(f.makeSegmentedChecker());
                ReporterExpected e;
                e.report.emplace_back("testds", "check", "2 files ok");
                wassert(actual(checker.get()).check(e, true));
            }

            wassert(f.state_is(3, segment::SEGMENT_DELETED));
            wassert(f.query_results({0, 2}));
        });
    }

    this->add_method("fix_dirty", R"(
        - [dirty] segments are not touched
    )", [&](Fixture& f) {
        f.make_hole_end();

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            wassert(actual(checker.get()).check(e, true));
        }

        wassert(f.state_is(3, segment::SEGMENT_DIRTY));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("fix_unaligned", R"(
        - [unaligned] segments are imported in-place
    )", [&](Fixture& f) {
        f.makeSegmentedChecker()->test_invalidate_in_index(f.test_relpath);

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            e.rescanned.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).check(e, true));
        }

        wassert(f.ensure_localds_clean(3, 4));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("fix_missing", R"(
        - [missing] segments are removed from the index
    )", [&](Fixture& f) {
        f.remove_segment();

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            e.deindexed.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).check(e, true));
        }

        wassert(f.ensure_localds_clean(2, 2));
        wassert(f.query_results({0, 2}));
    });

    this->add_method("fix_corrupted", R"(
        - [corrupted] segments can only be fixed by manual intervention. They
          are reported and left untouched
    )", [&](Fixture& f) {
        std::shared_ptr<Metadata> md(f.import_results[1]->clone());
        md->test_set("reftime", "2007-07-06 00:00:00");
        md = f.makeSegmentedChecker()->test_change_metadata(f.test_relpath, md, 0);

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            e.segment_manual_intervention.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).check(e, true));
        }

        wassert(f.state_is(3, segment::SEGMENT_CORRUPTED));
        wassert(f.query_results({-1, 3, 0, 2}));
    });

    this->add_method("fix_archive_age", R"(
        - [archive age] segments are not touched
    )", [&](Fixture& f) {
        f.cfg->set("archive age", "1");
        f.test_reread_config();

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);

            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            wassert(actual(checker.get()).check(e, true));
        }
        wassert(f.state_is(3, segment::SEGMENT_ARCHIVE_AGE));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("fix_delete_age", R"(
        - [delete age] segments are not touched
    )", [&](Fixture& f) {
        f.cfg->set("delete age", "1");
        f.test_reread_config();

        {
            auto o = SessionTime::local_override(t20070707 + 2 * 86400);

            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "check", "2 files ok");
            wassert(actual(checker.get()).check(e, true));
        }
        wassert(f.state_is(3, segment::SEGMENT_DELETE_AGE));
        wassert(f.query_results({1, 3, 0, 2}));
    });
}


template<typename Fixture>
void RepackTest<Fixture>::register_tests_concat()
{
    this->add_method("repack_hugefile", [&](Fixture& f) {
        f.make_hugefile();
        wassert(f.state_is(3, segment::SEGMENT_DIRTY));

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

    this->add_method("repack_timestamps", [&](Fixture& f) {
        // Set the segment to a past timestamp and rescan it, making it as if
        // it were imported a long time ago
        {
            sys::touch("testds/" + f.test_relpath, 199926000);

            NullReporter reporter;
            f.makeSegmentedChecker()->segment(f.test_relpath)->rescan(reporter);
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

template<typename Fixture>
void RepackTest<Fixture>::register_tests_dir()
{
}

template<typename TestFixture>
void RepackTest<TestFixture>::register_tests()
{
    switch (Fixture::segment_type())
    {
        case SEGMENT_CONCAT: register_tests_concat(); break;
        case SEGMENT_DIR:    register_tests_dir();    break;
        case SEGMENT_ZIP:    break;
        default: throw std::runtime_error("unsupported segment type");
    }

    if (can_delete_data() && TestFixture::segment_can_delete_data())
    {
        this->add_method("repack_deleted", R"(
            - [deleted] segments are removed from disk
        )", [&](Fixture& f) {
            f.delete_all_in_segment();

            {
                auto checker(f.makeSegmentedChecker());
                ReporterExpected e;
                e.report.emplace_back("testds", "repack", "2 files ok");
                e.deleted.emplace_back("testds", f.test_relpath);
                wassert(actual(checker.get()).repack(e, true));
            }

            wassert(f.ensure_localds_clean(2, 2));
            wassert(f.query_results({0, 2}));
        });

        this->add_method("repack_one_removed", [&](Fixture& f) {
            f.delete_one_in_segment();

            {
                auto checker(f.makeSegmentedChecker());
                ReporterExpected e;
                e.report.emplace_back("testds", "repack", "2 files ok");
                e.repacked.emplace_back("testds", f.test_relpath);
                wassert(actual(checker.get()).repack(e, true));
            }

            wassert(f.ensure_localds_clean(3, 3));
            wassert(f.query_results({3, 0, 2}));
        });
    }

    this->add_method("repack_dirty", R"(
        - [dirty] segments are rewritten to be without holes and have data in the right order.
          In concat segments, this is done to guarantee linear disk access when
          data are queried in the default sorting order. In dir segments, this
          is done to avoid sequence numbers growing indefinitely for datasets
          with frequent appends and removes.
    )", [&](Fixture& f) {
        f.make_hole_middle();
        // swap_data(); // FIXME: swap_data currently isn't detected as dirty on simple datasets

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.repacked.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).repack(e, true));
        }

        wassert(f.ensure_localds_clean(3, 4));
        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("repack_missing", R"(
        - [missing] segments are removed from the index
    )", [&](Fixture& f) {
        f.remove_segment();

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.deindexed.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).repack(e, true));
        }

        wassert(f.ensure_localds_clean(2, 2));
        wassert(f.query_results({0, 2}));
    });

    this->add_method("repack_corrupted", R"(
        - [corrupted] segments are not touched
    )", [&](Fixture& f) {
        std::shared_ptr<Metadata> md(f.import_results[1]->clone());
        md->test_set("reftime", "2007-07-06 00:00:00");
        md = f.makeSegmentedChecker()->test_change_metadata(f.test_relpath, md, 0);

        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            wassert(actual(checker.get()).repack(e, true));
        }

        wassert(f.state_is(3, segment::SEGMENT_CORRUPTED));
        wassert(f.query_results({-1, 3, 0, 2}));
    });


    this->add_method("repack_archive_age", R"(
        - [archive age] segments are repacked if needed, then moved to .archive/last
    )", [&](Fixture& f) {
        f.make_hole_middle();
        f.cfg->set("archive age", "1");
        f.test_reread_config();

        auto o = SessionTime::local_override(t20070707 + 2 * 86400);
        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.repacked.emplace_back("testds", f.test_relpath);
            e.archived.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).repack(e, true));
        }
        wassert(f.ensure_localds_clean(3, 4));

        // Check that the files have been moved to the archive
        wassert(actual_file("testds/" + f.test_relpath).not_exists());
        wassert(actual_file("testds/.archive/last/" + f.test_relpath_ondisk()).exists());
        wassert(actual_file("testds/.archive/last/" + f.test_relpath + ".metadata").exists());
        wassert(actual_file("testds/.archive/last/" + f.test_relpath + ".summary").exists());

        wassert(f.query_results({1, 3, 0, 2}));
    });

    this->add_method("repack_delete_age", R"(
        - [delete age] segments are deleted
    )", [&](Fixture& f) {
        f.make_hole_middle();
        f.cfg->set("delete age", "1");
        f.test_reread_config();

        auto o = SessionTime::local_override(t20070707 + 2 * 86400);
        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.deleted.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).repack(e, true));
        }
        wassert(f.ensure_localds_clean(2, 2));

        wassert(f.query_results({0, 2}));
    });

    this->add_method("repack_delete", R"(
        - [delete age] [dirty] a segment that needs to be both repacked and
          deleted, gets deleted without repacking
    )", [&](Fixture& f) {
        f.make_hole_middle();
        f.cfg->set("delete age", "1");
        f.test_reread_config();

        auto o = SessionTime::local_override(t20070707 + 2 * 86400);

        // State knows of a segment both to repack and to delete
        {
            auto state = f.scan_state();
            wassert(actual(state.get("testds:" + f.test_relpath).state) == segment::State(segment::SEGMENT_DIRTY | segment::SEGMENT_DELETE_AGE));
            wassert(actual(state.count(segment::SEGMENT_OK)) == 2u);
            wassert(actual(state.size()) == 3u);
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.report.emplace_back("testds", "repack", "2 files ok");
            e.deleted.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).repack(e, true));
        }
        wassert(f.all_clean(2));
        wassert(f.query_results({0, 2}));
    });

    this->add_method("repack_archive", R"(
        - [archive age] [dirty] a segment that needs to be both repacked and
          archived, gets repacked before archiving
    )", [&](Fixture& f) {
        f.make_hole_middle();
        f.cfg->set("archive age", "1");
        f.test_reread_config();

        auto o = SessionTime::local_override(t20070707 + 2 * 86400);

        // State knows of a segment both to repack and to delete
        {
            auto state = f.scan_state();
            wassert(actual(state.get("testds:" + f.test_relpath).state) == segment::State(segment::SEGMENT_DIRTY | segment::SEGMENT_ARCHIVE_AGE));
            wassert(actual(state.count(segment::SEGMENT_OK)) == 2u);
            wassert(actual(state.size()) == 3u);
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto checker(f.makeSegmentedChecker());
            ReporterExpected e;
            e.repacked.emplace_back("testds", f.test_relpath);
            e.archived.emplace_back("testds", f.test_relpath);
            wassert(actual(checker.get()).repack(e, true));
        }
        wassert(f.all_clean(3));
        wassert(f.query_results({1, 3, 0, 2}));
    });
}

template class CheckTest<FixtureConcat>;
template class CheckTest<FixtureDir>;
template class CheckTest<FixtureZip>;
template class FixTest<FixtureConcat>;
template class FixTest<FixtureDir>;
template class FixTest<FixtureZip>;
template class RepackTest<FixtureConcat>;
template class RepackTest<FixtureDir>;
template class RepackTest<FixtureZip>;

}
}
}
