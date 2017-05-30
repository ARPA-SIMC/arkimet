#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance-test.h"
#include "arki/exceptions.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segmented.h"
#include "arki/dataset/reporter.h"
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

namespace arki {
namespace dataset {
namespace maintenance_test {

SegmentTests::~SegmentTests() {}

void SegmentTests::register_tests(MaintenanceTest& tc)
{
    tc.add_method("check_exists", R"(
        - the segment must exist
    )", [&](Fixture& f) {
        tc.rm_r("testds/2007/07-07.grib");

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_DELETED));
    });

    tc.add_method("check_dataexists", R"(
        - all data known by the index for this segment must be present on disk
    )", [&](Fixture& f) {
        truncate_segment();

        NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });


    // Optional thorough check
    // run format-specific consistency checks on the content of each file

    // During fix
    // data files present on disk but not in the index are rescanned and added to the index
    // data files present in the index but not on disk are removed from the index

    // During repack
    // data files present on disk but not in the index are deleted from the disk
    // data files present in the index but not on disk are removed from the index
    // files older than delete age are removed
    // files older than archive age are moved to last/
}

namespace {

struct SegmentConcatTests : public SegmentTests
{
    void truncate_segment() override
    {
        sys::File f("testds/2007/07-07.grib", O_RDWR);
        f.ftruncate(7218);
    }
    void register_tests(MaintenanceTest& tc) override;
};

void SegmentConcatTests::register_tests(MaintenanceTest& tc)
{
    SegmentTests::register_tests(tc);

    tc.add_method("check_isfile", R"(
        - the segment must be a file
    )", [](Fixture& f) {
        sys::unlink("testds/2007/07-07.grib");
        sys::makedirs("testds/2007/07-07.grib");

        arki::dataset::NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_DELETED));
    });

    // no pair of (offset, size) data spans from the index can overlap
    // file size must be exactly the same as offset+size for the data in the index with the highest offset for this segment
    // all the data in the index must cover the whole file, without holes

    // During repack
    // if the order in the data file does not match the order required from the index, data files are rewritten rearranging the data, and the offsets in the index are updated accordingly. This is done to guarantee linear disk access when data are queried in the default sorting order.
}


struct SegmentDirTests : public SegmentTests
{
    void truncate_segment() override
    {
        sys::unlink("testds/2007/07-07.grib/000001.grib");
    }
    void register_tests(MaintenanceTest& tc) override;
};

void SegmentDirTests::register_tests(MaintenanceTest& tc)
{
    SegmentTests::register_tests(tc);

    tc.add_method("check_isdir", R"(
        - the segment must be a directory
    )", [](Fixture& f) {
        sys::rmtree("testds/2007/07-07.grib");
        sys::write_file("testds/2007/07-07.grib", "");

        arki::dataset::NullReporter nr;
        auto state = f.makeSegmentedChecker()->scan(nr);
        wassert(actual(state.size()) == 3u);
        wassert(actual(state.get("2007/07-07.grib").state) == segment::State(SEGMENT_UNALIGNED));
    });

    // During check
    // the file sequence numbers must start from 0 and there must be no gaps
    // sequence files on disk must match the order of data in the index

    // Optional thorough check
    // ensure each file contains one and only one datum, in case dir segments are used to store data that can be concatenated

    // During repack
    // data files are renumbered starting from 0, sequentially without gaps, in the order given by the index, or by reference time if there is no index. This is done to avoid sequence numbers growing indefinitely for datasets with frequent appends and removes.
}

}

MaintenanceTest::~MaintenanceTest()
{
    delete segment_tests;
}

void MaintenanceTest::init_segment_tests()
{
    switch (segment_type)
    {
        case SEGMENT_CONCAT: segment_tests = new SegmentConcatTests; break;
        case SEGMENT_DIR:    segment_tests = new SegmentDirTests;  break;
    }
}

void MaintenanceTest::rename(const std::string& old_pathname, const std::string& new_pathname)
{
    if (::rename(old_pathname.c_str(), new_pathname.c_str()) != 0)
        throw_system_error("cannot rename " + old_pathname + " to " + new_pathname);
}

void MaintenanceTest::rm_r(const std::string& pathname)
{
    if (sys::isdir(pathname))
        sys::rmtree("testds/2007/07-07.grib");
    else
        sys::unlink("testds/2007/07-07.grib");
}

void MaintenanceTest::touch(const std::string& pathname, time_t ts)
{
    struct utimbuf t = { ts, ts };
    if (::utime(pathname.c_str(), &t) != 0)
        throw_system_error("cannot set mtime/atime of " + pathname);
}

void MaintenanceTest::register_tests()
{
    segment_tests->register_tests(*this);

    add_method("clean", [](Fixture& f) {
        wassert(actual(f.makeSegmentedChecker().get()).check_clean());
        wassert(actual(f.makeSegmentedChecker().get()).maintenance_clean(3));
        wassert(actual(f.makeSegmentedChecker().get()).repack_clean());
        wassert(actual(f.makeSegmentedChecker().get()).maintenance_clean(3));
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
