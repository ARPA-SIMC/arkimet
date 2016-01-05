#include "arki/dataset/tests.h"
#include "arki/exceptions.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segmented.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include <arki/wibble/grcal/grcal.h>
#include <cstdlib>
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
                auto writer(f.makeLocalChecker());
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
            wassert(actual(f.makeLocalChecker().get()).maintenance_clean(3));

            // Change timestamp and rescan the file
            {
                struct utimbuf oldts = { 199926000, 199926000 };
                ensure(utime("testds/2007/07-08.grib", &oldts) == 0);

                auto writer(f.makeLocalChecker());
                writer->rescanSegment("2007/07-08.grib");
            }

            // Ensure that the archive is still clean
            wassert(actual(f.makeLocalChecker().get()).maintenance_clean(3));

            // Repack the file
            {
                auto writer(f.makeLocalChecker());
                ensure_equals(writer->repackSegment("2007/07-08.grib"), 0u);
            }

            // Ensure that the archive is still clean
            wassert(actual(f.makeLocalChecker().get()).maintenance_clean(3));
        });
        add_method("repack_delete", [](Fixture& f) {
            // Test accuracy of maintenance scan, on a dataset with one file to both repack and delete
            f.cfg.setValue("step", "yearly");

            // Data are from 07, 08, 10 2007
            int treshold[6] = { 2008, 1, 1, 0, 0, 0 };
            int now[6];
            wibble::grcal::date::now(now);
            long long int duration = wibble::grcal::date::duration(treshold, now);
            f.cfg.setValue("delete age", duration/(3600*24));

            testdata::GRIBData data;
            f.import_all(data);

            {
                auto writer(f.makeLocalChecker());
                arki::tests::MaintenanceResults expected(false, 1);
                // A repack is still needed because the data is not sorted by reftime
                expected.by_type[DatasetTest::COUNTED_DIRTY] = 1;
                // And the same file is also old enough to be deleted
                expected.by_type[DatasetTest::COUNTED_DELETE_AGE] = 1;
                wassert(actual(writer.get()).maintenance(expected));
            }

            // Perform packing and check that things are still ok afterwards
            {
                auto writer(f.makeLocalChecker());
                ReporterExpected e;
                e.deleted.emplace_back("testds", "20/2007.grib");
                wassert(actual(writer.get()).repack(e, true));
            }
            wassert(actual(f.makeLocalChecker().get()).maintenance_clean(0));

            // Perform full maintenance and check that things are still ok afterwards
            {
                auto checker(f.makeLocalChecker());
                wassert(actual(checker.get()).check_clean(true, true));
            }
        });
        add_method("scan_repack_archive", [](Fixture& f) {
            // Test accuracy of maintenance scan, on a dataset with one file to both repack and archive
            f.cfg.setValue("step", "yearly");

            // Data are from 07, 08, 10 2007
            int treshold[6] = { 2008, 1, 1, 0, 0, 0 };
            int now[6];
            wibble::grcal::date::now(now);
            long long int duration = wibble::grcal::date::duration(treshold, now);
            f.cfg.setValue("archive age", duration/(3600*24));

            testdata::GRIBData data;
            f.import_all(data);

            {
                auto writer(f.makeLocalChecker());
                MaintenanceResults expected(false, 1);
                // A repack is still needed because the data is not sorted by reftime
                expected.by_type[DatasetTest::COUNTED_DIRTY] = 1;
                // And the same file is also old enough to be deleted
                expected.by_type[DatasetTest::COUNTED_ARCHIVE_AGE] = 1;
                wassert(actual(writer.get()).maintenance(expected));
            }

            // Perform packing and check that things are still ok afterwards
            {
                auto writer(f.makeLocalChecker());

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

}
