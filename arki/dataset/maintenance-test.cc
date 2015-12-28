#include "arki/dataset/tests.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segmented.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include <arki/wibble/grcal/grcal.h>
#include "arki/wibble/exception.h"
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
    string extracfg;

    Fixture(const std::string& extracfg)
        : extracfg(extracfg)
    {
    }

    void test_setup()
    {
        std::stringstream in(extracfg);
        cfg.clear();
        cfg.parse(in, "(memory)");
        cfg.setValue("path", sys::abspath("testds"));
        cfg.setValue("name", "testds");
        cfg.setValue("unique", "reftime, origin, product, level, timerange, area");
        cfg.setValue("step", "daily");
    }

    void test_teardown()
    {
        delete segment_manager;
        segment_manager = nullptr;
    }

    const source::Blob& find_imported_second_in_file()
    {
        // Find the imported_result element whose offset is > 0
        for (int i = 0; i < 3; ++i)
        {
            const source::Blob& second_in_segment = import_results[i].sourceBlob();
            if (second_in_segment.offset > 0)
                return second_in_segment;
        }
        throw std::runtime_error("second in file not found");
    }

    void test_preconditions(const testdata::Fixture& fixture)
    {
        wassert(actual(fixture.fnames_before_cutoff.size()) > 0u);
        wassert(actual(fixture.fnames_after_cutoff.size()) > 0u);
        wassert(actual(fixture.fnames_before_cutoff.size() + fixture.fnames_after_cutoff.size()) == fixture.count_dataset_files());
    }

    void test_maintenance_on_clean(const testdata::Fixture& fixture)
    {
        unsigned file_count = fixture.count_dataset_files();

        wruntest(import_all, fixture);

        // Ensure everything appears clean
        {
            auto writer(makeLocalChecker());
            wassert(actual(writer.get()).maintenance_clean(file_count));

            // Check that maintenance does not accidentally create an archive
            wassert(actual_file("testds/.archive").not_exists());
        }

        // Ensure packing has nothing to report
        {
            auto writer(makeLocalChecker());
            wassert(actual(writer.get()).repack_clean(false));
            wassert(actual(writer.get()).maintenance_clean(file_count));
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto writer(makeLocalChecker());
            wassert(actual(writer.get()).repack_clean(true));
            wassert(actual(writer.get()).maintenance_clean(file_count));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto writer(makeLocalChecker());
            wassert(actual(writer.get()).check_clean(true, true));
            wassert(actual(writer.get()).maintenance_clean(file_count));
        }
    }

    void test_move_to_archive(const testdata::Fixture& fixture)
    {
        cfg.setValue("archive age", fixture.selective_days_since());
        wruntest(import_all, fixture);

        // Check if files to archive are detected
        {
            auto writer(makeLocalChecker());
            arki::tests::MaintenanceResults expected(false, fixture.count_dataset_files());
            expected.by_type[COUNTED_OK] = fixture.fnames_after_cutoff.size();
            expected.by_type[COUNTED_TO_ARCHIVE] = fixture.fnames_before_cutoff.size();
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto writer(makeLocalChecker());
            ReporterExpected e;
            for (set<string>::const_iterator i = fixture.fnames_before_cutoff.begin();
                    i != fixture.fnames_before_cutoff.end(); ++i)
                e.archived.emplace_back("testds", *i);
            wassert(actual(writer.get()).repack(e, true));
        }

        // Check that the files have been moved to the archive
        for (set<string>::const_iterator i = fixture.fnames_before_cutoff.begin();
                i != fixture.fnames_before_cutoff.end(); ++i)
        {
            wassert(actual_file("testds/.archive/last/" + *i).exists());
            wassert(actual_file("testds/.archive/last/" + *i + ".metadata").exists());
            wassert(actual_file("testds/.archive/last/" + *i + ".summary").exists());
            wassert(actual_file("testds/" + *i).not_exists());
        }

        // Maintenance should now show a normal situation
        {
            auto writer(makeLocalChecker());
            arki::tests::MaintenanceResults expected(true, fixture.count_dataset_files());
            expected.by_type[COUNTED_OK] = fixture.fnames_after_cutoff.size();
            expected.by_type[COUNTED_ARC_OK] = fixture.fnames_before_cutoff.size();
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto writer(makeLocalChecker());
            wassert(actual(writer.get()).check_clean(true, true));

            arki::tests::MaintenanceResults expected(true, fixture.count_dataset_files());
            expected.by_type[COUNTED_OK] = fixture.fnames_after_cutoff.size();
            expected.by_type[COUNTED_ARC_OK] = fixture.fnames_before_cutoff.size();
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Test that querying returns all items
        {
            std::unique_ptr<Reader> reader(makeReader(&cfg));

            unsigned count = 0;
            reader->query_data(Matcher(), [&](unique_ptr<Metadata>) { ++count; return true; });
            wassert(actual(count) == 3u);
        }
    }

    void test_delete_age(const testdata::Fixture& fixture)
    {
        cfg.setValue("delete age", fixture.selective_days_since());
        wruntest(import_all, fixture);

        // Check if files to delete are detected
        {
            auto writer(makeLocalChecker());
            arki::tests::MaintenanceResults expected(false, fixture.count_dataset_files());
            expected.by_type[COUNTED_OK] = fixture.fnames_after_cutoff.size();
            expected.by_type[COUNTED_TO_DELETE] = fixture.fnames_before_cutoff.size();
            wassert(actual(writer.get()).maintenance(expected));
        }
        // Perform packing and check that things are still ok afterwards
        {
            auto writer(makeLocalChecker());
            ReporterExpected e;
            for (set<string>::const_iterator i = fixture.fnames_before_cutoff.begin();
                    i != fixture.fnames_before_cutoff.end(); ++i)
                e.deleted.emplace_back("testds", *i);
            wassert(actual(writer.get()).repack(e, true));
        }

        auto writer(makeLocalChecker());
        wassert(actual(writer.get()).maintenance_clean(fixture.fnames_after_cutoff.size()));
    }

    void test_truncated_datafile_at_data_boundary(const testdata::Fixture& fixture)
    {
        cfg.setValue("step", fixture.max_selective_aggregation);
        wruntest(import_all, fixture);

        // Find the imported_result element whose offset is > 0
        const source::Blob& second_in_segment = find_imported_second_in_file();
        wassert(actual(second_in_segment.offset) > 0u);

        // Truncate at the position in second_in_segment
        string truncated_fname = second_in_segment.filename;
        segments().truncate(truncated_fname, second_in_segment.offset);

        // See that the truncated file is detected
        {
            auto writer(makeLocalChecker());
            arki::tests::MaintenanceResults expected(false, 2);
            expected.by_type[COUNTED_OK] = 1;
            expected.by_type[COUNTED_TO_RESCAN] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform packing and check that nothing has happened
        {
            auto writer(makeLocalChecker());
            wassert(actual(writer.get()).repack_clean(true));

            arki::tests::MaintenanceResults expected(false, 2);
            expected.by_type[COUNTED_OK] = 1;
            expected.by_type[COUNTED_TO_RESCAN] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto writer(makeLocalChecker());
            ReporterExpected e;
            e.rescanned.emplace_back("testds", truncated_fname);
            wassert(actual(writer.get()).check(e, true, true));

            wassert(actual(writer.get()).maintenance_clean(2));
        }

        // Try querying and make sure we get the two files
        {
            std::unique_ptr<Reader> reader(makeReader());

            unsigned count = 0;
            reader->query_data(Matcher(), [&](unique_ptr<Metadata>) { ++count; return true; });
            wassert(actual(count) == 2u);
        }
    }

    void test_corrupted_datafile(const testdata::Fixture& fixture)
    {
        cfg.setValue("step", fixture.max_selective_aggregation);
        wruntest(import_all_packed, fixture);

        // Find the imported_result element whose offset is > 0
        const source::Blob& second_in_segment = find_imported_second_in_file();
        wassert(actual(second_in_segment.offset) > 0u);

        // Corrupt the first datum in the file
        corrupt_datafile(second_in_segment.absolutePathname());

        // A quick check has nothing to complain
        {
            auto writer(makeLocalChecker());
            wassert(actual(writer.get()).maintenance_clean(2));
        }

        // A thorough check should find the corruption
        {
            //nag::TestCollect tc;
            auto writer(makeLocalChecker());
            arki::tests::MaintenanceResults expected(false, 2);
            expected.by_type[COUNTED_OK] = 1;
            expected.by_type[COUNTED_TO_RESCAN] = 1;
            wassert(actual(writer.get()).maintenance(expected, false));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto writer(makeLocalChecker(&cfg));
            ReporterExpected e;
            e.rescanned.emplace_back("testds", second_in_segment.filename);
            wassert(actual(writer.get()).check(e, true, false));

            // The corrupted file has been deindexed, now there is a gap in the data file
            arki::tests::MaintenanceResults expected(false, 2);
            expected.by_type[COUNTED_OK] = 1;
            expected.by_type[COUNTED_TO_PACK] = 1;
            wassert(actual(writer.get()).maintenance(expected, false));
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto writer(makeLocalChecker(&cfg));
            ReporterExpected e;
            e.repacked.emplace_back("testds", second_in_segment.filename);
            wassert(actual(writer.get()).repack(e, true));
        }

        // Maintenance and pack are ok now
        {
            auto writer(makeLocalChecker());
            wassert(actual(writer.get()).maintenance_clean(2));
            wassert(actual(writer.get()).maintenance_clean(2, false));
        }
    }

    void test_deleted_datafile_repack(const testdata::Fixture& fixture)
    {
        wruntest(import_all_packed, fixture);
        string deleted_fname = import_results[0].sourceBlob().filename;
        unsigned file_count = fixture.count_dataset_files();
        segments().remove(deleted_fname);

        // Initial check finds the deleted file
        {
            auto writer(makeLocalChecker());
            arki::tests::MaintenanceResults expected(false, file_count);
            expected.by_type[COUNTED_OK] = file_count - 1;
            expected.by_type[COUNTED_TO_DEINDEX] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Packing should notice the problem and do nothing
        {
            auto writer(makeLocalChecker(&cfg));
            ReporterExpected e;
            e.deindexed.emplace_back("testds", deleted_fname);
            wassert(actual(writer.get()).repack(e, false));

            arki::tests::MaintenanceResults expected(false, file_count);
            expected.by_type[COUNTED_OK] = file_count - 1;
            expected.by_type[COUNTED_TO_DEINDEX] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto writer(makeLocalChecker(&cfg));
            ReporterExpected e;
            e.deindexed.emplace_back("testds", deleted_fname);
            wassert(actual(writer.get()).repack(e, true));

            wassert(actual(writer.get()).maintenance_clean(file_count - 1));
        }
    }

    void test_deleted_datafile_check(const testdata::Fixture& fixture)
    {
        wruntest(import_all_packed, fixture);
        string deleted_fname = import_results[0].sourceBlob().filename;
        unsigned file_count = fixture.count_dataset_files();
        segments().remove(deleted_fname);

        // Initial check finds the deleted file
        {
            auto writer(makeLocalChecker());
            arki::tests::MaintenanceResults expected(false, file_count);
            expected.by_type[COUNTED_OK] = file_count - 1;
            expected.by_type[COUNTED_TO_DEINDEX] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto writer(makeLocalChecker());
            ReporterExpected e;
            e.deindexed.emplace_back("testds", deleted_fname);
            wassert(actual(writer.get()).check(e, true, true));

            wassert(actual(writer.get()).maintenance_clean(file_count - 1));
        }
    }

    void test_deleted_index_check(const testdata::Fixture& fixture)
    {
        unsigned file_count = fixture.count_dataset_files();
        wruntest(import_all_packed, fixture);
        sys::unlink_ifexists("testds/index.sqlite");
        sys::unlink_ifexists("testds/MANIFEST");
        sys::makedirs("testds/2014/");
        system("echo 'GRIB garbage 7777' > testds/2014/01.grib1.tmp");

        // See if the files to index are detected in the correct number
        {
            auto writer(makeLocalChecker());
            arki::tests::MaintenanceResults expected(false, file_count);
            expected.by_type[COUNTED_TO_INDEX] = file_count;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto writer(makeLocalChecker());
            ReporterExpected e;
            for (set<string>::const_iterator i = fixture.fnames_before_cutoff.begin();
                    i != fixture.fnames_before_cutoff.end(); ++i)
                e.rescanned.emplace_back("testds", *i);
            for (set<string>::const_iterator i = fixture.fnames_after_cutoff.begin();
                    i != fixture.fnames_after_cutoff.end(); ++i)
                e.rescanned.emplace_back("testds", *i);
            wassert(actual(writer.get()).check(e, true, true));

            wassert(actual(writer.get()).maintenance_clean(file_count));
            wassert(actual(writer.get()).repack_clean(true));
        }

        // The spurious file should not have been touched
        wassert(actual_file("testds/2014/01.grib1.tmp").exists());
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override
    {
        add_method("scan_clean", [](Fixture& f) {
            // Test accuracy of maintenance scan, on perfect dataset
            wruntest(f.test_preconditions, testdata::GRIBData());
            wruntest(f.test_preconditions, testdata::BUFRData());
            wruntest(f.test_preconditions, testdata::VM2Data());
            wruntest(f.test_preconditions, testdata::ODIMData());

            wruntest(f.test_maintenance_on_clean, testdata::GRIBData());
            wruntest(f.test_maintenance_on_clean, testdata::BUFRData());
            wruntest(f.test_maintenance_on_clean, testdata::VM2Data());
            wruntest(f.test_maintenance_on_clean, testdata::ODIMData());
        });
        add_method("scan_archive", [](Fixture& f) {
            // Test accuracy of maintenance scan, on perfect dataset, with data to archive
            nag::TestCollect tc;
            wruntest(f.test_move_to_archive, testdata::GRIBData());
            wruntest(f.test_move_to_archive, testdata::BUFRData());
            wruntest(f.test_move_to_archive, testdata::VM2Data());
            wruntest(f.test_move_to_archive, testdata::ODIMData());
            tc.clear();
        });
        add_method("scan_todelete", [](Fixture& f) {
            // Test accuracy of maintenance scan, on perfect dataset, with data to delete
            wruntest(f.test_delete_age, testdata::GRIBData());
            wruntest(f.test_delete_age, testdata::BUFRData());
            wruntest(f.test_delete_age, testdata::VM2Data());
            wruntest(f.test_delete_age, testdata::ODIMData());
        });
        add_method("scan_truncated", [](Fixture& f) {
            // Test accuracy of maintenance scan, on perfect dataset, with a truncated data file
            wruntest(f.test_truncated_datafile_at_data_boundary, testdata::GRIBData());
            wruntest(f.test_truncated_datafile_at_data_boundary, testdata::BUFRData());
            wruntest(f.test_truncated_datafile_at_data_boundary, testdata::VM2Data());
            wruntest(f.test_truncated_datafile_at_data_boundary, testdata::ODIMData());
        });
        add_method("scan_corrupted", [](Fixture& f) {
            // Test accuracy of maintenance scan, on a dataset with a corrupted data file
            /**
             * Here we have inconsistent behaviou across segment types and data types,
             * because:
             *  - some formats detect corruption, some formats skip garbage
             *  - concatenated data in files may skip corrupted data as garbage, but
             *  directory segments are already split in separate data units and will
             *  load, instead of skipping, corrupted data
             */
            wruntest(f.test_corrupted_datafile, testdata::GRIBData());
            wruntest(f.test_corrupted_datafile, testdata::BUFRData());
            // TODO: VM2 scanning does not yet deal gracefully with corruption
            wruntest(f.test_corrupted_datafile, testdata::VM2Data());
            // TODO: ODIM scanning does not yet deal gracefully with corruption
            wruntest(f.test_corrupted_datafile, testdata::ODIMData());
        });
        add_method("scan_hugefile", [](Fixture& f) {
            // Test accuracy of maintenance scan, on a dataset with a data file larger than 2**31
            if (f.cfg.value("type") == "simple") return; // TODO: we need to avoid the SLOOOOW rescan done by simple on the data file
            f.clean();

            // Simulate 2007/07-07.grib1 to be 6G already
            system("mkdir -p testds/2007");
            system("touch testds/2007/07-07.grib1");
            // Truncate the last grib out of a file
            if (truncate("testds/2007/07-07.grib1", 6000000000LLU) < 0)
                throw wibble::exception::System("truncating testds/2007/07-07.grib1");

            f.import();

            {
                auto writer(f.makeLocalChecker());
                MaintenanceResults expected(3, false);
                expected.by_type[DatasetTest::COUNTED_OK] = 2;
                expected.by_type[DatasetTest::COUNTED_TO_PACK] = 1;
                wassert(actual(writer.get()).maintenance(expected));
            }

#if 0
            stringstream s;

        // Rescanning a 6G+ file with grib_api is SLOW!

            // Perform full maintenance and check that things are still ok afterwards
            writer.check(s, true, false);
            ensure_equals(s.str(),
                "testdir: rescanned 2007/07.grib1\n"
                "testdir: 1 file rescanned, 7736 bytes reclaimed cleaning the index.\n");
            c.clear();
            writer.maintenance(c);
            ensure_equals(c.count(COUNTED_OK), 1u);
            ensure_equals(c.count(COUNTED_TO_PACK), 1u);
            ensure_equals(c.remaining(), "");
            ensure(not c.isClean());

            // Perform packing and check that things are still ok afterwards
            s.str(std::string());
            writer.repack(s, true);
            ensure_equals(s.str(), 
                "testdir: packed 2007/07.grib1 (34960 saved)\n"
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
        add_method("repack_deleted", [](Fixture& f) {
            // Test accuracy of maintenance scan, on dataset with one file deleted,
            // performing repack
            wruntest(f.test_deleted_datafile_repack, testdata::GRIBData());
            wruntest(f.test_deleted_datafile_repack, testdata::BUFRData());
            wruntest(f.test_deleted_datafile_repack, testdata::VM2Data());
            wruntest(f.test_deleted_datafile_repack, testdata::ODIMData());
        });
        add_method("check_deleted", [](Fixture& f) {
            // Test accuracy of maintenance scan, on dataset with one file deleted,
            // performing check
            wruntest(f.test_deleted_datafile_check, testdata::GRIBData());
            wruntest(f.test_deleted_datafile_check, testdata::BUFRData());
            wruntest(f.test_deleted_datafile_check, testdata::VM2Data());
            wruntest(f.test_deleted_datafile_check, testdata::ODIMData());
        });
        add_method("scan_noindex", [](Fixture& f) {
            // Test accuracy of maintenance scan, after deleting the index, with some
            // spurious extra files in the dataset
            wruntest(f.test_deleted_index_check, testdata::GRIBData());
            wruntest(f.test_deleted_index_check, testdata::BUFRData());
            wruntest(f.test_deleted_index_check, testdata::VM2Data());
            wruntest(f.test_deleted_index_check, testdata::ODIMData());
        });
        add_method("scan_randomfiles", [](Fixture& f) {
            // Test recreating a dataset from random datafiles
            system("rm -rf testds");
            system("mkdir testds");
            system("mkdir testds/foo");
            system("mkdir testds/foo/bar");
            system("cp inbound/test.grib1 testds/foo/bar/");
            system("echo 'GRIB garbage 7777' > testds/foo/bar/test.grib1.tmp");

            // See if the files to index are detected in the correct number
            {
                auto writer(f.makeLocalChecker());
                MaintenanceResults expected(false, 1);
                expected.by_type[DatasetTest::COUNTED_TO_INDEX] = 1;
                wassert(actual(writer.get()).maintenance(expected));
            }

            // Perform full maintenance and check that things are still ok afterwards
            {
                auto writer(f.makeLocalChecker());
                ReporterExpected e;
                e.rescanned.emplace_back("testds", "foo/bar/test.grib1");
                wassert(actual(writer.get()).check(e, true, true));

                // A repack is still needed because the data is not sorted by reftime
                MaintenanceResults expected(false, 1);
                expected.by_type[DatasetTest::COUNTED_TO_PACK] = 1;
                wassert(actual(writer.get()).maintenance(expected));
            }

            ensure(sys::exists("testds/foo/bar/test.grib1.tmp"));
            ensure_equals(sys::size("testds/foo/bar/test.grib1"), 44412u);

            // Perform packing and check that things are still ok afterwards
            {
                auto writer(f.makeLocalChecker());
                ReporterExpected e;
                e.repacked.emplace_back("testds", "foo/bar/test.grib1");
                wassert(actual(writer.get()).repack(e, true));
            }
            f.ensure_maint_clean(1);

            ensure_equals(sys::size("testds/foo/bar/test.grib1"), 44412u);

            // Test querying
            {
                std::unique_ptr<Reader> reader(f.makeReader(&f.cfg));
                metadata::Collection mdc(*reader, Matcher::parse("origin:GRIB1,200"));
                ensure_equals(mdc.size(), 1u);
                wassert(actual_type(mdc[0].source()).is_source_blob("grib1", sys::abspath("testds"), "foo/bar/test.grib1", 34960, 7218));
            }
        });
        add_method("repack_timestamps", [](Fixture& f) {
            // Ensure that if repacking changes the data file timestamp, it reindexes it properly
            f.clean_and_import();

            // Ensure the archive appears clean
            f.ensure_maint_clean(3);

            // Change timestamp and rescan the file
            {
                struct utimbuf oldts = { 199926000, 199926000 };
                ensure(utime("testds/2007/07-08.grib1", &oldts) == 0);

                auto writer(f.makeLocalChecker());
                writer->rescanFile("2007/07-08.grib1");
            }

            // Ensure that the archive is still clean
            f.ensure_maint_clean(3);

            // Repack the file
            {
                auto writer(f.makeLocalChecker());
                ensure_equals(writer->repackFile("2007/07-08.grib1"), 0u);
            }

            // Ensure that the archive is still clean
            f.ensure_maint_clean(3);
        });
        add_method("repack_timestamps", [](Fixture& f) {
            // Test accuracy of maintenance scan, on a dataset with one file to both repack and delete

            // Data are from 07, 08, 10 2007
            int treshold[6] = { 2008, 1, 1, 0, 0, 0 };
            int now[6];
            wibble::grcal::date::now(now);
            long long int duration = wibble::grcal::date::duration(treshold, now);

            system("rm -rf testds");
            system("mkdir testds");
            system("mkdir testds/2007");
            system("cp inbound/test.grib1 testds/2007/");

            ConfigFile cfg = f.cfg;
            cfg.setValue("step", "yearly");
            cfg.setValue("delete age", duration/(3600*24));

            // Run maintenance to build the dataset
            {
                auto writer(f.makeLocalChecker(&cfg));
                ReporterExpected e;
                e.rescanned.emplace_back("", "2007/test.grib1");
                wassert(actual(writer.get()).check(e, true, true));

                arki::tests::MaintenanceResults expected(false, 1);
                // A repack is still needed because the data is not sorted by reftime
                expected.by_type[DatasetTest::COUNTED_TO_PACK] = 1;
                // And the same file is also old enough to be deleted
                expected.by_type[DatasetTest::COUNTED_TO_DELETE] = 1;
                wassert(actual(writer.get()).maintenance(expected));
            }

            // Perform packing and check that things are still ok afterwards
            {
                auto writer(f.makeLocalChecker(&cfg));
                ReporterExpected e;
                e.deleted.emplace_back("", "2007/test.grib1");
                wassert(actual(writer.get()).repack(e, true));
            }
            f.ensure_maint_clean(0);

            // Perform full maintenance and check that things are still ok afterwards
            {
                auto writer(f.makeLocalChecker(&cfg));
                wassert(actual(writer.get()).check_clean(true, true));
                f.ensure_maint_clean(0);
            }
        });
        add_method("scan_repack_archive", [](Fixture& f) {
            // Test accuracy of maintenance scan, on a dataset with one file to both repack and archive

            // Data are from 07, 08, 10 2007
            int treshold[6] = { 2008, 1, 1, 0, 0, 0 };
            int now[6];
            wibble::grcal::date::now(now);
            long long int duration = wibble::grcal::date::duration(treshold, now);

            system("rm -rf testds");
            system("mkdir testds");
            system("mkdir testds/2007");
            system("cp inbound/test.grib1 testds/2007/");

            ConfigFile cfg = f.cfg;
            cfg.setValue("step", "yearly");
            cfg.setValue("archive age", duration/(3600*24));

            // Run maintenance to build the dataset
            {
                auto writer(f.makeLocalChecker(&cfg));

                ReporterExpected e;
                e.rescanned.emplace_back("", "2007/test.grib1");
                wassert(actual(writer.get()).check(e, true, true));

                MaintenanceResults expected(false, 1);
                // A repack is still needed because the data is not sorted by reftime
                expected.by_type[DatasetTest::COUNTED_TO_PACK] = 1;
                // And the same file is also old enough to be deleted
                expected.by_type[DatasetTest::COUNTED_TO_ARCHIVE] = 1;
                wassert(actual(writer.get()).maintenance(expected));
            }

            // Perform packing and check that things are still ok afterwards
            {
                auto writer(f.makeLocalChecker(&cfg));

                ReporterExpected e;
                e.repacked.emplace_back("", "2007/test.grib1");
                e.archived.emplace_back("", "2007/test.grib1");
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
