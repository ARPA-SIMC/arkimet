/*
 * Copyright (C) 2007--2015  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/dataset/tests.h>
#include <arki/dataset/maintenance.h>
#include <arki/dataset/local.h>
#include <arki/metadata/collection.h>
#include <arki/types/source/blob.h>
#include <arki/utils/files.h>
#include <arki/nag.h>
#include <wibble/sys/fs.h>
#include <wibble/grcal/grcal.h>

#include <cstdlib>
#include <sys/types.h>
#include <utime.h>
#include <unistd.h>

using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::utils;

namespace {

struct arki_dataset_maintenance_base : public arki::tests::DatasetTest {
    arki_dataset_maintenance_base()
    {
        cfg.setValue("path", "testds");
        cfg.setValue("name", "testds");
        cfg.setValue("unique", "reftime, origin, product, level, timerange, area");
        cfg.setValue("step", "daily");
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
        throw wibble::exception::Consistency("second in file not found");
    }

    void test_preconditions(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        wassert(actual(fixture.fnames_before_cutoff.size()) > 0);
        wassert(actual(fixture.fnames_after_cutoff.size()) > 0);
        wassert(actual(fixture.fnames_before_cutoff.size() + fixture.fnames_after_cutoff.size()) == fixture.count_dataset_files());
    }

    void test_maintenance_on_clean(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        unsigned file_count = fixture.count_dataset_files();

        wruntest(import_all, fixture);

        // Ensure everything appears clean
        {
            std::auto_ptr<WritableLocal> writer(makeLocalWriter());
            wassert(actual(writer.get()).maintenance_clean(file_count));

            // Check that maintenance does not accidentally create an archive
            wassert(!actual("testds/.archive").fileexists());
        }

        // Ensure packing has nothing to report
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            stringstream s;
            writer->repack(s, false);
            wassert(actual(s.str()) == "");

            wassert(actual(writer.get()).maintenance_clean(file_count));
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            stringstream s;
            writer->repack(s, true);
            wassert(actual(s.str()) == "");

            wassert(actual(writer.get()).maintenance_clean(file_count));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            stringstream s;
            writer->check(s, true, true);
            wassert(actual(s.str()) == ""); // Nothing should have happened

            wassert(actual(writer.get()).maintenance_clean(file_count));
        }
    }

    void test_move_to_archive(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        cfg.setValue("archive age", str::fmt(fixture.selective_days_since()));
        wruntest(import_all, fixture);

        // Check if files to archive are detected
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(false, fixture.count_dataset_files());
            expected.by_type[COUNTED_OK] = fixture.fnames_after_cutoff.size();
            expected.by_type[COUNTED_TO_ARCHIVE] = fixture.fnames_before_cutoff.size();
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            for (set<string>::const_iterator i = fixture.fnames_before_cutoff.begin();
                    i != fixture.fnames_before_cutoff.end(); ++i)
                s.require_line_contains(": archived " + *i);
            s.require_line_contains(": archive cleaned up");
            s.require_line_contains_re(str::fmtf(": %zd files? archived", fixture.fnames_before_cutoff.size()));
            wassert(actual(writer.get()).repack(s, true));
        }

        // Check that the files have been moved to the archive
        for (set<string>::const_iterator i = fixture.fnames_before_cutoff.begin();
                i != fixture.fnames_before_cutoff.end(); ++i)
        {
            wassert(actual("testds/.archive/last/" + *i).fileexists());
            wassert(actual("testds/.archive/last/" + *i + ".metadata").fileexists());
            wassert(actual("testds/.archive/last/" + *i + ".summary").fileexists());
            wassert(!actual("testds/" + *i).fileexists());
        }

        // Maintenance should now show a normal situation
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(true, fixture.count_dataset_files());
            expected.by_type[COUNTED_OK] = fixture.fnames_after_cutoff.size();
            expected.by_type[COUNTED_ARC_OK] = fixture.fnames_before_cutoff.size();
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            stringstream s;
            s.str(std::string());
            writer->check(s, true, true);
            wassert(actual(s.str()) == ""); // Nothing should have happened

            arki::tests::MaintenanceResults expected(true, fixture.count_dataset_files());
            expected.by_type[COUNTED_OK] = fixture.fnames_after_cutoff.size();
            expected.by_type[COUNTED_ARC_OK] = fixture.fnames_before_cutoff.size();
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Test that querying returns all items
        {
            std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));

            metadata::Counter counter;
            reader->queryData(dataset::DataQuery(Matcher::parse("")), counter);
            wassert(actual(counter.count) == 3);
        }
    }

    void test_delete_age(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        cfg.setValue("delete age", str::fmt(fixture.selective_days_since()));
        wruntest(import_all, fixture);

        // Check if files to delete are detected
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(false, fixture.count_dataset_files());
            expected.by_type[COUNTED_OK] = fixture.fnames_after_cutoff.size();
            expected.by_type[COUNTED_TO_DELETE] = fixture.fnames_before_cutoff.size();
            wassert(actual(writer.get()).maintenance(expected));
        }
        // Perform packing and check that things are still ok afterwards
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            for (set<string>::const_iterator i = fixture.fnames_before_cutoff.begin();
                    i != fixture.fnames_before_cutoff.end(); ++i)
                s.require_line_contains(": deleted " + *i);
            s.require_line_contains_re(str::fmtf(": %zd files? deleted, %zd files? removed from index",
                        fixture.fnames_before_cutoff.size(),
                        fixture.fnames_before_cutoff.size()));
            wassert(actual(writer.get()).repack(s, true));
        }

        auto_ptr<WritableLocal> writer(makeLocalWriter());
        wassert(actual(writer.get()).maintenance_clean(fixture.fnames_after_cutoff.size()));
    }

    void test_truncated_datafile_at_data_boundary(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        cfg.setValue("step", fixture.max_selective_aggregation);
        wruntest(import_all, fixture);

        // Find the imported_result element whose offset is > 0
        const source::Blob& second_in_segment = find_imported_second_in_file();
        wassert(actual(second_in_segment.offset) > 0);

        // Truncate at the position in second_in_segment
        string truncated_fname = second_in_segment.filename.substr(7);
        segments().truncate(truncated_fname, second_in_segment.offset);

        // See that the truncated file is detected
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(false, 2);
            expected.by_type[COUNTED_OK] = 1;
            expected.by_type[COUNTED_TO_RESCAN] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform packing and check that nothing has happened
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            wassert(actual(writer.get()).repack_clean(true));

            arki::tests::MaintenanceResults expected(false, 2);
            expected.by_type[COUNTED_OK] = 1;
            expected.by_type[COUNTED_TO_RESCAN] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());

            LineChecker s;
            s.require_line_contains(": rescanned " + truncated_fname);
            s.require_line_contains(": 1 file rescanned");
            wassert(actual(writer.get()).check(s, true, true));

            wassert(actual(writer.get()).maintenance_clean(2));
        }

        // Try querying and make sure we get the two files
        {
            std::auto_ptr<ReadonlyDataset> reader(makeReader());

            metadata::Counter counter;
            reader->queryData(dataset::DataQuery(Matcher::parse("")), counter);
            wassert(actual(counter.count) == 2);
        }
    }

    void test_corrupted_datafile(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        cfg.setValue("step", fixture.max_selective_aggregation);
        wruntest(import_all_packed, fixture);

        // Find the imported_result element whose offset is > 0
        const source::Blob& second_in_segment = find_imported_second_in_file();
        wassert(actual(second_in_segment.offset) > 0);

        // Corrupt the first datum in the file
        corrupt_datafile(second_in_segment.absolutePathname());

        // A quick check has nothing to complain
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            wassert(actual(writer.get()).maintenance_clean(2));
        }

        // A thorough check should find the corruption
        {
            //nag::TestCollect tc;
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(false, 2);
            expected.by_type[COUNTED_OK] = 1;
            expected.by_type[COUNTED_TO_RESCAN] = 1;
            wassert(actual(writer.get()).maintenance(expected, false));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
            LineChecker s;
            s.require_line_contains(": rescanned " + second_in_segment.filename.substr(7));
            s.require_line_contains(": 1 file rescanned");
            wassert(actual(writer.get()).check(s, true, false));

            // The corrupted file has been deindexed, now there is a gap in the data file
            arki::tests::MaintenanceResults expected(false, 2);
            expected.by_type[COUNTED_OK] = 1;
            expected.by_type[COUNTED_TO_PACK] = 1;
            wassert(actual(writer.get()).maintenance(expected, false));
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
            LineChecker s;
            s.require_line_contains(": packed " + second_in_segment.filename.substr(7));
            s.require_line_contains(": 1 file packed");
            wassert(actual(writer.get()).repack(s, true));
        }

        // Maintenance and pack are ok now
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            wassert(actual(writer.get()).maintenance_clean(2));
            wassert(actual(writer.get()).maintenance_clean(2, false));
        }
    }

    void test_deleted_datafile_repack(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        wruntest(import_all_packed, fixture);
        string deleted_fname = import_results[0].sourceBlob().filename.substr(7);
        unsigned file_count = fixture.count_dataset_files();
        segments().remove(deleted_fname);

        // Initial check finds the deleted file
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(false, file_count);
            expected.by_type[COUNTED_OK] = file_count - 1;
            expected.by_type[COUNTED_TO_DEINDEX] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Packing should notice the problem and do nothing
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
            LineChecker s;
            s.require_line_contains(": " + deleted_fname + " should be removed from the index");
            s.require_line_contains(": 1 file should be removed from the index");
            wassert(actual(writer.get()).repack(s, false));

            arki::tests::MaintenanceResults expected(false, file_count);
            expected.by_type[COUNTED_OK] = file_count - 1;
            expected.by_type[COUNTED_TO_DEINDEX] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform packing and check that things are still ok afterwards
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
            LineChecker s;
            s.require_line_contains(": deleted from index " + deleted_fname);
            s.require_line_contains(": 1 file removed from index");
            wassert(actual(writer.get()).repack(s, true));

            wassert(actual(writer.get()).maintenance_clean(file_count - 1));
        }
    }

    void test_deleted_datafile_check(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        wruntest(import_all_packed, fixture);
        string deleted_fname = import_results[0].sourceBlob().filename.substr(7);
        unsigned file_count = fixture.count_dataset_files();
        segments().remove(deleted_fname);

        // Initial check finds the deleted file
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(false, file_count);
            expected.by_type[COUNTED_OK] = file_count - 1;
            expected.by_type[COUNTED_TO_DEINDEX] = 1;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            s.require_line_contains(": deindexed " + deleted_fname);
            s.require_line_contains(": 1 file removed from index");
            wassert(actual(writer.get()).check(s, true, true));

            wassert(actual(writer.get()).maintenance_clean(file_count - 1));
        }
    }

    void test_deleted_index_check(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
    {
        unsigned file_count = fixture.count_dataset_files();
        wruntest(import_all_packed, fixture);
        sys::fs::deleteIfExists("testds/index.sqlite");
        sys::fs::deleteIfExists("testds/MANIFEST");
        sys::fs::mkpath("testds/2014/");
        system("echo 'GRIB garbage 7777' > testds/2014/01.grib1.tmp");

        // See if the files to index are detected in the correct number
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            arki::tests::MaintenanceResults expected(false, file_count);
            expected.by_type[COUNTED_TO_INDEX] = file_count;
            wassert(actual(writer.get()).maintenance(expected));
        }

        // Perform full maintenance and check that things are still ok afterwards
        {
            auto_ptr<WritableLocal> writer(makeLocalWriter());
            LineChecker s;
            for (set<string>::const_iterator i = fixture.fnames_before_cutoff.begin();
                    i != fixture.fnames_before_cutoff.end(); ++i)
                s.require_line_contains(": rescanned " + *i);
            for (set<string>::const_iterator i = fixture.fnames_after_cutoff.begin();
                    i != fixture.fnames_after_cutoff.end(); ++i)
                s.require_line_contains(": rescanned " + *i);
            s.require_line_contains(str::fmtf(": %d files rescanned", file_count));
            wassert(actual(writer.get()).check(s, true, true));

            wassert(actual(writer.get()).maintenance_clean(file_count));
            wassert(actual(writer.get()).repack_clean(true));
        }

        // The spurious file should not have been touched
        wassert(actual("testds/2014/01.grib1.tmp").fileexists());
    }
};

}

namespace tut {

typedef dataset_tg<arki_dataset_maintenance_base> tg;
typedef tg::object to;

// Test accuracy of maintenance scan, on perfect dataset
template<> template<> void to::test<1>()
{
    wruntest(test_preconditions, testdata::GRIBData());
    wruntest(test_preconditions, testdata::BUFRData());
    wruntest(test_preconditions, testdata::VM2Data());
    wruntest(test_preconditions, testdata::ODIMData());

    wruntest(test_maintenance_on_clean, testdata::GRIBData());
    wruntest(test_maintenance_on_clean, testdata::BUFRData());
    wruntest(test_maintenance_on_clean, testdata::VM2Data());
    wruntest(test_maintenance_on_clean, testdata::ODIMData());
}

// Test accuracy of maintenance scan, on perfect dataset, with data to archive
template<> template<> void to::test<2>()
{
    nag::TestCollect tc;
    wruntest(test_move_to_archive, testdata::GRIBData());
    wruntest(test_move_to_archive, testdata::BUFRData());
    wruntest(test_move_to_archive, testdata::VM2Data());
    wruntest(test_move_to_archive, testdata::ODIMData());
    tc.clear();
}

// Test accuracy of maintenance scan, on perfect dataset, with data to delete
template<> template<> void to::test<3>()
{
    wruntest(test_delete_age, testdata::GRIBData());
    wruntest(test_delete_age, testdata::BUFRData());
    wruntest(test_delete_age, testdata::VM2Data());
    wruntest(test_delete_age, testdata::ODIMData());
}

// Test accuracy of maintenance scan, on perfect dataset, with a truncated data file
template<> template<> void to::test<4>()
{
    wruntest(test_truncated_datafile_at_data_boundary, testdata::GRIBData());
    wruntest(test_truncated_datafile_at_data_boundary, testdata::BUFRData());
    wruntest(test_truncated_datafile_at_data_boundary, testdata::VM2Data());
    wruntest(test_truncated_datafile_at_data_boundary, testdata::ODIMData());
}

// Test accuracy of maintenance scan, on a dataset with a corrupted data file
template<> template<> void to::test<5>()
{
    /**
     * Here we have inconsistent behaviou across segment types and data types,
     * because:
     *  - some formats detect corruption, some formats skip garbage
     *  - concatenated data in files may skip corrupted data as garbage, but
     *  directory segments are already split in separate data units and will
     *  load, instead of skipping, corrupted data
     */
    wruntest(test_corrupted_datafile, testdata::GRIBData());
    wruntest(test_corrupted_datafile, testdata::BUFRData());
    // TODO: VM2 scanning does not yet deal gracefully with corruption
    wruntest(test_corrupted_datafile, testdata::VM2Data());
    // TODO: ODIM scanning does not yet deal gracefully with corruption
    wruntest(test_corrupted_datafile, testdata::ODIMData());
}

// Test accuracy of maintenance scan, on a dataset with a data file larger than 2**31
template<> template<> void to::test<6>()
{
	if (cfg.value("type") == "simple") return; // TODO: we need to avoid the SLOOOOW rescan done by simple on the data file
	clean();

	// Simulate 2007/07-07.grib1 to be 6G already
	system("mkdir -p testds/2007");
	system("touch testds/2007/07-07.grib1");
	// Truncate the last grib out of a file
	if (truncate("testds/2007/07-07.grib1", 6000000000LLU) < 0)
		throw wibble::exception::System("truncating testds/2007/07-07.grib1");

	import();

	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		MaintenanceCollector c;
		writer->maintenance(c, false);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 2u);
		ensure_equals(c.count(COUNTED_TO_PACK), 1u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
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
}

// Test accuracy of maintenance scan, on dataset with one file deleted,
// performing repack
template<> template<> void to::test<7>()
{
    wruntest(test_deleted_datafile_repack, testdata::GRIBData());
    wruntest(test_deleted_datafile_repack, testdata::BUFRData());
    wruntest(test_deleted_datafile_repack, testdata::VM2Data());
    wruntest(test_deleted_datafile_repack, testdata::ODIMData());
}

// Test accuracy of maintenance scan, on dataset with one file deleted,
// performing check
template<> template<> void to::test<8>()
{
    wruntest(test_deleted_datafile_check, testdata::GRIBData());
    wruntest(test_deleted_datafile_check, testdata::BUFRData());
    wruntest(test_deleted_datafile_check, testdata::VM2Data());
    wruntest(test_deleted_datafile_check, testdata::ODIMData());
}

// Test accuracy of maintenance scan, after deleting the index, with some
// spurious extra files in the dataset
template<> template<> void to::test<9>()
{
    wruntest(test_deleted_index_check, testdata::GRIBData());
    wruntest(test_deleted_index_check, testdata::BUFRData());
    wruntest(test_deleted_index_check, testdata::VM2Data());
    wruntest(test_deleted_index_check, testdata::ODIMData());
}

// Test recreating a dataset from random datafiles
template<> template<> void to::test<10>()
{
	system("rm -rf testds");
	system("mkdir testds");
	system("mkdir testds/foo");
	system("mkdir testds/foo/bar");
	system("cp inbound/test.grib1 testds/foo/bar/");
	system("echo 'GRIB garbage 7777' > testds/foo/bar/test.grib1.tmp");

	// See if the files to index are detected in the correct number
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_TO_INDEX), 1u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		OutputChecker s;
		writer->check(s, true, true);
		s.ensure_line_contains(": rescanned foo/bar/test.grib1");
		s.ensure_line_contains("1 file rescanned");
		s.ensure_all_lines_seen();

		MaintenanceCollector c;
		writer->maintenance(c);
		// A repack is still needed because the data is not sorted by reftime
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_TO_PACK), 1u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

    ensure(sys::fs::exists("testds/foo/bar/test.grib1.tmp"));
    ensure_equals(sys::fs::size("testds/foo/bar/test.grib1"), 44412u);

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": packed foo/bar/test.grib1");
		s.ensure_line_contains(": 1 file packed");
	}
	ensure_maint_clean(1);

    ensure_equals(sys::fs::size("testds/foo/bar/test.grib1"), 44412u);

	// Test querying
	{
		std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));

		metadata::Collection mdc;
		reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
		ensure_equals(mdc.size(), 1u);
        wassert(actual_type(mdc[0].source()).is_source_blob("grib1", sys::fs::abspath("testds"), "foo/bar/test.grib1", 34960, 7218));
	}
}

// Ensure that if repacking changes the data file timestamp, it reindexes it properly
template<> template<> void to::test<11>()
{
	clean_and_import();

	// Ensure the archive appears clean
	ensure_maint_clean(3);

	// Change timestamp and rescan the file
	{
		struct utimbuf oldts = { 199926000, 199926000 };
		ensure(utime("testds/2007/07-08.grib1", &oldts) == 0);

		auto_ptr<WritableLocal> writer(makeLocalWriter());
		writer->rescanFile("2007/07-08.grib1");
	}

	// Ensure that the archive is still clean
	ensure_maint_clean(3);

	// Repack the file
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		ensure_equals(writer->repackFile("2007/07-08.grib1"), 0u);
	}

	// Ensure that the archive is still clean
	ensure_maint_clean(3);
}

// Test accuracy of maintenance scan, on a dataset with one file to both repack and delete
template<> template<> void to::test<12>()
{
    // Data are from 07, 08, 10 2007
    int treshold[6] = { 2008, 1, 1, 0, 0, 0 };
    int now[6];
    grcal::date::now(now);
    long long int duration = grcal::date::duration(treshold, now);

    system("rm -rf testds");
    system("mkdir testds");
    system("mkdir testds/2007");
    system("cp inbound/test.grib1 testds/2007/");

    ConfigFile cfg = this->cfg;
    cfg.setValue("step", "yearly");
    cfg.setValue("delete age", str::fmt(duration/(3600*24)));

    // Run maintenance to build the dataset
    {
        auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
        OutputChecker s;
        writer->check(s, true, true);
        s.ensure_line_contains(": rescanned 2007/test.grib1");
        s.ensure_line_contains("1 file rescanned");
        s.ensure_all_lines_seen();

        arki::tests::MaintenanceResults expected(false, 1);
        // A repack is still needed because the data is not sorted by reftime
        expected.by_type[COUNTED_TO_PACK] = 1;
        // And the same file is also old enough to be deleted
        expected.by_type[COUNTED_TO_DELETE] = 1;
        wassert(actual(writer.get()).maintenance(expected));
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));

        OutputChecker s;
        writer->repack(s, true);
        s.ensure_line_contains(": deleted 2007/test.grib1");
        s.ensure_line_contains(": 1 file deleted, 1 file removed from index, 44412 total bytes freed.");
        s.ensure_all_lines_seen();
    }
    ensure_maint_clean(0);

    // Perform full maintenance and check that things are still ok afterwards
    {
        auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
        stringstream s;
        writer->check(s, true, true);
        ensure_equals(s.str(), string()); // Nothing should have happened

        ensure_maint_clean(0);
    }
}

// Test accuracy of maintenance scan, on a dataset with one file to both repack and archive
template<> template<> void to::test<13>()
{
    // Data are from 07, 08, 10 2007
    int treshold[6] = { 2008, 1, 1, 0, 0, 0 };
    int now[6];
    grcal::date::now(now);
    long long int duration = grcal::date::duration(treshold, now);

    system("rm -rf testds");
    system("mkdir testds");
    system("mkdir testds/2007");
    system("cp inbound/test.grib1 testds/2007/");

    ConfigFile cfg = this->cfg;
    cfg.setValue("step", "yearly");
    cfg.setValue("archive age", str::fmt(duration/(3600*24)));

    // Run maintenance to build the dataset
    {
        auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
        OutputChecker s;
        writer->check(s, true, true);
        s.ensure_line_contains(": rescanned 2007/test.grib1");
        s.ensure_line_contains("1 file rescanned");
        s.ensure_all_lines_seen();

        MaintenanceCollector c;
        writer->maintenance(c);
        ensure_equals(c.fileStates.size(), 1u);
        // A repack is still needed because the data is not sorted by reftime
        ensure_equals(c.count(COUNTED_TO_PACK), 1u);
        // And the same file is also old enough to be deleted
        ensure_equals(c.count(COUNTED_TO_ARCHIVE), 1u);
        ensure_equals(c.remaining(), "");
        ensure(not c.isClean());
    }

    // Perform packing and check that things are still ok afterwards
    {
        auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));

        OutputChecker s;
        writer->repack(s, true);
        s.ensure_line_contains(": packed 2007/test.grib1");
        s.ensure_line_contains(": archived 2007/test.grib1");
        s.ensure_line_contains(": 1 file packed, 1 file archived.");
        s.ensure_line_contains(": archive cleaned up");
        s.ensure_all_lines_seen();
    }
}

}

namespace {

tut::tg test_ondisk2("arki_dataset_maintenance_ondisk2", "type=ondisk2\n");
tut::tg test_simple_plain("arki_dataset_maintenance_simple_plain", "type=simple\nindex_type=plain\n");
tut::tg test_simple_sqlite("arki_dataset_maintenance_simple_sqlite", "type=simple\nindex_type=sqlite");
tut::tg test_ondisk2_dir("arki_dataset_maintenance_ondisk2_dirs", "type=ondisk2\nsegments=dir\n");
tut::tg test_simple_plain_dir("arki_dataset_maintenance_simple_plain_dirs", "type=simple\nindex_type=plain\nsegments=dir\n");
tut::tg test_simple_sqlite_dir("arki_dataset_maintenance_simple_sqlite_dirs", "type=simple\nindex_type=sqlite\nsegments=dir\n");

}
