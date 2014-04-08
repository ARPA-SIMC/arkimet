/*
 * Copyright (C) 2007--2013  Enrico Zini <enrico@enricozini.org>
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

#include "config.h"

#include <arki/dataset/tests.h>
#include <arki/dataset/maintenance.h>
#include <arki/dataset/local.h>
#include <arki/metadata/collection.h>
#include <arki/utils/files.h>
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
using namespace arki::dataset;
using namespace arki::utils;

namespace {

struct arki_dataset_maintenance_base : public arki::tests::DatasetTest {
    arki_dataset_maintenance_base()
    {
        cfg.setValue("path", "testds");
        cfg.setValue("name", "testds");
        cfg.setValue("step", "daily");
    }
};

}

namespace tut {

typedef dataset_tg<arki_dataset_maintenance_base> tg;
typedef tg::object to;

// Test accuracy of maintenance scan, on perfect dataset
template<> template<> void to::test<1>()
{
	clean_and_import();

	// Ensure the archive appears clean
	{
		ensure_maint_clean(3);

		// Check that maintenance does not accidentally create an archive
		ensure(!sys::fs::exists("testds/.archive"));
	}

	// Ensure packing has nothing to report
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		stringstream s;
		writer->repack(s, false);
		ensure_equals(s.str(), "");

		ensure_maint_clean(3);
	}

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		stringstream s;
		writer->repack(s, true);
		ensure_not_contains(s.str(), "2007/");

		ensure_maint_clean(3);
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		stringstream s;
		writer->check(s, true, true);
		ensure_equals(s.str(), string()); // Nothing should have happened

		ensure_maint_clean(3);
	}
}

// Test accuracy of maintenance scan, on perfect dataset, with data to archive
template<> template<> void to::test<2>()
{
	ConfigFile cfg = this->cfg;
	cfg.setValue("archive age", str::fmt(days_since(2007, 9, 1)));

	clean_and_import(&cfg);

	// Check if files to archive are detected
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));

		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 1u);
		ensure_equals(c.count(COUNTED_TO_ARCHIVE), 2u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": archived 2007/07-07.grib1");
		s.ensure_line_contains(": archived 2007/07-08.grib1");
		s.ensure_line_contains(": archive cleaned up");
		s.ensure_line_contains(": 2 files archived");
		s.ensure_all_lines_seen();
	}

	// Check that the files have been moved to the archive
	ensure(sys::fs::exists("testds/.archive/last/2007/07-07.grib1"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-07.grib1.metadata"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-07.grib1.summary"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-08.grib1"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-08.grib1.metadata"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-08.grib1.summary"));
	ensure(!sys::fs::exists("testds/2007/07-07.grib1"));
	ensure(!sys::fs::exists("testds/2007/07-08.grib1"));

	// Maintenance should now show a normal situation
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 1u);
		ensure_equals(c.count(COUNTED_ARC_OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		stringstream s;
		s.str(std::string());
		writer->check(s, true, true);
		ensure_equals(s.str(), string()); // Nothing should have happened

		MaintenanceCollector c;
		c.clear();
		writer->maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 1u);
		ensure_equals(c.count(COUNTED_ARC_OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Test that querying returns all items
	{
		std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));

		metadata::Counter counter;
		reader->queryData(dataset::DataQuery(Matcher::parse(""), false), counter);
		ensure_equals(counter.count, 3u);
	}
}

// Test accuracy of maintenance scan, on perfect dataset, with data to delete
template<> template<> void to::test<3>()
{
	// Data are from 07, 08, 10 2007
	int treshold[6] = { 2007, 9, 1, 0, 0, 0 };
	int now[6];
	grcal::date::now(now);
	long long int duration = grcal::date::duration(treshold, now);

	ConfigFile cfg = this->cfg;
	cfg.setValue("delete age", str::fmt(duration/(3600*24)));
	clean_and_import(&cfg);

	// Check that expired files are detected
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 1u);
		ensure_equals(c.count(COUNTED_TO_DELETE), 2u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));

		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": deleted 2007/07-07.grib1");
		s.ensure_line_contains(": deleted 2007/07-08.grib1");
		s.ensure_line_contains(": 2 files deleted, 2 files removed from index");
		s.ensure_all_lines_seen();
	}
	ensure_maint_clean(1);

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		stringstream s;
		writer->check(s, true, true);
		ensure_equals(s.str(), string()); // Nothing should have happened

		ensure_maint_clean(1);
	}
}

// Test accuracy of maintenance scan, on perfect dataset, with a truncated data file
template<> template<> void to::test<4>()
{
	ConfigFile cfg = this->cfg;
	cfg.setValue("step", "yearly");
	clean_and_import(&cfg);

	// Truncate the last grib out of a file
	if (truncate("testds/20/2007.grib1", 42178) < 0)
		throw wibble::exception::System("truncating testds/20/2007.grib1");

	// See that the truncated file is detected
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(COUNTED_OK), 0u);
		ensure_equals(c.count(COUNTED_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		OutputChecker s;

		writer->repack(s, true);
		s.ignore_line_containing("total bytes freed.");
		s.ensure_all_lines_seen();

		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 0u);
		ensure_equals(c.count(COUNTED_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));

		OutputChecker s;
		writer->check(s, true, true);
		s.ensure_line_contains(": rescanned 20/2007.grib1");
		s.ensure_line_contains(": 1 file rescanned");
		s.ensure_all_lines_seen();

		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 0u);
		ensure_equals(c.count(COUNTED_TO_PACK), 1u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform packing after the file has been rescanned and check that
	// things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));

		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": packed 20/2007.grib1");
		s.ensure_line_contains(": 1 file packed");
		s.ensure_all_lines_seen();

        arki::tests::MaintenanceResults expected(true, 1);
        expected.by_type[COUNTED_OK] = 1;
        wassert(actual(writer.get()).maintenance(expected));
	}
}

// Test accuracy of maintenance scan, on a dataset with a corrupted data file
template<> template<> void to::test<5>()
{
	ConfigFile cfg = this->cfg;
	cfg.setValue("step", "monthly");
	clean_and_import(&cfg);

	// Pack the dataset because 07.grib1 imported data out of order
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		stringstream s;
		writer->repack(s, true);
		ensure_contains(s.str(), ": packed 2007/07.grib1");
		ensure_contains(s.str(), ": 1 file packed");
	}

	// Corrupt the first grib in the file
        system("touch savets -r testds/2007/07.grib1 &&"
               "dd if=/dev/zero of=testds/2007/07.grib1 bs=1 count=2 conv=notrunc > /dev/null 2>&1 &&"
               "touch testds/2007/07.grib1 -r savets &&"
               "rm savets");

	// A quick check has nothing to complain
	ensure_maint_clean(2);

	// A thorough check should find the corruption
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		MaintenanceCollector c;
		writer->maintenance(c, false);
		ensure_equals(c.fileStates.size(), 2u);
		ensure_equals(c.count(COUNTED_OK), 1u);
		ensure_equals(c.count(COUNTED_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		stringstream s;
		writer->check(s, true, false);
		ensure_contains(s.str(), ": rescanned 2007/07.grib1");
		ensure_contains(s.str(), ": 1 file rescanned");

		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 1u);
		ensure_equals(c.count(COUNTED_TO_PACK), 1u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		stringstream s;
		writer->repack(s, true);
		ensure_contains(s.str(), ": packed 2007/07.grib1");
		ensure_contains(s.str(), ": 1 file packed");
	}

	// Maintenance and pack are ok now
	ensure_maint_clean(2);
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		stringstream s;
		writer->repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}
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
	clean_and_import();
	system("rm testds/2007/07-07.grib1");

	// Initial check finds the deleted file
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 2u);
		ensure_equals(c.count(COUNTED_TO_DEINDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Packing has something to report
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		OutputChecker s;
		writer->repack(s, false);
		s.ensure_line_contains(": 2007/07-07.grib1 should be removed from the index");
		s.ensure_line_contains(": 1 file should be removed from the index");
		s.ensure_all_lines_seen();

		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 2u);
		ensure_equals(c.count(COUNTED_TO_DEINDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": deleted from index 2007/07-07.grib1");
		s.ensure_line_contains(": 1 file removed from index");
		s.ensure_all_lines_seen();
	}
	ensure_maint_clean(2);

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		OutputChecker s;
		writer->check(s, true, true);
		s.ensure_all_lines_seen(); // Nothing should have happened

		ensure_maint_clean(2);
	}
}

// Test accuracy of maintenance scan, on dataset with one file deleted,
// performing check
template<> template<> void to::test<8>()
{
	clean_and_import();
	system("rm testds/2007/07-07.grib1");

	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 2u);
		ensure_equals(c.count(COUNTED_TO_DEINDEX), 1u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		OutputChecker s;

		writer->check(s, true, true);
		s.ensure_line_contains(": deindexed 2007/07-07.grib1");
		s.ensure_line_contains("1 file removed from index");
		s.ensure_all_lines_seen();
	}
	ensure_maint_clean(2);

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_all_lines_seen(); // Nothing should have happened

		ensure_maint_clean(2);
	}
}

// Test accuracy of maintenance scan, after deleting the index, with some
// spurious extra files in the dataset
template<> template<> void to::test<9>()
{
	clean_and_import();
	sys::fs::deleteIfExists("testds/index.sqlite");
	sys::fs::deleteIfExists("testds/MANIFEST");
	system("echo 'GRIB garbage 7777' > testds/2007/07.grib1.tmp");

	// See if the files to index are detected in the correct number
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_TO_INDEX), 3u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		OutputChecker s;

		writer->check(s, true, true);
		s.ensure_line_contains("rescanned 2007/07-07.grib1");
		s.ensure_line_contains("rescanned 2007/07-08.grib1");
		s.ensure_line_contains("rescanned 2007/10-09.grib1");
		s.ensure_line_contains("3 files rescanned");
		s.ensure_all_lines_seen();
	}
	ensure_maint_clean(3);

	// The spurious file should not have been touched
	ensure(sys::fs::exists("testds/2007/07.grib1.tmp"));

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_all_lines_seen(); // Nothing should have happened

		ensure_maint_clean(3);
	}
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
		reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
		ensure_equals(mdc.size(), 1u);
        wassert(actual(mdc[0].source).sourceblob_is("grib1", sys::fs::abspath("testds"), "foo/bar/test.grib1", 34960, 7218));
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

// Check that a freshly imported VM2 dataset is seen as packed
// (it didn't use to because newlines were seen as gaps)
template<> template<> void to::test<12>()
{
    clean_and_import(&cfg, "inbound/test.vm2");

    // Ensure the archive is clean
    {
        auto_ptr<WritableLocal> writer(makeLocalWriter());
        arki::tests::MaintenanceResults expected(true, 2);
        expected.by_type[COUNTED_OK] = 2;
        wassert(actual(writer.get()).maintenance(expected));

        wassert(!actual("testds/.archive").fileexists());
    }
}

// Test accuracy of maintenance scan, on a dataset with one file to both repack and delete
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
template<> template<> void to::test<14>()
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

}
