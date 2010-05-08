/*
 * Copyright (C) 2007,2008  Enrico Zini <enrico@enricozini.org>
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

#include <arki/dataset/test-utils.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/dataset/local.h>
#include <arki/dataset/maintenance.h>
#include <arki/scan/any.h>
#include <arki/utils/files.h>
#include <wibble/grcal/grcal.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::dataset;
using namespace wibble;

namespace {
struct TempConfig
{
	ConfigFile& cfg;
	ConfigFile backup;

	TempConfig(ConfigFile& cfg)
		: cfg(cfg), backup(cfg)
	{
	}

	TempConfig(ConfigFile& cfg, const std::string& key, const std::string& val)
		: cfg(cfg), backup(cfg)
	{
		override(key, val);
	}

	~TempConfig()
	{
		cfg = backup;
	}

	void override(const std::string& key, const std::string& val)
	{
		cfg.setValue(key, val);
	}
};
}

struct arki_dataset_local_shar : public DatasetTest {
	arki_dataset_local_shar()
	{
		cfg.setValue("path", "testds");
		cfg.setValue("name", "testds");
		cfg.setValue("type", "simple");
		cfg.setValue("step", "daily");
	}

};
TESTGRP(arki_dataset_local);

// Test accuracy of maintenance scan, on perfect dataset
template<> template<>
void to::test<1>()
{
	clean_and_import();

	// Ensure the archive appears clean
	{
		ensure_maint_clean(3);

		// Check that maintenance does not accidentally create an archive
		ensure(!sys::fs::access("testds/.archive", F_OK));
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
template<> template<>
void to::test<2>()
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
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.count(TO_ARCHIVE), 2u);
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
	ensure(sys::fs::access("testds/.archive/last/2007/07-07.grib1", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-07.grib1.metadata", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-07.grib1.summary", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-08.grib1", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-08.grib1.metadata", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-08.grib1.summary", F_OK));
	ensure(!sys::fs::access("testds/2007/07-07.grib1", F_OK));
	ensure(!sys::fs::access("testds/2007/07-08.grib1", F_OK));

	// Maintenance should now show a normal situation
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
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
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
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

// Test moving into archive data that have been compressed
template<> template<>
void to::test<3>()
{
	ConfigFile cfg = this->cfg;
	cfg.setValue("archive age", str::fmt(days_since(2007, 9, 1)));

	// Import and compress all the files
	clean_and_import(&cfg);
	scan::compress("testds/2007/07-07.grib1");
	scan::compress("testds/2007/07-08.grib1");
	scan::compress("testds/2007/10-09.grib1");
	sys::fs::deleteIfExists("testds/2007/07-07.grib1");
	sys::fs::deleteIfExists("testds/2007/07-08.grib1");
	sys::fs::deleteIfExists("testds/2007/10-09.grib1");

	// Test that querying returns all items
	{
		std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));

		metadata::Counter counter;
		reader->queryData(dataset::DataQuery(Matcher::parse(""), false), counter);
		ensure_equals(counter.count, 3u);
	}

	// Check if files to archive are detected
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));

		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.count(TO_ARCHIVE), 2u);
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
	ensure(!sys::fs::access("testds/.archive/last/2007/07-07.grib1", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-07.grib1.gz", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-07.grib1.gz.idx", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-07.grib1.metadata", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-07.grib1.summary", F_OK));
	ensure(!sys::fs::access("testds/.archive/last/2007/07-08.grib1", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-08.grib1.gz", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-08.grib1.gz.idx", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-08.grib1.metadata", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2007/07-08.grib1.summary", F_OK));
	ensure(!sys::fs::access("testds/2007/07-07.grib1", F_OK));
	ensure(!sys::fs::access("testds/2007/07-07.grib1.gz", F_OK));
	ensure(!sys::fs::access("testds/2007/07-07.grib1.gz.idx", F_OK));
	ensure(!sys::fs::access("testds/2007/07-07.grib1.metadata", F_OK));
	ensure(!sys::fs::access("testds/2007/07-07.grib1.summary", F_OK));
	ensure(!sys::fs::access("testds/2007/07-08.grib1", F_OK));
	ensure(!sys::fs::access("testds/2007/07-08.grib1.gz", F_OK));
	ensure(!sys::fs::access("testds/2007/07-08.grib1.gz.idx", F_OK));
	ensure(!sys::fs::access("testds/2007/07-08.grib1.metadata", F_OK));
	ensure(!sys::fs::access("testds/2007/07-08.grib1.summary", F_OK));

	// Maintenance should now show a normal situation
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
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
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
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
template<> template<>
void to::test<4>()
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
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.count(TO_DELETE), 2u);
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
template<> template<>
void to::test<5>()
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
		ensure_equals(c.count(OK), 0u);
		ensure_equals(c.count(TO_RESCAN), 1u);
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
		ensure_equals(c.count(OK), 0u);
		ensure_equals(c.count(TO_RESCAN), 1u);
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
		ensure_equals(c.count(OK), 0u);
		ensure_equals(c.count(TO_PACK), 1u);
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

		ensure_maint_clean(1);
	}
}

// Test accuracy of maintenance scan, on a dataset with a corrupted data file
template<> template<>
void to::test<6>()
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
	system("dd if=/dev/zero of=testds/2007/07.grib1 bs=1 count=2 conv=notrunc status=noxfer > /dev/null 2>&1");

	// A quick check has nothing to complain
	ensure_maint_clean(2);

	// A thorough check should find the corruption
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		MaintenanceCollector c;
		writer->maintenance(c, false);
		ensure_equals(c.fileStates.size(), 2u);
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.count(TO_RESCAN), 1u);
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
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.count(TO_PACK), 1u);
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
template<> template<>
void to::test<7>()
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
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.count(TO_PACK), 1u);
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
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());
cerr  << "ZAERBA 4" << endl;

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(), 
		"testdir: packed 2007/07.grib1 (34960 saved)\n"
		"testdir: 1 file packed, 2576 bytes reclaimed on the index, 37536 total bytes freed.\n");
	c.clear();

	// Maintenance and pack are ok now
	writer.maintenance(c, false);
	ensure_equals(c.count(OK), 2u);
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
template<> template<>
void to::test<8>()
{
	clean_and_import();
	system("rm testds/2007/07-07.grib1");

	// Initial check finds the deleted file
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.count(DELETED), 1u);
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
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.count(DELETED), 1u);
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
template<> template<>
void to::test<9>()
{
	clean_and_import();
	system("rm testds/2007/07-07.grib1");

	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.count(DELETED), 1u);
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
template<> template<>
void to::test<10>()
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
		ensure_equals(c.count(TO_INDEX), 3u);
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
	ensure(sys::fs::access("testds/2007/07.grib1.tmp", F_OK));

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
template<> template<>
void to::test<11>()
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
		ensure_equals(c.count(TO_INDEX), 1u);
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
		ensure_equals(c.count(TO_PACK), 1u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	ensure(sys::fs::access("testds/foo/bar/test.grib1.tmp", F_OK));
	ensure_equals(utils::files::size("testds/foo/bar/test.grib1"), 44412);

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter());
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": packed foo/bar/test.grib1");
		s.ensure_line_contains(": 1 file packed");
	}
	ensure_maint_clean(1);

	ensure_equals(utils::files::size("testds/foo/bar/test.grib1"), 44412);

	// Test querying
	{
		std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));

		metadata::Collection mdc;
		reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
		ensure_equals(mdc.size(), 1u);
		UItem<types::source::Blob> blob = mdc[0].source.upcast<types::source::Blob>();
		ensure_equals(blob->format, "grib1"); 
		ensure_equals(blob->filename, sys::fs::abspath("testds/foo/bar/test.grib1"));
		ensure_equals(blob->offset, 34960u);
	}
}

// Test querying the datasets
template<> template<>
void to::test<12>()
{
	using namespace arki::types;

	clean_and_import();
	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	metadata::Collection mdc;
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::BLOB);
	ensure_equals(source->format, "grib1");
	UItem<source::Blob> blob = source.upcast<source::Blob>();
	ensure_equals(blob->filename, sys::fs::abspath("testds/2007/07-08.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 7218u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), false), mdc);
	ensure_equals(mdc.size(), 1u);
}

// Test querying with data only
template<> template<>
void to::test<13>()
{
	clean_and_import();
	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	std::stringstream os;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse("origin:GRIB1,200"));
	reader->queryBytes(bq, os);

	ensure_equals(os.str().substr(0, 4), "GRIB");
}

// Test querying with inline data
template<> template<>
void to::test<14>()
{
	using namespace arki::types;

	clean_and_import();
	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	metadata::Collection mdc;
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::INLINE);
	ensure_equals(source->format, "grib1");
	UItem<source::Inline> isource = source.upcast<source::Inline>();
	ensure_equals(isource->size, 7218u);

	wibble::sys::Buffer buf = mdc[0].getData();
	ensure_equals(buf.size(), isource->size);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), true), mdc);
	ensure_equals(mdc.size(), 1u);
}

// Test querying with archived data
template<> template<>
void to::test<15>()
{
	using namespace arki::types;

	ConfigFile cfg = this->cfg;
	cfg.setValue("archive age", str::fmt(days_since(2007, 9, 1)));
	clean_and_import(&cfg);
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": archived 2007/07-07.grib1");
		s.ensure_line_contains(": archived 2007/07-08.grib1");
		s.ignore_line_containing("archive cleaned up");
		s.ensure_line_contains(": 2 files archived");
		s.ensure_all_lines_seen();
	}

	std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));
	metadata::Collection mdc;

	reader->queryData(dataset::DataQuery(Matcher::parse(""), true), mdc);
	ensure_equals(mdc.size(), 3u);
	mdc.clear();

	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::INLINE);
	ensure_equals(source->format, "grib1");
	UItem<source::Inline> isource = source.upcast<source::Inline>();
	ensure_equals(isource->size, 7218u);

	wibble::sys::Buffer buf = mdc[0].getData();
	ensure_equals(buf.size(), isource->size);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("reftime:=2007-07-08"), true), mdc);
	ensure_equals(mdc.size(), 1u);
	isource = mdc[0].source.upcast<source::Inline>();
	ensure_equals(isource->size, 7218u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	// Query bytes
	stringstream out;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse(""));
	reader->queryBytes(bq, out);
	ensure_equals(out.str().size(), 44412u);

	out.str(string());
	bq.matcher = Matcher::parse("origin:GRIB1,200");
	reader->queryBytes(bq, out);
	ensure_equals(out.str().size(), 7218u);

	out.str(string());
	bq.matcher = Matcher::parse("reftime:=2007-07-08");
	reader->queryBytes(bq, out);
	ensure_equals(out.str().size(), 7218u);

	/* TODO
		case BQ_POSTPROCESS: {
		case BQ_REP_METADATA: {
		case BQ_REP_SUMMARY: {
	virtual void querySummary(const Matcher& matcher, Summary& summary);
	*/

	// Query summary
	Summary s;
	reader->querySummary(Matcher::parse(""), s);
	ensure_equals(s.count(), 3u);
	ensure_equals(s.size(), 44412u);

	s.clear();
	reader->querySummary(Matcher::parse("origin:GRIB1,200"), s);
	ensure_equals(s.count(), 1u);
	ensure_equals(s.size(), 7218u);

	s.clear();
	reader->querySummary(Matcher::parse("reftime:=2007-07-08"), s);
	ensure_equals(s.count(), 1u);
	ensure_equals(s.size(), 7218u);
}

// Tolerate empty dirs
template<> template<>
void to::test<16>()
{
	// Start with an empty dir
	system("rm -rf testds");
	system("mkdir testds");

	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	metadata::Collection mdc;
	reader->queryData(dataset::DataQuery(Matcher::parse(""), false), mdc);
	ensure(mdc.empty());

	Summary s;
	reader->querySummary(Matcher::parse(""), s);
	ensure_equals(s.count(), 0u);

	std::stringstream os;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse(""));
	reader->queryBytes(bq, os);
	ensure(os.str().empty());
}



template<> template<> void to::test<17>() { ForceSqlite fs; to::test<1>(); }
template<> template<> void to::test<18>() { ForceSqlite fs; to::test<2>(); }
template<> template<> void to::test<19>() { ForceSqlite fs; to::test<3>(); }
template<> template<> void to::test<20>() { ForceSqlite fs; to::test<4>(); }
template<> template<> void to::test<21>() { ForceSqlite fs; to::test<5>(); }
template<> template<> void to::test<22>() { ForceSqlite fs; to::test<6>(); }
template<> template<> void to::test<23>() { ForceSqlite fs; to::test<7>(); }
template<> template<> void to::test<24>() { ForceSqlite fs; to::test<8>(); }
template<> template<> void to::test<25>() { ForceSqlite fs; to::test<9>(); }
template<> template<> void to::test<26>() { ForceSqlite fs; to::test<10>(); }
template<> template<> void to::test<27>() { ForceSqlite fs; to::test<11>(); }
template<> template<> void to::test<28>() { ForceSqlite fs; to::test<12>(); }
template<> template<> void to::test<29>() { ForceSqlite fs; to::test<13>(); }
template<> template<> void to::test<30>() { ForceSqlite fs; to::test<14>(); }
template<> template<> void to::test<31>() { ForceSqlite fs; to::test<15>(); }
template<> template<> void to::test<32>() { ForceSqlite fs; to::test<16>(); }

template<> template<> void to::test<33>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<1>(); }
template<> template<> void to::test<34>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<2>(); }
template<> template<> void to::test<35>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<3>(); }
template<> template<> void to::test<36>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<4>(); }
template<> template<> void to::test<37>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<5>(); }
template<> template<> void to::test<38>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<6>(); }
template<> template<> void to::test<39>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<7>(); }
template<> template<> void to::test<40>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<8>(); }
template<> template<> void to::test<41>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<9>(); }
template<> template<> void to::test<42>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<10>(); }
template<> template<> void to::test<43>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<11>(); }
template<> template<> void to::test<44>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<12>(); }
template<> template<> void to::test<45>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<13>(); }
template<> template<> void to::test<46>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<14>(); }
template<> template<> void to::test<47>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<15>(); }
template<> template<> void to::test<48>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<16>(); }



}

// vim:set ts=4 sw=4:
