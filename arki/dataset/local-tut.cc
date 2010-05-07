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
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/dataset/local.h>
#include <arki/dataset/maintenance.h>
#include <arki/scan/any.h>
#include <arki/scan/grib.h>
#include <arki/utils/files.h>
#include <wibble/grcal/grcal.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
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

struct arki_dataset_local_shar : public dataset::maintenance::MaintFileVisitor {
	// Little dirty hack: implement MaintFileVisitor so we can conveniently
	// access State
	ConfigFile cfg;

	arki_dataset_local_shar()
	{
		cfg.setValue("path", "testds");
		cfg.setValue("name", "testds");
		cfg.setValue("type", "simple");
		cfg.setValue("step", "daily");
	}

	std::auto_ptr<Local> makeReader(const ConfigFile* wcfg = 0)
	{
		if (!wcfg) wcfg = &cfg;
		ReadonlyDataset* ds = ReadonlyDataset::create(*wcfg);
		Local* wl = dynamic_cast<Local*>(ds);
		ensure(wl);
		return auto_ptr<Local>(wl);
	}

	std::auto_ptr<WritableLocal> makeWriter(const ConfigFile* wcfg = 0)
	{
		if (!wcfg) wcfg = &cfg;
		WritableDataset* ds = WritableDataset::create(*wcfg);
		WritableLocal* wl = dynamic_cast<WritableLocal*>(ds);
		ensure(wl);
		return auto_ptr<WritableLocal>(wl);
	}

	// Clean the dataset directory
	void clean(const ConfigFile* wcfg = 0)
	{
		if (!wcfg) wcfg = &cfg;

		system(("rm -rf " + wcfg->value("path")).c_str());
		system(("mkdir " + wcfg->value("path")).c_str());
	}

	// Import a file
	void import(const ConfigFile* wcfg = 0, const std::string& testfile = "inbound/test.grib1")
	{
		if (!wcfg) wcfg = &cfg;

		{
			std::auto_ptr<WritableLocal> writer = makeWriter(wcfg);

			scan::Grib scanner;
			scanner.open(testfile);

			Metadata md;
			while (scanner.next(md))
			{
				WritableDataset::AcquireResult res = writer->acquire(md);
				ensure_equals(res, WritableDataset::ACQ_OK);
			}
		}

		utils::files::removeDontpackFlagfile(wcfg->value("path"));
	}

	// Recreate the dataset importing data into it
	void clean_and_import(const ConfigFile* wcfg = 0, const std::string& testfile = "inbound/test.grib1")
	{
		if (!wcfg) wcfg = &cfg;

		clean(wcfg);
		import(wcfg, testfile);
	}

	std::string days_since(int year, int month, int day)
	{
		// Data are from 07, 08, 10 2007
		int threshold[6] = { year, month, day, 0, 0, 0 };
		int now[6];
		grcal::date::now(now);
		long long int duration = grcal::date::duration(threshold, now);

		//cerr << str::fmt(duration/(3600*24)) + " days";
		return str::fmt(duration/(3600*24));
	}

	virtual void operator()(const std::string& file, State state) {}
};
TESTGRP(arki_dataset_local);

// Test accuracy of maintenance scan, on perfect dataset
template<> template<>
void to::test<1>()
{
	clean_and_import();

	// Ensure the archive appears clean
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());

		// Check that maintenance does not accidentally create an archive
		ensure(!sys::fs::access("testdir/.archive", F_OK));
	}

	// Ensure packing has nothing to report
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->repack(s, false);
		ensure_equals(s.str(), "");

		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->repack(s, true);
		ensure_not_contains(s.str(), "2007/");

		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->check(s, true, true);
		ensure_equals(s.str(), string()); // Nothing should have happened

		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}
}

template<> template<> void to::test<2>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<1>(); }

// Test accuracy of maintenance scan, on perfect dataset, with data to archive
template<> template<>
void to::test<3>()
{
	ConfigFile cfg = this->cfg;
	cfg.setValue("archive age", days_since(2007, 9, 1));

	clean_and_import(&cfg);

	// Check if files to archive are detected
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);

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
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->repack(s, true);
		ensure_contains(s.str(), ": archived 2007/07-07.grib1\n");
		ensure_contains(s.str(), ": archived 2007/07-08.grib1\n");
		ensure_not_contains(s.str(), ": archived 2007/10-10.grib1\n");
		ensure_contains(s.str(), ": 2 files archived");
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
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
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
		std::auto_ptr<Local> reader = makeReader(&cfg);

		metadata::Counter counter;
		reader->queryData(dataset::DataQuery(Matcher::parse(""), false), counter);
		ensure_equals(counter.count, 3u);
	}
}

template<> template<> void to::test<4>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<3>(); }


// Test moving into archive data that have been compressed
template<> template<>
void to::test<5>()
{
	ConfigFile cfg = this->cfg;
	cfg.setValue("archive age", days_since(2007, 9, 1));

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
		std::auto_ptr<Local> reader = makeReader(&cfg);

		metadata::Counter counter;
		reader->queryData(dataset::DataQuery(Matcher::parse(""), false), counter);
		ensure_equals(counter.count, 3u);
	}

	// Check if files to archive are detected
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);

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
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->repack(s, true);
		ensure_equals(s.str(),
				"testdir: archived 2007/07-07.grib1\n"
				"testdir: archived 2007/07-08.grib1\n"
				"testdir: archive cleaned up\n"
				"testdir: 2 files archived, 30448 bytes reclaimed on the index, 30448 total bytes freed.\n");
	}

	// Check that the files have been moved to the archive
	ensure(sys::fs::access("testdir/.archive/last/2007/07-07.grib1", F_OK));
	ensure(sys::fs::access("testdir/.archive/last/2007/07-07.grib1.metadata", F_OK));
	ensure(sys::fs::access("testdir/.archive/last/2007/07-07.grib1.summary", F_OK));
	ensure(sys::fs::access("testdir/.archive/last/2007/07-08.grib1", F_OK));
	ensure(sys::fs::access("testdir/.archive/last/2007/07-08.grib1.metadata", F_OK));
	ensure(sys::fs::access("testdir/.archive/last/2007/07-08.grib1.summary", F_OK));
	ensure(!sys::fs::access("testdir/2007/07-07.grib1", F_OK));
	ensure(!sys::fs::access("testdir/2007/07-08.grib1", F_OK));

	// Maintenance should now show a normal situation
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
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
		std::auto_ptr<Local> reader = makeReader(&cfg);

		metadata::Counter counter;
		reader->queryData(dataset::DataQuery(Matcher::parse(""), false), counter);
		ensure_equals(counter.count, 3u);
	}
}

template<> template<> void to::test<6>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<5>(); }


// Test accuracy of maintenance scan, on perfect dataset, with data to delete
template<> template<>
void to::test<7>()
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
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
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
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->repack(s, true);
		ensure_contains(s.str(), ": deleted 2007/07-07.grib1");
		ensure_contains(s.str(), ": deleted 2007/07-08.grib1");
		ensure_not_contains(s.str(), ": deleted 2007/10-10.grib1");
		ensure_contains(s.str(), ": 2 files deleted, 2 files removed from index");

		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->check(s, true, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}
}
template<> template<> void to::test<8>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<7>(); }

// Test accuracy of maintenance scan, on perfect dataset, with a truncated data file
template<> template<>
void to::test<9>()
{
	ConfigFile cfg = this->cfg;
	cfg.setValue("step", "yearly");
	clean_and_import(&cfg);

	// Truncate the last grib out of a file
	if (truncate("testds/20/2007.grib1", 42178) < 0)
		throw wibble::exception::System("truncating testds/20/2007.grib1");

	// See that the truncated file is detected
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
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
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;

		writer->repack(s, true);
		ensure_equals(s.str(),
				"testdir: 30448 bytes reclaimed on the index, 30448 total bytes freed.\n");

		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(OK), 0u);
		ensure_equals(c.count(TO_RESCAN), 1u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->check(s, true, true);
		ensure_equals(s.str(), 
			"testdir: rescanned 20/2007.grib1\n"
			"testdir: 1 file rescanned, 7736 bytes reclaimed cleaning the index.\n");

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
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->repack(s, true);
		ensure_equals(s.str(),
			"testdir: packed 20/2007.grib1 (0 saved)\n"
			"testdir: 1 file packed, 2576 bytes reclaimed on the index, 2576 total bytes freed.\n");

		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(OK), 1u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}
}
template<> template<> void to::test<10>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<9>(); }

// Test accuracy of maintenance scan, on a dataset with a corrupted data file
template<> template<>
void to::test<11>()
{
	ConfigFile cfg = this->cfg;
	cfg.setValue("step", "monthly");
	clean_and_import(&cfg);

	// Pack the dataset because 07.grib1 imported data out of order
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->repack(s, true);
		ensure_contains(s.str(), ": packed 2007/07.grib1");
		ensure_contains(s.str(), ": 1 file packed");
	}

	// Corrupt the first grib in the file
	system("dd if=/dev/zero of=testdir/2007/07.grib1 bs=1 count=2 conv=notrunc status=noxfer > /dev/null 2>&1");

	// A quick check has nothing to complain
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.fileStates.size(), 2u);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// A thorough check should find the corruption
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
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
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->check(s, true, false);
		ensure_contains(s.str(), ": rescanned 2007/07.grib1\n");
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
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);
		stringstream s;
		writer->repack(s, true);
		ensure_contains(s.str(), ": packed 2007/07.grib1\n");
		ensure_contains(s.str(), ": 1 file packed");
	}

	// Maintenance and pack are ok now
	{
		auto_ptr<WritableLocal> writer = makeWriter(&cfg);

		MaintenanceCollector c;
		writer->maintenance(c, false);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());

		stringstream s;
		writer->repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}
}
template<> template<> void to::test<12>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<11>(); }

// Test accuracy of maintenance scan, on a dataset with a data file larger than 2**31
template<> template<>
void to::test<13>()
{
	clean();

	// Simulate 2007/07-07.grib1 to be 6G already
	system("mkdir -p testdir/2007");
	system("touch testdir/2007/07-07.grib1");
	// Truncate the last grib out of a file
	if (truncate("testdir/2007/07-07.grib1", 6000000000LLU) < 0)
		throw wibble::exception::System("truncating testdir/2007/07-07.grib1");

	import();

	{
		auto_ptr<WritableLocal> writer = makeWriter();
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
template<> template<> void to::test<14>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<13>(); }


}

// vim:set ts=4 sw=4:
