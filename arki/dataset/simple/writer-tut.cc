/*
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/dataset/test-utils.h>
#include <arki/dataset/simple/writer.h>
#include <arki/dataset/simple/index.h>
#include <arki/dataset/simple/reader.h>
#include <arki/types/assigneddataset.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/scan/grib.h>
#include <arki/utils/metadata.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>

namespace arki {
namespace tests {

#define ensure_dataset_clean(x, y, z) arki::tests::impl_ensure_dataset_clean(wibble::tests::Location(__FILE__, __LINE__, #x ", " #y), (x), (y), (z))
template<typename DS>
void impl_ensure_dataset_clean(const wibble::tests::Location& loc, DS& ds, size_t filecount, size_t resultcount)
{
	using namespace std;
	using namespace arki;
	using namespace arki::utils;

	MaintenanceCollector c;
	ds.maintenance(c);
	inner_ensure_equals(c.fileStates.size(), filecount);
	inner_ensure_equals(c.count(dataset::maintenance::MaintFileVisitor::ARC_OK), filecount);
	inner_ensure_equals(c.remaining(), string());
	inner_ensure(c.isClean());

	metadata::Collector mdc;
	ds.queryData(dataset::DataQuery(Matcher(), false), mdc);
	inner_ensure_equals(mdc.size(), resultcount);
}

}
}

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::dataset::simple;
using namespace arki::types;
using namespace arki::utils;

struct ForceSqlite
{
	bool old;

	ForceSqlite(bool val = true) : old(dataset::simple::Manifest::get_force_sqlite())
	{
		dataset::simple::Manifest::set_force_sqlite(val);
	}
	~ForceSqlite()
	{
		dataset::simple::Manifest::set_force_sqlite(old);
	}
};

static inline UItem<types::AssignedDataset> getDataset(const Metadata& md)
{
	return md.get(types::TYPE_ASSIGNEDDATASET).upcast<types::AssignedDataset>();
}

struct arki_dataset_simple_writer_shar : public dataset::maintenance::MaintFileVisitor {
	// Little dirty hack: implement MaintFileVisitor so we can conveniently
	// access State

	ConfigFile cfg;

	arki_dataset_simple_writer_shar()
	{
		system("rm -rf testds");
		system("mkdir testds");
		system("mkdir testds/.archive");
		system("mkdir testds/.archive/last");

		cfg.setValue("path", "testds");
		cfg.setValue("name", "testds");
		cfg.setValue("type", "simple");
		cfg.setValue("step", "daily");
	}

	virtual void operator()(const std::string& file, State state) {}

	std::string idxfname() const
	{
		return dataset::simple::Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
	}
};
TESTGRP(arki_dataset_simple_writer);

// Test acquiring data
template<> template<>
void to::test<1>()
{
	// Clean the dataset
	system("rm -rf testds");
	system("mkdir testds");

	Metadata md;
	scan::Grib scanner;
	scanner.open("inbound/test.grib1");

	dataset::simple::Writer writer(cfg);
	ensure(scanner.next(md));

	// Import once in the empty dataset
	WritableDataset::AcquireResult res = writer.acquire(md);
	ensure_equals(res, WritableDataset::ACQ_OK);
	#if 0
	for (vector<Note>::const_iterator i = md.notes.begin();
			i != md.notes.end(); ++i)
		cerr << *i << endl;
	#endif
	UItem<types::AssignedDataset> ds = getDataset(md);
	ensure_equals(ds->name, "testds");
	ensure_equals(ds->id, "");

	Item<types::source::Blob> source = md.source.upcast<types::source::Blob>();
	ensure_equals(source->filename, "07-08.grib1");
	ensure_equals(source->offset, 0u);
	ensure_equals(source->size, 7218u);


	// Import again works fine
	res = writer.acquire(md);
	ensure_equals(res, WritableDataset::ACQ_OK);
	ds = getDataset(md);
	ensure_equals(ds->name, "testds");
	ensure_equals(ds->id, "");

	source = md.source.upcast<types::source::Blob>();
	ensure_equals(source->filename, "07-08.grib1");
	ensure_equals(source->offset, 7218u);
	ensure_equals(source->size, 7218u);

	// Flush the changes and check that everything is allright
	writer.flush();
	ensure(wibble::sys::fs::access("testds/2007/07-08.grib1", F_OK));
	ensure(wibble::sys::fs::access("testds/2007/07-08.grib1.metadata", F_OK));
	ensure(wibble::sys::fs::access("testds/2007/07-08.grib1.summary", F_OK));
	ensure(wibble::sys::fs::access("testds/" + idxfname(), F_OK));
	ensure(files::timestamp("testds/2007/07-08.grib1") <= files::timestamp("testds/2007/07-08.grib1.metadata"));
	ensure(files::timestamp("testds/2007/07-08.grib1.metadata") <= files::timestamp("testds/2007/07-08.grib1.summary"));
	ensure(files::timestamp("testds/2007/07-08.grib1.summary") <= files::timestamp("testds/" + idxfname()));

	
	{
		dataset::simple::Reader reader("testds");
		ensure_dataset_clean(reader, 1, 2);
	}
}

#if 0
// Test maintenance scan on non-indexed files
template<> template<>
void to::test<2>()
{
	MaintenanceCollector c;
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/last/");
	}

	// Query now is ok
	{
		Archive arc("testds/.archive/last");
		arc.openRO();
		metadata::Collector mdc;
		arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
		ensure_equals(mdc.size(), 0u);

		// Maintenance should show one file to index
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(), 
				"testds: rescanned in archive last/test.grib1\n"
				"testds: archive cleaned up\n"
				"testds: database cleaned up\n"
				"testds: rebuild summary cache\n"
				"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	ensure_archive_clean("testds/.archive/last", 1, 3);
}

// Test maintenance scan on missing metadata
template<> template<>
void to::test<3>()
{
	MaintenanceCollector c;
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/last/");
		arc.acquire("test.grib1");
		sys::fs::deleteIfExists("testds/.archive/last/test.grib1.metadata");
		sys::fs::deleteIfExists("testds/.archive/last/test.grib1.summary");
		ensure(sys::fs::access("testds/.archive/last/test.grib1", F_OK));
		ensure(!sys::fs::access("testds/.archive/last/test.grib1.metadata", F_OK));
		ensure(!sys::fs::access("testds/.archive/last/test.grib1.summary", F_OK));
		ensure(sys::fs::access("testds/.archive/last/" + idxfname(), F_OK));
	}

	// Query now is ok
	{
		Archive arc("testds/.archive/last");
		arc.openRO();
		metadata::Collector mdc;
		arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
		ensure_equals(mdc.size(), 3u);

		// Maintenance should show one file to rescan
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Fix the database
	{
		Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	ensure_archive_clean("testds/.archive/last", 1, 3);
}

// Test maintenance scan on missing summary
template<> template<>
void to::test<4>()
{
	MaintenanceCollector c;
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/last/");
		arc.acquire("test.grib1");
		sys::fs::deleteIfExists("testds/.archive/last/test.grib1.summary");
		ensure(sys::fs::access("testds/.archive/last/test.grib1", F_OK));
		ensure(sys::fs::access("testds/.archive/last/test.grib1.metadata", F_OK));
		ensure(!sys::fs::access("testds/.archive/last/test.grib1.summary", F_OK));
		ensure(sys::fs::access("testds/.archive/last/" + idxfname(), F_OK));
	}

	// Query now is ok
	{
		Archive arc("testds/.archive/last");
		arc.openRO();
		metadata::Collector mdc;
		arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
		ensure_equals(mdc.size(), 3u);

		// Maintenance should show one file to rescan
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	ensure_archive_clean("testds/.archive/last", 1, 3);
}

// Test maintenance scan on missing metadata
template<> template<>
void to::test<5>()
{
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/last/1.grib1");
		system("cp inbound/test.grib1 testds/.archive/last/2.grib1");
		system("cp inbound/test.grib1 testds/.archive/last/3.grib1");
		arc.acquire("1.grib1");
		arc.acquire("2.grib1");
		arc.acquire("3.grib1");

		sys::fs::deleteIfExists("testds/.archive/last/2.grib1.metadata");
		ensure(sys::fs::access("testds/.archive/last/2.grib1", F_OK));
		ensure(!sys::fs::access("testds/.archive/last/2.grib1.metadata", F_OK));
		ensure(sys::fs::access("testds/.archive/last/2.grib1.summary", F_OK));
		ensure(sys::fs::access("testds/.archive/last/" + idxfname(), F_OK));
	}

	// Query now is ok
	{
		metadata::Collector mdc;
		Archive arc("testds/.archive/last");
		arc.openRO();
		arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
		ensure_equals(mdc.size(), 9u);
	}

	// Maintenance should show one file to rescan
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		MaintenanceCollector c;
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		Writer writer(cfg);

		MaintenanceCollector c;
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/2.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(ARC_OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	ensure_archive_clean("testds/.archive/last", 3, 9);
}

// Test maintenance scan on compressed archives
template<> template<>
void to::test<6>()
{
	MaintenanceCollector c;
	{
		// Import a file
		Archive arc("testds/.archive/last");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/last/");
		arc.acquire("test.grib1");

		// Compress it
		metadata::Collector mdc;
		Metadata::readFile("testds/.archive/last/test.grib1.metadata", mdc);
		ensure_equals(mdc.size(), 3u);
		mdc.compressDataFile(1024, "metadata file testds/.archive/last/test.grib1.metadata");
		sys::fs::deleteIfExists("testds/.archive/last/test.grib1");

		ensure(!sys::fs::access("testds/.archive/last/test.grib1", F_OK));
		ensure(sys::fs::access("testds/.archive/last/test.grib1.gz", F_OK));
		ensure(sys::fs::access("testds/.archive/last/test.grib1.gz.idx", F_OK));
		ensure(sys::fs::access("testds/.archive/last/test.grib1.metadata", F_OK));
		ensure(sys::fs::access("testds/.archive/last/test.grib1.summary", F_OK));
		ensure(sys::fs::access("testds/.archive/last/" + idxfname(), F_OK));
	}

	// Everything is still ok
	ensure_archive_clean("testds/.archive/last", 1, 3);

	// Try removing summary and metadata
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.metadata");
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.summary");

	// Cannot query anymore
	{
		Archive arc("testds/.archive/last");
		arc.openRO();
		metadata::Collector mdc;
		try {
			arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
			ensure(false);
		} catch (std::exception& e) {
			ensure(str::startsWith(e.what(), "file needs to be manually decompressed before scanning."));
		}

		// Maintenance should show one file to rescan
		c.clear();
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	ensure_archive_clean("testds/.archive/last", 1, 3);
}

// Test handling of empty archive dirs (such as last with everything moved away)
template<> template<>
void to::test<7>()
{
	// Import a file in a secondary archive
	{
		system("mkdir testds/.archive/foo");
		Archive arc("testds/.archive/foo");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/foo/");
		arc.acquire("test.grib1");
	}

	// Everything should be fine now
	Archives arc("testds/.archive");
	ensure_dataset_clean(arc, 1, 3);
}


// Retest with sqlite
template<> template<> void to::test<8>() { ForceSqlite fs; test<1>(); }
template<> template<> void to::test<9>() { ForceSqlite fs; test<2>(); }
template<> template<> void to::test<10>() { ForceSqlite fs; test<3>(); }
template<> template<> void to::test<11>() { ForceSqlite fs; test<4>(); }
template<> template<> void to::test<12>() { ForceSqlite fs; test<5>(); }
template<> template<> void to::test<13>() { ForceSqlite fs; test<6>(); }
template<> template<> void to::test<14>() { ForceSqlite fs; test<7>(); }

#endif

}
