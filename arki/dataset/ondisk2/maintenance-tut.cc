/*
 * Copyright (C) 2007--2010  Enrico Zini <enrico@enricozini.org>
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

#include <arki/dataset/ondisk2/test-utils.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/ondisk2/reader.h>
#include <arki/metadata.h>
#include <arki/configfile.h>
#include <arki/scan/grib.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/metadata.h>
#include <wibble/sys/fs.h>
#include <wibble/grcal/grcal.h>

#include <sstream>
#include <iostream>
#include <algorithm>

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset::ondisk2;
using namespace arki::dataset::ondisk2::writer;
using namespace arki::utils::files;

namespace tut {

struct arki_dataset_ondisk2_maintenance_shar : public dataset::maintenance::MaintFileVisitor {
	// Little dirty hack: implement MaintFileVisitor so we can conveniently
	// access State

	ConfigFile cfg;

	arki_dataset_ondisk2_maintenance_shar()
	{
		system("rm -rf testdir");

		cfg.setValue("path", "testdir");
		cfg.setValue("name", "testdir");
		cfg.setValue("type", "ondisk2");
		cfg.setValue("step", "daily");
		cfg.setValue("unique", "origin, reftime");
	}

	void acquireSamples()
	{
		Metadata md;
		scan::Grib scanner;
		Writer writer(cfg);
		scanner.open("inbound/test.grib1");
		size_t count = 0;
		for ( ; scanner.next(md); ++count)
			ensure_equals(writer.acquire(md), WritableDataset::ACQ_OK);
		ensure_equals(count, 3u);
		writer.flush();
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
TESTGRP(arki_dataset_ondisk2_maintenance);

// Test accuracy of maintenance scan, on perfect dataset
template<> template<>
void to::test<1>()
{
	acquireSamples();
	removeDontpackFlagfile("testdir");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());

	// Check that maintenance does not accidentally create an archive
	ensure(!sys::fs::access("testdir/.archive", F_OK));

	{
		// Test packing has nothing to report
		stringstream s;
		writer.repack(s, false);
		ensure_equals(s.str(), "");

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}

	{
		// Perform packing and check that things are still ok afterwards
		stringstream s;
		writer.repack(s, true);
		ensure_equals(s.str(),
			"testdir: 30448 bytes reclaimed on the index, 30448 total bytes freed.\n");

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		stringstream s;
		writer.check(s, true, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
		c.clear();
		writer.maintenance(c);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on dataset with one file deleted,
// performing repack
template<> template<>
void to::test<2>()
{
	acquireSamples();
	removeDontpackFlagfile("testdir");

	system("rm testdir/2007/07-07.grib1");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(DELETED), 1u);
	ensure_equals(c.remaining(), string());
	ensure(not c.isClean());

	{
		// Test packing has something to report
		stringstream s;
		writer.repack(s, false);
		ensure_equals(s.str(),
			"testdir: 2007/07-07.grib1 should be removed from the index\n"
			"testdir: 1 file should be removed from the index.\n");

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.count(DELETED), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		// Perform packing and check that things are still ok afterwards
		stringstream s;
		writer.repack(s, true);
		ensure_equals(s.str(),
			"testdir: deleted from index 2007/07-07.grib1\n"
			"testdir: 1 file removed from index, 30448 bytes reclaimed on the index, 30448 total bytes freed.\n");
		c.clear();

		writer.maintenance(c);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		stringstream s;
		writer.check(s, true, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
		c.clear();
		writer.maintenance(c);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on dataset with one file deleted,
// performing check
template<> template<>
void to::test<3>()
{
	acquireSamples();

	system("rm testdir/2007/07-07.grib1");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(DELETED), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: deindexed 2007/07-07.grib1\n"
		"testdir: 1 file removed from index, 30448 bytes reclaimed cleaning the index.\n");

	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on dataset with one file to reclaim,
// performing repack
template<> template<>
void to::test<4>()
{
	acquireSamples();
	removeDontpackFlagfile("testdir");
	{
		// Remove one element
		arki::dataset::ondisk2::Writer writer(cfg);
		writer.remove("2");
		writer.flush();
	}

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	{
		// Test packing has something to report
		stringstream s;
		writer.repack(s, false);
		ensure_equals(s.str(),
			"testdir: 2007/07-07.grib1 should be deleted\n"
			"testdir: 1 file should be deleted.\n");

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.count(TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	// Perform packing and check that things are still ok afterwards
	{
		stringstream s;
		writer.repack(s, true);
		ensure_equals(s.str(),
			"testdir: deleted 2007/07-07.grib1 (34960 freed)\n"
			"testdir: 1 file deleted, 30448 bytes reclaimed on the index, 65408 total bytes freed.\n");

		c.clear();

		writer.maintenance(c);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		stringstream s;
		writer.check(s, true, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
		c.clear();
		writer.maintenance(c);
		ensure_equals(c.count(OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on dataset with one file to reclaim,
// performing check
template<> template<>
void to::test<5>()
{
	acquireSamples();
	{
		// Remove one element
		arki::dataset::ondisk2::Writer writer(cfg);
		writer.remove("2");
		writer.flush();
	}

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-07.grib1\n"
		"testdir: 1 file rescanned, 30448 bytes reclaimed cleaning the index.\n");
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on dataset with one file to pack,
// performing repack
template<> template<>
void to::test<6>()
{
	cfg.setValue("step", "monthly");
	acquireSamples();
	removeDontpackFlagfile("testdir");
	{
		// Remove one element
		arki::dataset::ondisk2::Writer writer(cfg);
		writer.remove("2");
		writer.flush();
	}

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 2u);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	// Perform packing and check that things are still ok afterwards
	stringstream s;
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed 2007/07.grib1 (34960 saved)\n"
		"testdir: 1 file packed, 30448 bytes reclaimed on the index, 65408 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	s.str(std::string());
	writer.check(s, true, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on dataset with one file to pack,
// performing check
template<> template<>
void to::test<7>()
{
	cfg.setValue("step", "monthly");
	acquireSamples();
	{
		// Remove one element
		arki::dataset::ondisk2::Writer writer(cfg);
		writer.remove("2");
		writer.flush();
	}

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 2u);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	// No packing occurs here: check does not mangle data files
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: 30448 bytes reclaimed cleaning the index.\n");

	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed 2007/07.grib1 (34960 saved)\n"
		"testdir: 1 file packed, 2576 bytes reclaimed on the index, 37536 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, after deleting the index
template<> template<>
void to::test<8>()
{
	acquireSamples();
	system("rm testdir/index.sqlite");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 0u);
	ensure_equals(c.count(TO_INDEX), 3u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-07.grib1\n"
		"testdir: rescanned 2007/07-08.grib1\n"
		"testdir: rescanned 2007/10-09.grib1\n"
		"testdir: 3 files rescanned, 30448 bytes reclaimed cleaning the index.\n");
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, after deleting the index, with some
// spurious extra files in the dataset
template<> template<>
void to::test<9>()
{
	acquireSamples();
	system("rm testdir/index.sqlite");
	system("echo 'GRIB garbage 7777' > testdir/2007/07.grib1.tmp");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(TO_INDEX), 3u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-07.grib1\n"
		"testdir: rescanned 2007/07-08.grib1\n"
		"testdir: rescanned 2007/10-09.grib1\n"
		"testdir: 3 files rescanned, 30448 bytes reclaimed cleaning the index.\n");
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	ensure(sys::fs::access("testdir/2007/07.grib1.tmp", F_OK));

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test recreating a dataset from random datafiles
template<> template<>
void to::test<10>()
{
	system("mkdir testdir");
	system("mkdir testdir/foo");
	system("mkdir testdir/foo/bar");
	system("cp inbound/test.grib1 testdir/foo/bar/");
	system("echo 'GRIB garbage 7777' > testdir/foo/bar/test.grib1.tmp");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned foo/bar/test.grib1\n"
		"testdir: 1 file rescanned, 30448 bytes reclaimed cleaning the index.\n");
	c.clear();
	writer.maintenance(c);
	// A repack is still needed because the data is not sorted by reftime
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	ensure(sys::fs::access("testdir/foo/bar/test.grib1.tmp", F_OK));
	ensure_equals(utils::files::size("testdir/foo/bar/test.grib1"), 44412);

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed foo/bar/test.grib1 (0 saved)\n"
		"testdir: 1 file packed, 2576 bytes reclaimed on the index, 2576 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	ensure_equals(utils::files::size("testdir/foo/bar/test.grib1"), 44412);

	// Test querying
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	utils::metadata::Collector mdc;
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
	ensure_equals(blob->offset, 34960u);

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test recreating a dataset from just a datafile with duplicate data and a rebuild flagfile
template<> template<>
void to::test<11>()
{
	system("mkdir testdir");
	system("mkdir testdir/foo");
	system("mkdir testdir/foo/bar");
	system("cat inbound/test.grib1 inbound/test.grib1 > testdir/foo/bar/test.grib1");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned foo/bar/test.grib1\n"
		"testdir: 1 file rescanned, 30448 bytes reclaimed cleaning the index.\n");

	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	ensure_equals(utils::files::size("testdir/foo/bar/test.grib1"), 44412*2);

	{
		// Test querying: reindexing should have chosen the last version of
		// duplicate items
		Reader reader(cfg);
		ensure(reader.hasWorkingIndex());
		utils::metadata::Collector mdc;
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
		ensure_equals(mdc.size(), 1u);
		UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
		ensure_equals(blob->format, "grib1"); 
		ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
		ensure_equals(blob->offset, 51630u);
		ensure_equals(blob->size, 34960u);

		mdc.clear();
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
		ensure_equals(mdc.size(), 1u);
		blob = mdc[0].source.upcast<source::Blob>();
		ensure_equals(blob->format, "grib1"); 
		ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
		ensure_equals(blob->offset,  44412u);
		ensure_equals(blob->size, 7218u);
	}

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed foo/bar/test.grib1 (44412 saved)\n"
		"testdir: 1 file packed, 2576 bytes reclaimed on the index, 46988 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	ensure_equals(utils::files::size("testdir/foo/bar/test.grib1"), 44412);

	// Test querying, and see that things have moved to the beginning
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	utils::metadata::Collector mdc;
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 34960u);

	// Query the second element and check that it starts after the first one
	// (there used to be a bug where the rebuild would use the offsets of
	// the metadata instead of the data)
	mdc.clear();
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
	ensure_equals(blob->offset, 34960u);
	ensure_equals(blob->size, 7218u);

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles, and duplicate items inside
template<> template<>
void to::test<12>()
{
	acquireSamples();
	system("cat inbound/test.grib1 >> testdir/2007/07-08.grib1");
	{
		WIndex index(cfg);
		index.open();
		index.reset("2007/07-08.grib1");
	}

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards

	// By catting test.grib1 into 07-08.grib1, we create 2 metadata that do
	// not fit in that file (1 does).
	// Because they are duplicates of metadata in other files, one cannot
	// use the order of the data in the file to determine which one is the
	// newest. The situation cannot be fixed automatically because it is
	// impossible to determine which of the two duplicates should be thrown
	// away; therefore, we can only interrupt the maintenance and raise an
	// exception calling for manual fixing.
	try {
		writer.check(s, true, true);
		ensure(false);
	} catch (wibble::exception::Consistency& ce) {
		ensure(true);
	} catch (...) {
		ensure(false);
	}
}

// Test accuracy of maintenance scan, on perfect dataset, with data to archive
template<> template<>
void to::test<13>()
{
	cfg.setValue("archive age", days_since(2007, 9, 1));

	acquireSamples();
	removeDontpackFlagfile("testdir");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(TO_ARCHIVE), 2u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform packing and check that things are still ok afterwards
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: archived 2007/07-07.grib1\n"
		"testdir: archived 2007/07-08.grib1\n"
		"testdir: archive cleaned up\n"
		"testdir: 2 files archived, 30448 bytes reclaimed on the index, 30448 total bytes freed.\n");

	// Check that the files have been moved to the archive
	ensure(sys::fs::access("testdir/.archive/last/2007/07-07.grib1", F_OK));
	ensure(sys::fs::access("testdir/.archive/last/2007/07-07.grib1.metadata", F_OK));
	ensure(sys::fs::access("testdir/.archive/last/2007/07-07.grib1.summary", F_OK));
	ensure(sys::fs::access("testdir/.archive/last/2007/07-08.grib1", F_OK));
	ensure(sys::fs::access("testdir/.archive/last/2007/07-08.grib1.metadata", F_OK));
	ensure(sys::fs::access("testdir/.archive/last/2007/07-08.grib1.summary", F_OK));
	ensure(!sys::fs::access("testdir/2007/07-07.grib1", F_OK));
	ensure(!sys::fs::access("testdir/2007/07-08.grib1", F_OK));

	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(ARC_OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	s.str(std::string());
	writer.check(s, true, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(ARC_OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Test querying
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	utils::metadata::Collector mdc;
	reader.queryData(dataset::DataQuery(Matcher::parse(""), false), mdc);
	ensure_equals(mdc.size(), 3u);

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(!sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on perfect dataset, with data to delete
template<> template<>
void to::test<14>()
{
	// Data are from 07, 08, 10 2007
	int treshold[6] = { 2007, 9, 1, 0, 0, 0 };
	int now[6];
	grcal::date::now(now);
	long long int duration = grcal::date::duration(treshold, now);

	//cerr << str::fmt(duration/(3600*24)) + " days";
	cfg.setValue("delete age", str::fmt(duration/(3600*24)));

	acquireSamples();
	removeDontpackFlagfile("testdir");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(TO_DELETE), 2u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform packing and check that things are still ok afterwards
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: deleted 2007/07-07.grib1 (34960 freed)\n"
		"testdir: deleted 2007/07-08.grib1 (7218 freed)\n"
		"testdir: 2 files deleted, 2 files removed from index, 30448 bytes reclaimed on the index, 72626 total bytes freed.\n");


	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	s.str(std::string());
	writer.check(s, true, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(!sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on perfect dataset, with a truncated data file
template<> template<>
void to::test<15>()
{
	cfg.setValue("step", "yearly");
	acquireSamples();
	removeDontpackFlagfile("testdir");

	// Truncate the last grib out of a file
	if (truncate("testdir/20/2007.grib1", 42178) < 0)
		throw wibble::exception::System("truncating testdir/20/2007.grib1");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(OK), 0u);
	ensure_equals(c.count(TO_RESCAN), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform packing and check that things are still ok afterwards
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: 30448 bytes reclaimed on the index, 30448 total bytes freed.\n");

	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 0u);
	ensure_equals(c.count(TO_RESCAN), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	s.str(std::string());
	writer.check(s, true, true);
	ensure_equals(s.str(), 
		"testdir: rescanned 20/2007.grib1\n"
		"testdir: 1 file rescanned, 7736 bytes reclaimed cleaning the index.\n");
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 0u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	// Perform packing after the file has been rescanned and check that
	// things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed 20/2007.grib1 (0 saved)\n"
		"testdir: 1 file packed, 2576 bytes reclaimed on the index, 2576 total bytes freed.\n");
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(!sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
}

// Test accuracy of maintenance scan, on a dataset with a corrupted data file
template<> template<>
void to::test<16>()
{
	stringstream s;

	cfg.setValue("step", "monthly");
	acquireSamples();
	system("rm testdir/needs-check-do-not-pack");
	arki::dataset::ondisk2::Writer writer(cfg);
	// Pack the dataset because 07.grib1 imported data out of order
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed 2007/07.grib1 (0 saved)\n"
		"testdir: 1 file packed, 30448 bytes reclaimed on the index, 30448 total bytes freed.\n");
	s.str(std::string());

	// Corrupt the first grib in the file
	system("dd if=/dev/zero of=testdir/2007/07.grib1 bs=1 count=2 conv=notrunc status=noxfer > /dev/null 2>&1");

	MaintenanceCollector c;
	writer.maintenance(c);
	ensure_equals(c.fileStates.size(), 2u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	c.clear();
	writer.maintenance(c, false);
	ensure_equals(c.fileStates.size(), 2u);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.count(TO_RESCAN), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

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
}

// Test accuracy of maintenance scan, on a dataset with a data file larger than 2**31
template<> template<>
void to::test<17>()
{
	stringstream s;

	// Simulate 2007/07-07.grib1 to be 6G already
	system("mkdir -p testdir/2007");
	system("touch testdir/2007/07-07.grib1");
	// Truncate the last grib out of a file
	if (truncate("testdir/2007/07-07.grib1", 6000000000LLU) < 0)
		throw wibble::exception::System("truncating testdir/2007/07-07.grib1");
	acquireSamples();
	system("rm testdir/needs-check-do-not-pack");
	arki::dataset::ondisk2::Writer writer(cfg);

	MaintenanceCollector c;
	writer.maintenance(c, false);
	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

#if 0
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

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles, and duplicate items inside
template<> template<>
void to::test<18>()
{
	acquireSamples();

	// Compress one data file
	{
		utils::metadata::Collector mdc;
		Reader reader(cfg);
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
		ensure_equals(mdc.size(), 1u);
		mdc.compressDataFile(1024, "metadata file testdir/2007/07-08.grib1");
		sys::fs::deleteIfExists("testdir/2007/07-08.grib1");
	}

	// The dataset should still be clean
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// The dataset should still be clean even with an accurate scan
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c, false);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Remove the index
	system("rm testdir/index.sqlite");

	// See how maintenance scan copes
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(OK), 0u);
		ensure_equals(c.count(TO_INDEX), 3u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());

		stringstream s;

		// Perform full maintenance and check that things are still ok afterwards
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testdir: rescanned 2007/07-07.grib1\n"
			"testdir: rescanned 2007/07-08.grib1\n"
			"testdir: rescanned 2007/10-09.grib1\n"
			"testdir: 3 files rescanned, 30448 bytes reclaimed cleaning the index.\n");
		c.clear();
		writer.maintenance(c);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());

		// Perform packing and check that things are still ok afterwards
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
		c.clear();

		writer.maintenance(c);
		ensure_equals(c.count(OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());

		// Ensure that we have the summary cache
		ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
		ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
		ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
	}
}


}

// vim:set ts=4 sw=4:
