/*
 * Copyright (C) 2007,2008,2009  Enrico Zini <enrico@enricozini.org>
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
#include <arki/dataset/ondisk2/maintenance.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/ondisk2/reader.h>
#include <arki/metadata.h>
#include <arki/configfile.h>
#include <arki/scan/grib.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
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

struct arki_dataset_ondisk2_maintenance_shar : public MaintFileVisitor {
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

	stringstream s;

	// Perform packing and check that things are still ok afterwards
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 30448 bytes reclaimed on the index, 30448 total bytes freed.\n");

	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	MetadataCounter counter;
	s.str(std::string());
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());

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

	stringstream s;

	// Perform packing and check that things are still ok afterwards
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: deleted from index 2007/07-07.grib1\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file removed from index, 30448 bytes reclaimed on the index, 30448 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	MetadataCounter counter;
	s.str(std::string());
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());

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
	MetadataCounter counter;
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
	ensure_equals(s.str(),
		"testdir: deindexed 2007/07-07.grib1\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
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

	// Perform packing and check that things are still ok afterwards
	stringstream s;
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: deleted 2007/07-07.grib1 (34960 freed)\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file deleted, 30448 bytes reclaimed on the index, 65408 total bytes freed.\n");

	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	MetadataCounter counter;
	s.str(std::string());
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
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
	MetadataCounter counter;
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-07.grib1\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
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
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file packed, 30448 bytes reclaimed on the index, 65408 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	MetadataCounter counter;
	s.str(std::string());
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
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
	MetadataCounter counter;
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
	ensure_equals(s.str(),
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
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
		"testdir: database cleaned up\n"
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
	MetadataCounter counter;
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-07.grib1\n"
		"testdir: rescanned 2007/07-08.grib1\n"
		"testdir: rescanned 2007/10-09.grib1\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
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
	MetadataCounter counter;
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-07.grib1\n"
		"testdir: rescanned 2007/07-08.grib1\n"
		"testdir: rescanned 2007/10-09.grib1\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
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
	MetadataCounter counter;
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
	ensure_equals(s.str(),
		"testdir: rescanned foo/bar/test.grib1\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
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
		"testdir: database cleaned up\n"
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
	MetadataCollector mdc;
	reader.queryMetadata(Matcher::parse("origin:GRIB1,200"), false, mdc);
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
	MetadataCounter counter;
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
	ensure_equals(s.str(),
		"testdir: rescanned foo/bar/test.grib1\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file rescanned, 30448 bytes reclaimed cleaning the index.\n");

	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	ensure_equals(utils::files::size("testdir/foo/bar/test.grib1"), 44412*2);

	{
		// Test querying
		Reader reader(cfg);
		ensure(reader.hasWorkingIndex());
		MetadataCollector mdc;
		reader.queryMetadata(Matcher::parse("origin:GRIB1,80"), false, mdc);
		ensure_equals(mdc.size(), 1u);
		UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
		ensure_equals(blob->format, "grib1"); 
		ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
		ensure_equals(blob->offset, 51630u);
		ensure_equals(blob->size, 34960u);

		// Query the second element and check that it starts after the first one
		// (there used to be a bug where the rebuild would use the offsets of
		// the metadata instead of the data)
		mdc.clear();
		reader.queryMetadata(Matcher::parse("origin:GRIB1,200"), false, mdc);
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
		"testdir: database cleaned up\n"
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
	MetadataCollector mdc;
	reader.queryMetadata(Matcher::parse("origin:GRIB1,80"), false, mdc);
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
	reader.queryMetadata(Matcher::parse("origin:GRIB1,200"), false, mdc);
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
	MetadataCounter counter;
	writer.check(s, counter);
	ensure_equals(counter.count, 2u);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-08.grib1\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file rescanned, 30448 bytes reclaimed cleaning the index, 2 data items could not be reindexed.\n");
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 2u);
	ensure_equals(c.count(TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	ensure_equals(utils::files::size("testdir/2007/07-08.grib1"), 7218 + 44412);

	{
		// Test querying: reindexing should have chosen the last version of
		// duplicate items
		Reader reader(cfg);
		ensure(reader.hasWorkingIndex());
		MetadataCollector mdc;
		reader.queryMetadata(Matcher::parse("origin:GRIB1,200"), false, mdc);
		ensure_equals(mdc.size(), 1u);
		UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
		ensure_equals(blob->format, "grib1"); 
		ensure_equals(blob->filename, sys::fs::abspath("testdir/2007/07-08.grib1"));
		ensure_equals(blob->offset, 7218u);
		ensure_equals(blob->size, 7218u);

		// Query another element and check that it has not been relocated to
		// the wrong file
		mdc.clear();
		reader.queryMetadata(Matcher::parse("origin:GRIB1,80"), false, mdc);
		ensure_equals(mdc.size(), 1u);
		blob = mdc[0].source.upcast<source::Blob>();
		ensure_equals(blob->format, "grib1"); 
		ensure_equals(blob->filename, sys::fs::abspath("testdir/2007/07-07.grib1"));
		ensure_equals(blob->offset, 0u);
		ensure_equals(blob->size, 34960u);
	}

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed 2007/07-08.grib1 (44412 saved)\n"
		"testdir: database cleaned up\n"
		"testdir: 1 file packed, 2576 bytes reclaimed on the index, 46988 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	ensure_equals(utils::files::size("testdir/2007/07-08.grib1"), 7218);

	// Test querying, after repack this item should have been moved to the
	// beginning of the file
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	MetadataCollector mdc;
	reader.queryMetadata(Matcher::parse("origin:GRIB1,200"), false, mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/2007/07-08.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 7218u);

	// Query another element and check that it has not been relocated to
	// the wrong file
	mdc.clear();
	reader.queryMetadata(Matcher::parse("origin:GRIB1,80"), false, mdc);
	ensure_equals(mdc.size(), 1u);
	blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/2007/07-07.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 34960u);

	// Ensure that we have the summary cache
	ensure(sys::fs::access("testdir/.summaries/all.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-07.summary", F_OK));
	ensure(sys::fs::access("testdir/.summaries/2007-10.summary", F_OK));
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
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
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
	MetadataCounter counter;
	s.str(std::string());
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
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
	MetadataCollector mdc;
	reader.queryMetadata(Matcher::parse(""), false, mdc);
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
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 2 files deleted, 2 files removed from index, 30448 bytes reclaimed on the index, 72626 total bytes freed.\n");


	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	MetadataCounter counter;
	s.str(std::string());
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
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
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 30448 bytes reclaimed on the index, 30448 total bytes freed.\n");

	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(OK), 0u);
	ensure_equals(c.count(TO_RESCAN), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	MetadataCounter counter;
	s.str(std::string());
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
	ensure_equals(s.str(), 
		"testdir: rescanned 20/2007.grib1\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
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
		"testdir: database cleaned up\n"
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

#if 0
// Test accuracy of maintenance scan, with index, on dataset with some
// outdated summaries
template<> template<>
void to::test<9>()
{
	acquireSamples();
	system("touch -d yesterday testdir/2007/*");
	system("touch -d yesterday testdir/*");
	system("touch testdir/2007/07-08.grib1.metadata");

	Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);
	c.sort();

	ensure_equals(c.writerPath, "testdir");
	ensure_equals(c.hasEnded, true);
	ensure_equals(c.fullIndex, false);
	ensure_equals(c.datafileRebuild.size(), 0u);
	ensure_equals(c.summaryRebuildFile.size(), 1u);
	ensure_equals(c.summaryRebuildFile[0], "2007/07-08.grib1");
	ensure_equals(c.summaryRebuildDir.size(), 0u);

	// Perform full maintenance and check that things are ok afterwards
	stringstream maintlog;
	MetadataCounter counter;
	FullMaintenance m(maintlog, counter);
	writer.maintenance(m);
	ensure_equals(counter.count, 0u);
	c.clear();
	writer.maintenance(c);
	ensure_equals(counter.count, 0u);
	ensure(c.isClean());
}

// Test accuracy of maintenance scan, with index, on dataset with some
// outdated summaries
template<> template<>
void to::test<10>()
{
	acquireSamples();
	system("touch -d yesterday testdir/2007/*");
	system("touch -d yesterday testdir/*");
	system("touch testdir/2007/07-08.grib1.summary");

	Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);
	c.sort();

	ensure_equals(c.writerPath, "testdir");
	ensure_equals(c.hasEnded, true);
	ensure_equals(c.fullIndex, false);
	ensure_equals(c.datafileRebuild.size(), 0u);
	ensure_equals(c.summaryRebuildFile.size(), 0u);
	ensure_equals(c.summaryRebuildDir.size(), 1u);
	ensure_equals(c.summaryRebuildDir[0], "2007");

	// Perform full maintenance and check that things are ok afterwards
	stringstream maintlog;
	MetadataCounter counter;
	FullMaintenance m(maintlog, counter);
	writer.maintenance(m);
	ensure_equals(counter.count, 0u);
	c.clear();
	writer.maintenance(c);
	ensure_equals(counter.count, 0u);
	ensure(c.isClean());
}

// Test accuracy of maintenance scan, with index, on dataset with some
// outdated summaries
template<> template<>
void to::test<11>()
{
	acquireSamples();
	system("touch -d yesterday testdir/2007/*");
	system("touch -d yesterday testdir/*");
	system("touch testdir/2007/summary");

	Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);
	c.sort();

	ensure_equals(c.writerPath, "testdir");
	ensure_equals(c.hasEnded, true);
	ensure_equals(c.fullIndex, false);
	ensure_equals(c.datafileRebuild.size(), 0u);
	ensure_equals(c.summaryRebuildFile.size(), 0u);
	ensure_equals(c.summaryRebuildDir.size(), 1u);
	ensure_equals(c.summaryRebuildDir[0], "");

	// Perform full maintenance and check that things are ok afterwards
	stringstream maintlog;
	MetadataCounter counter;
	FullMaintenance m(maintlog, counter);
	writer.maintenance(m);
	ensure_equals(counter.count, 0u);
	c.clear();
	writer.maintenance(c);
	ensure_equals(counter.count, 0u);
	ensure(c.isClean());
}

// Test invalidating and rebuilding a dataset
template<> template<>
void to::test<13>()
{
	acquireSamples();
	system("touch -d yesterday testdir/2007/*");
	system("touch -d yesterday testdir/*");

	Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);
	ensure(c.isClean());

	writer.invalidateAll();

	c.clear();
	writer.maintenance(c);

	ensure_equals(c.writerPath, "testdir");
	ensure_equals(c.hasEnded, true);
	ensure_equals(c.fullIndex, true);
	ensure_equals(c.datafileRebuild.size(), 3u);
	ensure_equals(c.summaryRebuildFile.size(), 0u);
	ensure_equals(c.summaryRebuildDir.size(), 0u);
	ensure(!c.isClean());

	// Perform full maintenance and check that things are ok afterwards
	stringstream maintlog;
	MetadataCounter counter;
	FullMaintenance m(maintlog, counter);
	writer.maintenance(m);
	ensure_equals(counter.count, 0u);
	c.clear();
	writer.maintenance(c);
	ensure_equals(counter.count, 0u);
	ensure(c.isClean());
}

#endif

}

// vim:set ts=4 sw=4:
