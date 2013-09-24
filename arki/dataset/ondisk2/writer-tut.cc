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

#include <arki/dataset/ondisk2/test-utils.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/ondisk2/reader.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/configfile.h>
#include <arki/scan/grib.h>
#include <arki/scan/bufr.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/summary.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <algorithm>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::dataset::ondisk2;
using namespace arki::dataset::ondisk2::writer;
using namespace arki::utils;

namespace tut {

struct arki_dataset_ondisk2_writer_shar : public arki::tests::DatasetTest {
	// Little dirty hack: implement MaintFileVisitor so we can conveniently
	// access State

	ConfigFile cfg;

	arki_dataset_ondisk2_writer_shar()
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
};
TESTGRP(arki_dataset_ondisk2_writer);

// Test accuracy of maintenance scan, on dataset with one file to reclaim,
// performing repack
template<> template<>
void to::test<1>()
{
	acquireSamples();
	files::removeDontpackFlagfile("testdir");
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
	ensure_equals(c.count(COUNTED_OK), 2u);
	ensure_equals(c.count(COUNTED_TO_INDEX), 1u);
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
		ensure_equals(c.count(COUNTED_OK), 2u);
		ensure_equals(c.count(COUNTED_TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

    // Perform packing and check that things are still ok afterwards
    {
        stringstream s;
        writer.repack(s, true);
        wtest(contains, "testdir: deleted 2007/07-07.grib1 (34960 freed)", s.str());
        wtest(re_match, "testdir: 1 file deleted, [0-9]+ total bytes freed.", s.str());

		c.clear();

		writer.maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 2u);
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
		ensure_equals(c.count(COUNTED_OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Ensure that we have the summary cache
	ensure(sys::fs::exists("testdir/.summaries/all.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
}

// Test accuracy of maintenance scan, on dataset with one file to reclaim,
// performing check
template<> template<>
void to::test<2>()
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
	ensure_equals(c.count(COUNTED_OK), 2u);
	ensure_equals(c.count(COUNTED_TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-07.grib1\n"
		"testdir: 1 file rescanned.\n");
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(COUNTED_OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(COUNTED_OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::exists("testdir/.summaries/all.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
}

// Test accuracy of maintenance scan, on dataset with one file to pack,
// performing repack
template<> template<>
void to::test<3>()
{
	cfg.setValue("step", "monthly");
	acquireSamples();
	files::removeDontpackFlagfile("testdir");
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
	ensure_equals(c.count(COUNTED_OK), 1u);
	ensure_equals(c.count(COUNTED_TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

    // Perform packing and check that things are still ok afterwards
    stringstream s;
    writer.repack(s, true);
    wtest(contains, "testdir: packed 2007/07.grib1 (34960 saved)", s.str());
    wtest(re_match, "testdir: 1 file packed, [0-9]+ total bytes freed.", s.str());
    c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(COUNTED_OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
	s.str(std::string());
	writer.check(s, true, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(COUNTED_OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::exists("testdir/.summaries/all.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
}

// Test accuracy of maintenance scan, on dataset with one file to pack,
// performing check
template<> template<>
void to::test<4>()
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
	ensure_equals(c.count(COUNTED_OK), 1u);
	ensure_equals(c.count(COUNTED_TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	// No packing occurs here: check does not mangle data files
	writer.check(s, true, true);
	ensure_equals(s.str(), "");

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
		"testdir: 1 file packed, 37536 total bytes freed.\n");
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(COUNTED_OK), 2u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::exists("testdir/.summaries/all.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
}

// Test accuracy of maintenance scan, after deleting the index
template<> template<>
void to::test<5>()
{
	acquireSamples();
	system("rm testdir/index.sqlite");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(COUNTED_OK), 0u);
	ensure_equals(c.count(COUNTED_TO_INDEX), 3u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned 2007/07-07.grib1\n"
		"testdir: rescanned 2007/07-08.grib1\n"
		"testdir: rescanned 2007/10-09.grib1\n"
		"testdir: 3 files rescanned.\n");
	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(COUNTED_OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Perform packing and check that things are still ok afterwards
	s.str(std::string());
	writer.repack(s, true);
	ensure_equals(s.str(), string()); // Nothing should have happened
	c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(COUNTED_OK), 3u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	// Ensure that we have the summary cache
	ensure(sys::fs::exists("testdir/.summaries/all.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
}

// Test recreating a dataset from just a datafile with duplicate data and a rebuild flagfile
template<> template<>
void to::test<6>()
{
	system("mkdir testdir");
	system("mkdir testdir/foo");
	system("mkdir testdir/foo/bar");
	system("cat inbound/test.grib1 inbound/test.grib1 > testdir/foo/bar/test.grib1");

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(COUNTED_TO_INDEX), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	stringstream s;

	// Perform full maintenance and check that things are still ok afterwards
	writer.check(s, true, true);
	ensure_equals(s.str(),
		"testdir: rescanned foo/bar/test.grib1\n"
		"testdir: 1 file rescanned.\n");

	c.clear();
	writer.maintenance(c);
	ensure_equals(c.count(COUNTED_TO_PACK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(not c.isClean());

	ensure_equals(utils::files::size("testdir/foo/bar/test.grib1"), 44412*2);

	{
		// Test querying: reindexing should have chosen the last version of
		// duplicate items
		Reader reader(cfg);
		ensure(reader.hasWorkingIndex());
		metadata::Collection mdc;
		reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
		ensure_equals(mdc.size(), 1u);
                wtest(sourceblob_is, "grib1", sys::fs::abspath("testdir"), "foo/bar/test.grib1", 51630, 34960, mdc[0].source);

        mdc.clear();
        reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
        ensure_equals(mdc.size(), 1u);
        wtest(sourceblob_is, "grib1", sys::fs::abspath("testdir"), "foo/bar/test.grib1", 44412, 7218, mdc[0].source);
    }

    // Perform packing and check that things are still ok afterwards
    s.str(std::string());
    writer.repack(s, true);
    wtest(contains, "testdir: packed foo/bar/test.grib1 (44412 saved)", s.str());
    wtest(re_match, "testdir: 1 file packed, [0-9]+ total bytes freed.", s.str());
    c.clear();

	writer.maintenance(c);
	ensure_equals(c.count(COUNTED_OK), 1u);
	ensure_equals(c.remaining(), "");
	ensure(c.isClean());

	ensure_equals(utils::files::size("testdir/foo/bar/test.grib1"), 44412);

	// Test querying, and see that things have moved to the beginning
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	metadata::Collection mdc;
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
	ensure_equals(mdc.size(), 1u);
        wtest(sourceblob_is, "grib1", sys::fs::abspath("testdir"), "foo/bar/test.grib1", 0, 34960, mdc[0].source);

	// Query the second element and check that it starts after the first one
	// (there used to be a bug where the rebuild would use the offsets of
	// the metadata instead of the data)
	mdc.clear();
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);
        wtest(sourceblob_is, "grib1", sys::fs::abspath("testdir"), "foo/bar/test.grib1", 34960, 7218, mdc[0].source);

	// Ensure that we have the summary cache
	ensure(sys::fs::exists("testdir/.summaries/all.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
}

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles, and duplicate items inside
template<> template<>
void to::test<7>()
{
	acquireSamples();
	system("cat inbound/test.grib1 >> testdir/2007/07-08.grib1");
    {
        index::WContents index(cfg);
        index.open();
        index.reset("2007/07-08.grib1");
    }

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(COUNTED_OK), 2u);
	ensure_equals(c.count(COUNTED_TO_INDEX), 1u);
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

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles, and duplicate items inside
template<> template<>
void to::test<8>()
{
	acquireSamples();

	// Compress one data file
	{
		metadata::Collection mdc;
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
		ensure_equals(c.count(COUNTED_OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// The dataset should still be clean even with an accurate scan
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c, false);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 3u);
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
		ensure_equals(c.count(COUNTED_OK), 0u);
		ensure_equals(c.count(COUNTED_TO_INDEX), 3u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());

		stringstream s;

		// Perform full maintenance and check that things are still ok afterwards
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testdir: rescanned 2007/07-07.grib1\n"
			"testdir: rescanned 2007/07-08.grib1\n"
			"testdir: rescanned 2007/10-09.grib1\n"
			"testdir: 3 files rescanned.\n");
		c.clear();
		writer.maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());

		// Perform packing and check that things are still ok afterwards
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
		c.clear();

		writer.maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());

		// Ensure that we have the summary cache
		ensure(sys::fs::exists("testdir/.summaries/all.summary"));
		ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
		ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));
	}
}


// Test sanity checks on summary cache
template<> template<>
void to::test<9>()
{
	// If we are root we can always write the summary cache, so the tests
	// will fail
	if (getuid() == 0)
		return;

	acquireSamples();
	files::removeDontpackFlagfile("testdir");

	arki::dataset::ondisk2::Writer writer(cfg);

	// Dataset is ok
	{
		MaintenanceCollector c;
		writer.maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 3u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

    // Perform packing to regenerate the summary cache
    {
        stringstream s;
        writer.repack(s, true);
        wtest(equals, "", s.str());
    }

	// Ensure that we have the summary cache
	ensure(sys::fs::exists("testdir/.summaries/all.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-07.summary"));
	ensure(sys::fs::exists("testdir/.summaries/2007-10.summary"));

	// Make one summary cache file not writable
	chmod("testdir/.summaries/all.summary", 0400);

	// Perform check and see that we detect it
	{
		stringstream s;
		writer.check(s, false, true);
		ensure_equals(s.str(), "testdir: " + sys::fs::abspath("testdir/.summaries/all.summary") + " is not writable.\n");
	}

	// Fix it
	{
		stringstream s;
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testdir: " + sys::fs::abspath("testdir/.summaries/all.summary") + " is not writable.\n"
			"testdir: rebuilding summary cache.\n");
	}

	// Check again and see that everything is fine
	{
		stringstream s;
		writer.check(s, false, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}
#if 0

	// Perform packing and check that things are still ok afterwards

	// Perform full maintenance and check that things are still ok afterwards
#endif

}

// Test that the summary cache is properly invalidated on import
template<> template<>
void to::test<10>()
{
	// Perform maintenance on empty dir, creating an empty summary cache
	{
		arki::dataset::ondisk2::Writer writer(cfg);
		MaintenanceCollector c;
		writer.maintenance(c);

		ensure_equals(c.fileStates.size(), 0u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Query the summary, there should be no data
	{
		Reader reader(cfg);
		ensure(reader.hasWorkingIndex());
		Summary s;
		reader.querySummary(Matcher(), s);
		ensure_equals(s.count(), 0u);
	}

	// Acquire files
	acquireSamples();

	// Query the summary again, there should be data
	{
		Reader reader(cfg);
		ensure(reader.hasWorkingIndex());
		Summary s;
		reader.querySummary(Matcher(), s);
		ensure_equals(s.count(), 3u);
	}
}

// Try to reproduce a bug where two conflicting BUFR files were not properly
// imported with USN handling
template<> template<>
void to::test<11>()
{
    ConfigFile cfg;
    cfg.setValue("path", "gts_temp");
    cfg.setValue("name", "gts_temp");
    cfg.setValue("type", "ondisk2");
    cfg.setValue("step", "daily");
    cfg.setValue("unique", "origin, reftime, proddef");
    cfg.setValue("filter", "product:BUFR:t=temp");
    cfg.setValue("replace", "USN");

    Metadata md;
    scan::Bufr scanner;
    Writer writer(cfg);
    scanner.open("inbound/conflicting-temp-same-usn.bufr");
    size_t count = 0;
    for ( ; scanner.next(md); ++count)
        ensure_equals(writer.acquire(md), WritableDataset::ACQ_OK);
    ensure_equals(count, 2u);
    writer.flush();
}

}

// vim:set ts=4 sw=4:
