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

#include <arki/dataset/test-utils.h>
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
#include <strings.h>

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset::ondisk2;
using namespace arki::dataset::ondisk2::writer;

namespace arki {
namespace dataset {
namespace ondisk2 {
namespace writer {

struct MaintenanceCollector : public MaintFileVisitor
{
	std::map <std::string, State> fileStates;
	size_t counts[STATE_MAX];
	static const char* names[];
	std::set<State> checked;

	MaintenanceCollector()
	{
		bzero(counts, sizeof(counts));
	}

	void clear()
	{
		bzero(counts, sizeof(counts));
		fileStates.clear();
		checked.clear();
	}

	bool isClean() const
	{
		for (size_t i = 0; i < STATE_MAX; ++i)
			if (i != OK && i != ARC_OK && counts[i])
				return false;
		return true;
	}

	virtual void operator()(const std::string& file, State state)
	{
		fileStates[file] = state;
		++counts[state];
	}

	void dump(std::ostream& out) const
	{
		using namespace std;
		out << "Results:" << endl;
		for (size_t i = 0; i < STATE_MAX; ++i)
			out << " " << names[i] << ": " << counts[i] << endl;
		for (std::map<std::string, State>::const_iterator i = fileStates.begin();
				i != fileStates.end(); ++i)
			out << "   " << i->first << ": " << names[i->second] << endl;
	}

	size_t count(State s)
	{
		checked.insert(s);
		return counts[s];
	}

	std::string remaining() const
	{
		std::vector<std::string> res;
		for (size_t i = 0; i < MaintFileVisitor::STATE_MAX; ++i)
		{
			if (checked.find((State)i) != checked.end())
				continue;
			if (counts[i] == 0)
				continue;
			res.push_back(str::fmtf("%s: %d", names[i], counts[i]));
		}
		return str::join(res.begin(), res.end());
	}
};

const char* MaintenanceCollector::names[] = {
	"ok",
	"to archive",
	"to delete",
	"to pack",
	"to index",
	"to rescan",
	"deleted",
	"arc ok",
	"arc to delete",
	"arc to index",
	"arc to rescan",
	"arc deleted",
	"state max",
};

}
}
}
}

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

	virtual void operator()(const std::string& file, State state) {}
};
TESTGRP(arki_dataset_ondisk2_maintenance);

// Test accuracy of maintenance scan, on perfect dataset
template<> template<>
void to::test<1>()
{
	acquireSamples();

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count(OK), 3u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());

	// Check that maintenance does not accidentally create an archive
	ensure(!sys::fs::access("testdir/archive", F_OK));

	stringstream s;

	// Perform packing and check that things are still ok afterwards
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 29416 bytes reclaimed on the index, 29416 total bytes freed.\n");

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
}

// Test accuracy of maintenance scan, on dataset with one file deleted,
// performing repack
template<> template<>
void to::test<2>()
{
	acquireSamples();

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
		"testdir: 1 file removed from index, 29416 bytes reclaimed on the index, 29416 total bytes freed.\n");
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
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file removed from index, 29416 bytes reclaimed cleaning the index.\n");

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
}

// Test accuracy of maintenance scan, on dataset with one file to reclaim,
// performing repack
template<> template<>
void to::test<4>()
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

	// Perform packing and check that things are still ok afterwards
	stringstream s;
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: deleted 2007/07-07.grib1 (34960 freed)\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file deleted, 29416 bytes reclaimed on the index, 64376 total bytes freed.\n");

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
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file rescanned, 29416 bytes reclaimed cleaning the index.\n");
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

}

// Test accuracy of maintenance scan, on dataset with one file to pack,
// performing repack
template<> template<>
void to::test<6>()
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

	// Perform packing and check that things are still ok afterwards
	stringstream s;
	writer.repack(s, true);
	ensure_equals(s.str(),
		"testdir: packed 2007/07.grib1 (34960 saved)\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file packed, 29416 bytes reclaimed on the index, 64376 total bytes freed.\n");
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
	MetadataCounter counter;
	writer.check(s, counter);
	ensure_equals(counter.count, 0u);
	ensure_equals(s.str(),
		"testdir: packed 2007/07.grib1 (34960 saved)\n"
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file packed, 29416 bytes reclaimed cleaning the index.\n");
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
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 3 files rescanned, 29416 bytes reclaimed cleaning the index.\n");
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
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 3 files rescanned, 29416 bytes reclaimed cleaning the index.\n");
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
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file rescanned, 29416 bytes reclaimed cleaning the index.\n");
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
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file rescanned, 29416 bytes reclaimed cleaning the index.\n");

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
		"testdir: database cleaned up\n"
		"testdir: rebuild summary cache\n"
		"testdir: 1 file rescanned, 29416 bytes reclaimed cleaning the index, 2 data items could not be reindexed.\n");
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
}

// Test accuracy of maintenance scan, on perfect dataset, with data to archive
template<> template<>
void to::test<13>()
{
	// Data are from 07, 08, 10 2007
	int treshold[6] = { 2007, 9, 1, 0, 0, 0 };
	int now[6];
	grcal::date::now(now);
	long long int duration = grcal::date::duration(treshold, now);

	//cerr << str::fmt(duration/(3600*24)) + " days";
	cfg.setValue("archive age", str::fmt(duration/(3600*24)));

	acquireSamples();

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
		"testdir: 2 files archived, 29416 bytes reclaimed on the index, 29416 total bytes freed.\n");

	// Check that the files have been moved to the archive
	ensure(sys::fs::access("testdir/archive/2007/07-07.grib1", F_OK));
	ensure(sys::fs::access("testdir/archive/2007/07-07.grib1.metadata", F_OK));
	ensure(sys::fs::access("testdir/archive/2007/07-07.grib1.summary", F_OK));
	ensure(sys::fs::access("testdir/archive/2007/07-08.grib1", F_OK));
	ensure(sys::fs::access("testdir/archive/2007/07-08.grib1.metadata", F_OK));
	ensure(sys::fs::access("testdir/archive/2007/07-08.grib1.summary", F_OK));
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
		"testdir: 2 files deleted, 2 files removed from index, 29416 bytes reclaimed on the index, 71594 total bytes freed.\n");


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
