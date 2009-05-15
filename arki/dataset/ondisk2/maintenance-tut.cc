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
#if 0
#include <arki/dataset/ondisk2/maint/directory.h>
#include <arki/dataset/ondisk2/maint/datafile.h>
#include <arki/dataset/ondisk2/common.h>
#include <arki/dataset/ondisk2/reader.h>
#endif
#include <arki/metadata.h>
#include <arki/configfile.h>
#include <arki/scan/grib.h>
#include <arki/utils.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <algorithm>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset::ondisk2;
using namespace arki::dataset::ondisk2::writer;

struct MaintenanceCollector2 : public MaintFileVisitor
{
	map <string, State> fileStates;
	size_t count_ok;
	size_t count_pack;
	size_t count_index;
	size_t count_rescan;

	MaintenanceCollector2()
		: count_ok(0), count_pack(0), count_index(0), count_rescan(0) {}

	bool isClean() const
	{
		return count_pack == 0 and count_index == 0 and count_rescan == 0;
	}

	virtual void operator()(const std::string& file, State state)
	{
		fileStates[file] = state;
		switch (state)
		{
			case OK:	++count_ok;	break;
			case TO_PACK:	++count_pack;	break;
			case TO_INDEX:	++count_index;	break;
			case TO_RESCAN:	++count_rescan;	break;
		}
	}
};


#if 0
struct MaintenanceCollector : public MaintenanceAgent
{
	string writerPath;
	bool hasEnded, fullIndex;
	vector<string> datafileRebuild, summaryRebuildFile, reindexFile, summaryRebuildDir;

	MaintenanceCollector() :
		hasEnded(false), fullIndex(false) {}

	virtual void start(Writer& w) { writerPath = w.path(); }
	virtual void end() { hasEnded = true; }

	virtual void needsDatafileRebuild(Datafile& df)
	{
		datafileRebuild.push_back(df.relname);
	}
	virtual void needsSummaryRebuild(Datafile& df)
	{
		//cerr << "needsSummaryRebuild " << df.pathname << endl;
		summaryRebuildFile.push_back(df.relname);
	}
	virtual void needsReindex(maint::Datafile& df)
	{
		reindexFile.push_back(df.relname);
	}
	virtual void needsSummaryRebuild(Directory& df)
	{
		//cerr << "needsSummaryRebuild " << df.fullpath() << endl;
		summaryRebuildDir.push_back(df.relpath());
	}
	virtual void needsFullIndexRebuild(RootDirectory& df)
	{
		fullIndex = true;
	}
	void sort()
	{
		std::sort(datafileRebuild.begin(), datafileRebuild.end());
		std::sort(summaryRebuildFile.begin(), summaryRebuildFile.end());
		std::sort(summaryRebuildDir.begin(), summaryRebuildDir.end());
	}

	void clear()
	{
		writerPath.clear();
		hasEnded = false;
		fullIndex = false;
		datafileRebuild.clear();
		summaryRebuildFile.clear();
		summaryRebuildDir.clear();
	}

	bool isClean()
	{
		if (writerPath.empty()) return false;
		//cerr << "writerPath ok" << endl;
		if (!hasEnded) return false;
		//cerr << "hasEnded ok" << endl;
		if (fullIndex) return false;
		//cerr << "fullIndex ok" << endl;
		if (!datafileRebuild.empty()) return false;
		//cerr << "datafileRebuild ok" << endl;
		if (!summaryRebuildFile.empty()) return false;
		//cerr << "summaryRebuildFile ok" << endl;
		if (!summaryRebuildDir.empty()) return false;
		//cerr << "summaryRebuildDir ok" << endl;
		return true;
	}
};

#endif

struct arki_dataset_ondisk2_maintenance_shar {
	ConfigFile cfg;

	arki_dataset_ondisk2_maintenance_shar()
	{
		system("rm -rf testdir");

		cfg.setValue("path", "testdir");
		cfg.setValue("name", "ondisk2");
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
TESTGRP(arki_dataset_ondisk2_maintenance);

// Test accuracy of maintenance scan, on perfect dataset
template<> template<>
void to::test<1>()
{
	acquireSamples();

	arki::dataset::ondisk2::Writer writer(cfg);
	MaintenanceCollector2 c;
	writer.maintenance(c);

	ensure_equals(c.fileStates.size(), 3u);
	ensure_equals(c.count_ok, 3u);
	ensure_equals(c.count_pack, 0u);
	ensure_equals(c.count_index, 0u);
	ensure_equals(c.count_rescan, 0u);
	ensure(c.isClean());

#if 0
	// Perform full maintenance and check that things are still ok afterwards
	stringstream maintlog;
	MetadataCounter counter;
	FullMaintenance m(maintlog, counter);
	writer.maintenance(m);
	ensure_equals(counter.count, 0u);
	c.clear();
	writer.maintenance(c);
	ensure_equals(counter.count, 0u);
	ensure(c.isClean());
#endif
}

#if 0
// Test accuracy of maintenance scan, without index, on dataset with some
// rebuild flagfiles
template<> template<>
void to::test<2>()
{
	cfg.setValue("index", string());
	cfg.setValue("unique", string());
	acquireSamples();
	createNewRebuildFlagfile("testdir/2007/07-08.grib1");
	createNewRebuildFlagfile("testdir/2007/10-09.grib1");

	Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);
	c.sort();

	ensure_equals(c.writerPath, "testdir");
	ensure_equals(c.hasEnded, true);
	ensure_equals(c.fullIndex, false);
	//cerr << "REBUILD" << endl << c.datafileRebuild << endl;
	ensure_equals(c.datafileRebuild.size(), 2u);
	ensure_equals(c.datafileRebuild[0], "2007/07-08.grib1");
	ensure_equals(c.datafileRebuild[1], "2007/10-09.grib1");
	ensure_equals(c.summaryRebuildFile.size(), 0u);
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

// Test accuracy of maintenance scan, without index, on dataset with some
// outdated summaries
template<> template<>
void to::test<3>()
{
	cfg.setValue("index", string());
	cfg.setValue("unique", string());
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
	//system("bash");
	ensure(c.isClean());
}

// Test accuracy of maintenance scan, without index, on dataset with some
// outdated summaries
template<> template<>
void to::test<4>()
{
	cfg.setValue("index", string());
	cfg.setValue("unique", string());
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

// Test accuracy of maintenance scan, without index, on dataset with some
// outdated summaries
template<> template<>
void to::test<5>()
{
	cfg.setValue("index", string());
	cfg.setValue("unique", string());
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


// Test accuracy of maintenance scan, with index, on perfect dataset
template<> template<>
void to::test<6>()
{
	acquireSamples();

	Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.writerPath, "testdir");
	ensure_equals(c.hasEnded, true);
	ensure_equals(c.fullIndex, false);
	ensure_equals(c.datafileRebuild.size(), 0u);
	ensure_equals(c.summaryRebuildFile.size(), 0u);
	ensure_equals(c.summaryRebuildDir.size(), 0u);
	ensure(c.isClean());

	// Perform full maintenance and check that things are still ok afterwards
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

// Test accuracy of maintenance scan, with index, with index-out-of-date flagfile
template<> template<>
void to::test<7>()
{
	acquireSamples();
	createNewIndexFlagfile("testdir");

	Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);

	ensure_equals(c.writerPath, "testdir");
	ensure_equals(c.hasEnded, true);
	ensure_equals(c.fullIndex, true);
	ensure_equals(c.datafileRebuild.size(), 0u);
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

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles
template<> template<>
void to::test<8>()
{
	acquireSamples();
	createNewRebuildFlagfile("testdir/2007/07-08.grib1");
	createNewRebuildFlagfile("testdir/2007/10-09.grib1");

	Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);
	c.sort();

	ensure_equals(c.writerPath, "testdir");
	ensure_equals(c.hasEnded, true);
	ensure_equals(c.fullIndex, false);
	ensure_equals(c.datafileRebuild.size(), 2u);
	ensure_equals(c.datafileRebuild[0], "2007/07-08.grib1");
	ensure_equals(c.datafileRebuild[1], "2007/10-09.grib1");
	ensure_equals(c.summaryRebuildFile.size(), 0u);
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

// Test recreating a dataset from just a datafile and a rebuild flagfile
template<> template<>
void to::test<12>()
{
	system("mkdir testdir");
	system("mkdir testdir/foo");
	system("mkdir testdir/foo/bar");
	system("cp inbound/test.grib1 testdir/foo/bar/");
	createNewRebuildFlagfile("testdir/foo/bar/test.grib1");

	Writer writer(cfg);
	stringstream maintlog;
	MetadataCounter counter;
	FullMaintenance m(maintlog, counter);
	//FullMaintenance m(cerr, counter);
	writer.maintenance(m);

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/foo/bar/test.grib1"));
	ensure(!hasPackFlagfile("testdir/foo/bar/test.grib1"));
	ensure(utils::hasFlagfile("testdir/foo/bar/test.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/foo/bar/test.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/foo/bar/summary"));
	ensure(utils::hasFlagfile("testdir/foo/summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(utils::hasFlagfile("testdir/index.sqlite"));

	// Test querying
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	MetadataCollector mdc;
	reader.queryMetadata(Matcher::parse("origin:GRIB1,200"), false, mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
	ensure_equals(blob->offset, 0u);
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

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles, and duplicate items inside
template<> template<>
void to::test<14>()
{
	acquireSamples();
	createNewRebuildFlagfile("testdir/2007/07-08.grib1");
	createNewRebuildFlagfile("testdir/2007/10-09.grib1");
	system("cat inbound/test.grib1 >> testdir/2007/07-08.grib1");

	Writer writer(cfg);
	MaintenanceCollector c;
	writer.maintenance(c);
	c.sort();

	ensure_equals(c.writerPath, "testdir");
	ensure_equals(c.hasEnded, true);
	ensure_equals(c.fullIndex, false);
	ensure_equals(c.datafileRebuild.size(), 2u);
	ensure_equals(c.datafileRebuild[0], "2007/07-08.grib1");
	ensure_equals(c.datafileRebuild[1], "2007/10-09.grib1");
	ensure_equals(c.summaryRebuildFile.size(), 0u);
	ensure_equals(c.summaryRebuildDir.size(), 0u);

	// Perform full maintenance and check that things are ok afterwards
	stringstream maintlog;
	MetadataCounter counter;
	FullMaintenance m(maintlog, counter);
	writer.maintenance(m);
	ensure_equals(counter.count, 3u);
	c.clear();
	counter.count = 0;
	writer.maintenance(c);
	ensure_equals(counter.count, 0u);
	ensure(c.isClean());

	size_t count = 0;
	count += countDeletedMetadata("testdir/2007/07-07.grib1.metadata");
	count += countDeletedMetadata("testdir/2007/07-08.grib1.metadata");
	count += countDeletedMetadata("testdir/2007/10-09.grib1.metadata");
	ensure_equals(count, 3u);
}

#endif

}

// vim:set ts=4 sw=4:
