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

#include <arki/dataset/tests.h>
#include <arki/dataset/ondisk/maint/directory.h>
#include <arki/dataset/ondisk/maint/datafile.h>
#include <arki/dataset/ondisk/reader.h>
#include <arki/dataset/ondisk/writer.h>
#include <arki/dataset/ondisk/maintenance.h>
#include <arki/metadata.h>
#include <arki/configfile.h>
#include <arki/scan/grib.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <algorithm>
#include <cstring>

namespace std {

ostream& operator<<(ostream& o, const vector<string>& vs)
{
	for (vector<string>::const_iterator i = vs.begin(); i != vs.end(); ++i)
		o << *i << endl;
	return o;
}

}

namespace arki {
namespace dataset {
namespace ondisk {
namespace maint {

struct CollectorAgent : public MaintenanceAgent
{
	std::string writerPath;
	bool hasEnded, fullIndex;
	std::vector<std::string> datafileRebuild, summaryRebuildFile, reindexFile, summaryRebuildDir;

	CollectorAgent() :
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

}
}
}
}

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset::ondisk;
using namespace arki::dataset::ondisk::maint;
using namespace arki::utils::files;

struct arki_dataset_ondisk_maintenance_shar {
	ConfigFile cfg;

	arki_dataset_ondisk_maintenance_shar()
	{
		system("rm -rf testdir");

		cfg.setValue("path", "testdir");
		cfg.setValue("name", "test");
		cfg.setValue("step", "daily");
		cfg.setValue("index", "origin, reftime");
		cfg.setValue("unique", "origin, reftime");
	}

	void acquireSamples()
	{
		Metadata md;
		scan::Grib scanner;
		dataset::ondisk::Writer writer(cfg);
		scanner.open("inbound/test.grib1");
		size_t count = 0;
		for ( ; scanner.next(md); ++count)
			ensure_equals(writer.acquire(md), WritableDataset::ACQ_OK);
		ensure_equals(count, 3u);
		writer.flush();
	}
};
TESTGRP(arki_dataset_ondisk_maintenance);

// Test accuracy of maintenance scan, without index, on perfect dataset
template<> template<>
void to::test<1>()
{
	cfg.setValue("index", string());
	cfg.setValue("unique", string());
	acquireSamples();

	Writer writer(cfg);
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
	ensure(c.isClean());
}

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
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
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
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
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
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
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
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
	ensure(c.isClean());
}


// Test accuracy of maintenance scan, with index, on perfect dataset
template<> template<>
void to::test<6>()
{
	acquireSamples();

	Writer writer(cfg);
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
	ensure(c.isClean());
}

// Test accuracy of maintenance scan, with index, with index-out-of-date flagfile
template<> template<>
void to::test<7>()
{
	acquireSamples();
	createNewIndexFlagfile("testdir");

	Writer writer(cfg);
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
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
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
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
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
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
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
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
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
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
	FullMaintenance m(maintlog);
	//FullMaintenance m(cerr);
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
	utils::metadata::Collector mdc;
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 7218u);

	// Query the second element and check that it starts after the first one
	// (there used to be a bug where the rebuild would use the offsets of
	// the metadata instead of the data)
	mdc.clear();
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
	ensure_equals(blob->offset, 7218u);
	ensure_equals(blob->size, 34960u);
}

// Test invalidating and rebuilding a dataset
template<> template<>
void to::test<13>()
{
	acquireSamples();
	system("touch -d yesterday testdir/2007/*");
	system("touch -d yesterday testdir/*");

	Writer writer(cfg);
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	writer.maintenance(m);
	c.clear();
	writer.maintenance(c);
	ensure(c.isClean());
}

// Test recreating a dataset from just a datafile with duplicate data and a rebuild flagfile
template<> template<>
void to::test<14>()
{
	system("mkdir testdir");
	system("mkdir testdir/foo");
	system("mkdir testdir/foo/bar");
	system("cat inbound/test.grib1 inbound/test.grib1 > testdir/foo/bar/test.grib1");
	createNewRebuildFlagfile("testdir/foo/bar/test.grib1");

	Writer writer(cfg);
	stringstream maintlog;
	FullMaintenance m(maintlog);
	//FullMaintenance m(cerr);
	writer.maintenance(m);

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/foo/bar/test.grib1"));
	ensure(hasPackFlagfile("testdir/foo/bar/test.grib1"));
	ensure(utils::hasFlagfile("testdir/foo/bar/test.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/foo/bar/test.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/foo/bar/summary"));
	ensure(utils::hasFlagfile("testdir/foo/summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(utils::hasFlagfile("testdir/index.sqlite"));

	// Test querying
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	utils::metadata::Collector mdc;
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
	ensure_equals(blob->offset, 44412u);
	ensure_equals(blob->size, 7218u);

	// Query the second element and check that it starts after the first one
	// (there used to be a bug where the rebuild would use the offsets of
	// the metadata instead of the data)
	mdc.clear();
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/foo/bar/test.grib1"));
	ensure_equals(blob->offset, 51630u);
	ensure_equals(blob->size, 34960u);
}

// Test accuracy of maintenance scan, with index, on dataset with some
// rebuild flagfiles, and duplicate items inside
template<> template<>
void to::test<15>()
{
	acquireSamples();
	createNewRebuildFlagfile("testdir/2007/07-08.grib1");
	createNewRebuildFlagfile("testdir/2007/10-09.grib1");
	system("cat inbound/test.grib1 >> testdir/2007/07-08.grib1");

	Writer writer(cfg);
	CollectorAgent c;
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
	FullMaintenance m(maintlog);
	// By catting test.grib1 into 07-08.grib1, we create 2 metadata that do
	// not fit in that file (1 does).
	// Because they are duplicates of metadata in other files, one cannot
	// use the order of the data in the file to determine which one is the
	// newest. The situation cannot be fixed automatically because it is
	// impossible to determine which of the two duplicates should be thrown
	// away; therefore, we can only interrupt the maintenance and raise an
	// exception calling for manual fixing.
	try {
		writer.maintenance(m);
		ensure(false);
	} catch (wibble::exception::Consistency& ce) {
		ensure(true);
	} catch (...) {
		ensure(false);
	}
}

// Test repack, with index
template<> template<>
void to::test<16>()
{
	// Allow replacing duplicate values
	cfg.setValue("replace", "yes");
	// Acquire twice to create deleted elements
	acquireSamples();
	acquireSamples();

	off_t sz1 = utils::files::size("testdir/2007/07-07.grib1");
	off_t sz2 = utils::files::size("testdir/2007/07-08.grib1");
	off_t sz3 = utils::files::size("testdir/2007/10-09.grib1");

	ensure(!utils::files::hasIndexFlagfile("testdir"));
	ensure(!utils::files::hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(!utils::files::hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(!utils::files::hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(utils::files::hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(utils::files::hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(utils::files::hasPackFlagfile("testdir/2007/10-09.grib1"));

	stringstream out;
	{
		Writer writer(cfg);
		writer.repack(out, true);
		writer.flush();
	}

	ensure(!utils::files::hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(!utils::files::hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(!utils::files::hasPackFlagfile("testdir/2007/10-09.grib1"));
	ensure(!utils::files::hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(!utils::files::hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(!utils::files::hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(!utils::files::hasIndexFlagfile("testdir"));

	off_t sz1a = utils::files::size("testdir/2007/07-07.grib1");
	off_t sz2a = utils::files::size("testdir/2007/07-08.grib1");
	off_t sz3a = utils::files::size("testdir/2007/10-09.grib1");

	ensure(sz1a > 0);
	ensure(sz2a > 0);
	ensure(sz3a > 0);
	ensure(sz1a < sz1);
	ensure(sz2a < sz2);
	ensure(sz3a < sz3);

	// Test querying
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	utils::metadata::Collector mdc;
	// Use reftime in the query to force use of the index
	reader.queryData(dataset::DataQuery(Matcher::parse("reftime:>=2000"), true), mdc);
	ensure_equals(mdc.size(), 3u);

	UItem<source::Inline> blob = mdc[0].source.upcast<source::Inline>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->size, 34960u);
	wibble::sys::Buffer buf = mdc[0].getData();
	ensure_equals(memcmp(buf.data(), "GRIB", 4), 0);
	ensure_equals(memcmp((const char*)buf.data() + 34956, "7777", 4), 0);

	blob = mdc[1].source.upcast<source::Inline>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->size, 7218u);
	buf = mdc[1].getData();
	ensure_equals(memcmp(buf.data(), "GRIB", 4), 0);
	ensure_equals(memcmp((const char*)buf.data() + 7214, "7777", 4), 0);

	blob = mdc[2].source.upcast<source::Inline>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->size, 2234u);
	buf = mdc[2].getData();
	ensure_equals(memcmp(buf.data(), "GRIB", 4), 0);
	ensure_equals(memcmp((const char*)buf.data() + 2230, "7777", 4), 0);
}

}

// vim:set ts=4 sw=4:
