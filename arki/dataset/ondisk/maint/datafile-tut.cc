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
#include <arki/dataset/ondisk/maint/datafile.h>
#include <arki/dataset/ondisk/maint/directory.h>
#include <arki/dataset/ondisk/writer/directory.h>
#include <arki/dataset/ondisk/writer/datafile.h>
#include <arki/metadata.h>
#include <arki/scan/grib.h>
#include <arki/configfile.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <fstream>

#include <sys/stat.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::dataset::ondisk;
using namespace arki::dataset::ondisk::maint;
using namespace arki::utils::files;
using namespace wibble::sys;

static inline size_t filesize(const std::string& fname)
{
	return fs::stat(fname)->st_size;
}
static inline size_t datasize(const Metadata& md)
{
	return md.source.upcast<types::source::Blob>()->size;
}

struct TestWriter
{
	writer::RootDirectory rd;
	std::string fname;
	vector<Metadata> testmd;

	TestWriter(const ConfigFile& cfg, const std::string& fname)
		: rd(cfg), fname(fname)
	{
        scan::Grib scanner;
        scanner.open("inbound/test.grib1");
        Metadata md;
        while (scanner.next(md))
			testmd.push_back(md);
        ensure_equals(testmd.size(), 3u);
	}
	~TestWriter() {}

	void flush()
	{
		rd.flush();
	}
	void acquireAll()
	{
		writer::Datafile* df = rd.file(fname);
		for (MetadataCollector::iterator i = testmd.begin(); i != testmd.end(); ++i)
			df->append(*i);
	}
	void replace(size_t ofs, Metadata& md)
	{
		writer::Datafile* df = rd.file(fname);
		df->remove(ofs);
		df->append(md);
	}
	void remove(size_t ofs)
	{
		writer::Datafile* df = rd.file(fname);
		df->remove(ofs);
	}
};

struct arki_dataset_ondisk_maint_datafile_shar {
	RootDirectory* rootDir;
	Datafile* df;
	ConfigFile cfg;
	TestWriter* tw;

	arki_dataset_ondisk_maint_datafile_shar() : rootDir(0), df(0), tw(0)
	{
		cfg.setValue("path", "testdatafile");
		cfg.setValue("name", "testdir");
		cfg.setValue("step", "daily");

		system("rm -rf testdatafile");
		rootDir = new RootDirectory(cfg);
		df = new Datafile(rootDir, "testfile.grib1");
		tw = new TestWriter(cfg, "testfile.grib1");
	}

	~arki_dataset_ondisk_maint_datafile_shar()
	{
		if (rootDir) delete rootDir;
		if (df) delete df;
		if (tw) delete tw;
	}

	void acquireAll()
	{
		tw->acquireAll();
		tw->flush();
	}

	void acquireAllReplaceFirst()
	{
		tw->acquireAll();
		tw->flush();
		tw->replace(0, tw->testmd[1]);
		tw->flush();
	}

	void acquireAllRemoveFirst()
	{
		tw->acquireAll();
		tw->flush();
		tw->remove(0);
		tw->flush();
	}
};
TESTGRP(arki_dataset_ondisk_maint_datafile);

// Test replace and pack
template<> template<>
void to::test<1>()
{
	acquireAllReplaceFirst();
	size_t origsize = filesize("inbound/test.grib1");
	size_t firstsize = datasize(tw->testmd[0]);
	size_t secondsize = datasize(tw->testmd[1]);

	// Not really needed, but it's a good place to test countDeletedMetadata itself
	ensure_equals(countDeletedMetadata("testdatafile/testfile.grib1.metadata"), 1u);

	// Repack the file
	ensure_equals(df->repack(), 1u);

	ensure(fs::access("testdatafile/testfile.grib1", F_OK));
	ensure(!fs::access("testdatafile/testfile.grib1.new", F_OK));
	ensure(fs::access("testdatafile/testfile.grib1.metadata", F_OK));
	ensure(!fs::access("testdatafile/testfile.grib1.metadata.new", F_OK));
	ensure(fs::access("testdatafile/testfile.grib1.summary", F_OK));
	ensure(!hasRebuildFlagfile("testdatafile/testfile.grib1"));
	ensure(!hasPackFlagfile("testdatafile/testfile.grib1"));

	// The file must have shrunk
	ensure_equals(filesize("testdatafile/testfile.grib1"), origsize - firstsize + secondsize);

	// There should be only 3 metadata now
	vector<Metadata> allmd = Metadata::readFile("testdatafile/testfile.grib1.metadata");
	ensure_equals(allmd.size(), 3u);
	ensure(!allmd[0].deleted);
	ensure(!allmd[1].deleted);
	ensure(!allmd[2].deleted);
}

// Test remove and pack
template<> template<>
void to::test<2>()
{
	acquireAllRemoveFirst();

	size_t origsize = filesize("testdatafile/testfile.grib1");
	size_t firstsize = datasize(tw->testmd[0]);

	// Not really needed, but it's a good place to test countDeletedMetadata itself
	ensure_equals(countDeletedMetadata("testdatafile/testfile.grib1.metadata"), 1u);

	ensure_equals(df->repack(), 1u);

	ensure(fs::access("testdatafile/testfile.grib1", F_OK));
	ensure(!fs::access("testdatafile/testfile.grib1.new", F_OK));
	ensure(fs::access("testdatafile/testfile.grib1.metadata", F_OK));
	ensure(!fs::access("testdatafile/testfile.grib1.metadata.new", F_OK));
	ensure(fs::access("testdatafile/testfile.grib1.summary", F_OK));
	ensure(!hasRebuildFlagfile("testdatafile/testfile.grib1"));
	ensure(!hasPackFlagfile("testdatafile/testfile.grib1"));

	// The file must have shrunk
	ensure_equals(filesize("testdatafile/testfile.grib1"), origsize - firstsize);

	// There should be only 2 metadata now
	vector<Metadata> allmd = Metadata::readFile("testdatafile/testfile.grib1.metadata");
	ensure_equals(allmd.size(), 2u);
	ensure(!allmd[0].deleted);
	ensure(!allmd[1].deleted);
}

// Test rebuild
template<> template<>
void to::test<3>()
{
	system("cp inbound/test.grib1 testdatafile/testfile.grib1");
	createRebuildFlagfile("testdatafile/testfile.grib1");
	df->rebuild(false);

	ensure(utils::hasFlagfile("testdatafile/testfile.grib1"));
	ensure(!utils::hasFlagfile("testdatafile/testfile.grib1.new"));
	ensure(utils::hasFlagfile("testdatafile/testfile.grib1.metadata"));
	ensure(!utils::hasFlagfile("testdatafile/testfile.grib1.metadata.new"));
	ensure(utils::hasFlagfile("testdatafile/testfile.grib1.summary"));
	ensure(!hasRebuildFlagfile("testdatafile/testfile.grib1"));
	ensure(!hasPackFlagfile("testdatafile/testfile.grib1"));
}

#if 0
// Test datafile rebuild on non-indexed directory
template<> template<>
void to::test<1>()
{
	RootDirectory root(cfg);
	system("cp inbound/test.grib1 testdir/");
	createRebuildFlagfile("testdir/test.grib1");

	Datafile df(&root, "test.grib1");
	MetadataCounter counter;
	df.rebuild(counter, false);
	root.rebuildSummary();
	ensure(!hasIndexFlagfile("testdir"));
	ensure_equals(counter.count, 0u);

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/test.grib1"));
	ensure(!hasPackFlagfile("testdir/test.grib1"));
	ensure(utils::hasFlagfile("testdir/test.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/test.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(!utils::hasFlagfile("testdir/index.sqlite"));

	// Test querying
	Reader reader(cfg);
	ensure(!reader.hasWorkingIndex());
	MetadataCollector mdc;
	reader.query(Matcher::parse("origin:GRIB1,200"), false, mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/test.grib1"));
	ensure_equals(blob->offset, 0u);
}

// Test datafile rebuild on indexed directory
template<> template<>
void to::test<2>()
{
	IndexedRootDirectory root(cfg);
	system("cp inbound/test.grib1 testdir/");
	createRebuildFlagfile("testdir/test.grib1");

	Datafile df(&root, "test.grib1");
	MetadataCounter counter;
	df.rebuild(counter, true);
	ensure(hasIndexFlagfile("testdir"));
	ensure_equals(counter.count, 0u);

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/test.grib1"));
	ensure(!hasPackFlagfile("testdir/test.grib1"));
	ensure(utils::hasFlagfile("testdir/test.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/test.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(utils::hasFlagfile("testdir/index.sqlite"));

	// Test querying
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	MetadataCollector mdc;
	reader.query(Matcher::parse("origin:GRIB1,200"), false, mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/test.grib1"));
	ensure_equals(blob->offset, 0u);
}

// Test datafile rebuild on nonindexed directory, with duplicates
template<> template<>
void to::test<3>()
{
	RootDirectory root(cfg);
	system("cat inbound/test.grib1 inbound/test.grib1 > testdir/test.grib1");
	createRebuildFlagfile("testdir/test.grib1");

	Datafile df(&root, "test.grib1");
	MetadataCounter counter;
	df.rebuild(counter, false);
	ensure(!hasIndexFlagfile("testdir"));
	ensure_equals(counter.count, 0u);

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/test.grib1"));
	ensure(!hasPackFlagfile("testdir/test.grib1"));
	ensure(utils::hasFlagfile("testdir/test.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/test.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(!utils::hasFlagfile("testdir/index.sqlite"));

	// Test querying
	Reader reader(cfg);
	ensure(!reader.hasWorkingIndex());
	MetadataCollector mdc;
	reader.query(Matcher::parse("origin:GRIB1,200"), false, mdc);
	ensure_equals(mdc.size(), 2u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/test.grib1"));
	// Having no index, the first matching metadata is at the beginning
	ensure_equals(blob->offset, 0u);
}

// Test datafile rebuild on indexed directory, with duplicates
template<> template<>
void to::test<4>()
{
	IndexedRootDirectory root(cfg);
	system("cat inbound/test.grib1 inbound/test.grib1 > testdir/test.grib1");
	createRebuildFlagfile("testdir/test.grib1");

	Datafile df(&root, "test.grib1");
	MetadataCounter counter;
	df.rebuild(counter, true);
	root.rebuildSummary();
	ensure(hasIndexFlagfile("testdir"));
	ensure_equals(counter.count, 3u);

	ensure_equals(countDeletedMetadata("testdir/test.grib1.metadata"), 3u);

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/test.grib1"));
	ensure(hasPackFlagfile("testdir/test.grib1"));
	ensure(utils::hasFlagfile("testdir/test.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/test.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(utils::hasFlagfile("testdir/index.sqlite"));

	// Test querying
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	MetadataCollector mdc;
	reader.query(Matcher::parse("origin:GRIB1,200"), false, mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/test.grib1"));
	// Make sure the resulting metadata points at the first duplicate element
	ensure_equals(blob->offset, 0u);
}
#endif

}

// vim:set ts=4 sw=4:
