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
#include <arki/dataset/ondisk/maint/directory.h>
#include <arki/dataset/ondisk/maint/datafile.h>
#include <arki/dataset/ondisk/reader.h>
#include <arki/dataset/ondisk/writer.h>
#include <arki/scan/grib.h>
#include <arki/metadata.h>
#include <arki/configfile.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset::ondisk;
using namespace arki::dataset::ondisk::maint;
using namespace arki::utils::files;

struct arki_dataset_ondisk_maint_directory_shar {
	ConfigFile cfg;

	arki_dataset_ondisk_maint_directory_shar()
	{
		system("rm -rf testdir");

		cfg.setValue("path", "testdir");
		cfg.setValue("name", "test");
		cfg.setValue("step", "daily");
		cfg.setValue("index", "origin, reftime");
		cfg.setValue("unique", "origin, reftime");
	}

	void acquire()
	{
		acquire(cfg);
	}

	void acquire(const ConfigFile& cfg)
	{
        scan::Grib scanner;
        scanner.open("inbound/test.grib1");
        Metadata md;

		Writer w(cfg);
        while (scanner.next(md))
			w.acquire(md);

		w.flush();
	}
};
TESTGRP(arki_dataset_ondisk_maint_directory);

// Test deleteSummaries and rebuildSummary, with index
template<> template<>
void to::test<1>()
{
	acquire();

	IndexedRootDirectory root(cfg);

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/10-09.grib1"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(utils::hasFlagfile("testdir/index.sqlite"));

	root.deleteSummaries();

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/10-09.grib1"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.metadata"));
	ensure(!utils::hasFlagfile("testdir/2007/07-07.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/07-08.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/10-09.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/summary"));
	ensure(!utils::hasFlagfile("testdir/summary"));
	ensure(utils::hasFlagfile("testdir/index.sqlite"));

	// This does not work recursively, but we do not have subdirectories
	SubDirectory sd(&root, "2007");
	Datafile(&sd, "07-07.grib1").rebuildSummary();
	Datafile(&sd, "07-08.grib1").rebuildSummary();
	Datafile(&sd, "10-09.grib1").rebuildSummary();
	sd.rebuildSummary();
	root.rebuildSummary();
	
	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/10-09.grib1"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(utils::hasFlagfile("testdir/index.sqlite"));

	// Test querying
	Reader reader(cfg);
	ensure(reader.hasWorkingIndex());
	MetadataCollector mdc;
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/2007/07-08.grib1"));
	ensure_equals(blob->offset, 0u);
}

// Test deleteSummaries and rebuildSummary, without index
template<> template<>
void to::test<2>()
{
	cfg.setValue("index", "");
	cfg.setValue("unique", "");

	acquire();

	RootDirectory root(cfg);

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/10-09.grib1"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(!utils::hasFlagfile("testdir/index.sqlite"));

	root.deleteSummaries();

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/10-09.grib1"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.metadata"));
	ensure(!utils::hasFlagfile("testdir/2007/07-07.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/07-08.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/10-09.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/summary"));
	ensure(!utils::hasFlagfile("testdir/summary"));
	ensure(!utils::hasFlagfile("testdir/index.sqlite"));

	// This does not work recursively, but we do not have subdirectories
	SubDirectory sd(&root, "2007");
	Datafile(&sd, "07-07.grib1").rebuildSummary();
	Datafile(&sd, "07-08.grib1").rebuildSummary();
	Datafile(&sd, "10-09.grib1").rebuildSummary();
	sd.rebuildSummary();
	root.rebuildSummary();
	
	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/10-09.grib1"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(!utils::hasFlagfile("testdir/index.sqlite"));

	// Test querying
	Reader reader(cfg);
	ensure(!reader.hasWorkingIndex());
	MetadataCollector mdc;
	reader.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->format, "grib1"); 
	ensure_equals(blob->filename, sys::fs::abspath("testdir/2007/07-08.grib1"));
	ensure_equals(blob->offset, 0u);
}

// Test invalidateAll, with index
template<> template<>
void to::test<3>()
{
	acquire();

	IndexedRootDirectory root(cfg);

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/10-09.grib1"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(utils::hasFlagfile("testdir/index.sqlite"));

	root.invalidateAll();

	ensure(hasIndexFlagfile("testdir"));
	ensure(hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/10-09.grib1"));
	ensure(!utils::hasFlagfile("testdir/2007/07-07.grib1.metadata"));
	ensure(!utils::hasFlagfile("testdir/2007/07-08.grib1.metadata"));
	ensure(!utils::hasFlagfile("testdir/2007/10-09.grib1.metadata"));
	ensure(!utils::hasFlagfile("testdir/2007/07-07.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/07-08.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/10-09.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/summary"));
	ensure(!utils::hasFlagfile("testdir/summary"));
	ensure(utils::hasFlagfile("testdir/index.sqlite"));
}

// Test invalidateAll, without index
template<> template<>
void to::test<4>()
{
	cfg.setValue("index", "");
	cfg.setValue("unique", "");

	acquire();

	RootDirectory root(cfg);

	ensure(!hasIndexFlagfile("testdir"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/10-09.grib1"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.metadata"));
	ensure(utils::hasFlagfile("testdir/2007/07-07.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/07-08.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/10-09.grib1.summary"));
	ensure(utils::hasFlagfile("testdir/2007/summary"));
	ensure(utils::hasFlagfile("testdir/summary"));
	ensure(!utils::hasFlagfile("testdir/index.sqlite"));

	root.invalidateAll();

	ensure(!hasIndexFlagfile("testdir"));
	ensure(hasRebuildFlagfile("testdir/2007/07-07.grib1"));
	ensure(hasRebuildFlagfile("testdir/2007/07-08.grib1"));
	ensure(hasRebuildFlagfile("testdir/2007/10-09.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-07.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/07-08.grib1"));
	ensure(!hasPackFlagfile("testdir/2007/10-09.grib1"));
	ensure(!utils::hasFlagfile("testdir/2007/07-07.grib1.metadata"));
	ensure(!utils::hasFlagfile("testdir/2007/07-08.grib1.metadata"));
	ensure(!utils::hasFlagfile("testdir/2007/10-09.grib1.metadata"));
	ensure(!utils::hasFlagfile("testdir/2007/07-07.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/07-08.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/10-09.grib1.summary"));
	ensure(!utils::hasFlagfile("testdir/2007/summary"));
	ensure(!utils::hasFlagfile("testdir/summary"));
	ensure(!utils::hasFlagfile("testdir/index.sqlite"));
}

}

// vim:set ts=4 sw=4:
