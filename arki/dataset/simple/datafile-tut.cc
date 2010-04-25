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

#include <arki/dataset/test-utils.h>
#include <arki/dataset/simple/datafile.h>
#include <arki/utils/files.h>
#include <arki/metadata.h>
#include <arki/scan/grib.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <fstream>

#include <sys/stat.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::dataset::simple;
using namespace wibble;

static inline size_t filesize(const std::string& fname)
{
	return sys::fs::stat(fname)->st_size;
}
static inline size_t datasize(const Metadata& md)
{
	return md.source.upcast<types::source::Blob>()->size;
}

struct arki_dataset_simple_datafile_shar {
	string fname;
	string mdfname;
	string sumfname;

	arki_dataset_simple_datafile_shar()
		: fname("testfile.grib"),
		  mdfname("testfile.grib.metadata"),
		  sumfname("testfile.grib.summary")
	{
	}
};
TESTGRP(arki_dataset_simple_datafile);

// Try to append some data
template<> template<>
void to::test<1>()
{
	system(("rm -f " + fname).c_str());
	ino_t inomd;
	ino_t inosum;

	scan::Grib scanner;

	Metadata md;
	size_t totsize = 0;
	scanner.open("inbound/test.grib1");

	{
		Datafile df(fname);

		// It should exist but be empty
		ensure(sys::fs::access(fname, F_OK));
		ensure_equals(filesize(fname), 0u);

		// Get a metadata
		ensure(scanner.next(md));
		Item<types::Source> origSource = md.source;
		size_t size = datasize(md);

		// Append the data
		df.append(md);
		// The new data is there
		ensure_equals(filesize(fname), size);
		ensure_equals(md.source, Item<types::Source>(types::source::Blob::create("grib1", fname, 0, size)));
		// Metadata and summaries don't get touched
		ensure(!sys::fs::access(mdfname, F_OK));
		ensure(!sys::fs::access(sumfname, F_OK));

		totsize += size;

		// Get another metadata
		ensure(scanner.next(md));
		origSource = md.source;
		size = datasize(md);

		df.append(md);
		// The new data is there
		ensure_equals(filesize(fname), totsize + size);
		ensure_equals(md.source, Item<types::Source>(types::source::Blob::create("grib1", fname, totsize, size)));
		// Metadata and summaries don't get touched
		ensure(!sys::fs::access(mdfname, F_OK));
		ensure(!sys::fs::access(sumfname, F_OK));

		totsize += size;

		df.flush();
		// Metadata and summaries are now there
		ensure(sys::fs::access(mdfname, F_OK));
		ensure(sys::fs::access(sumfname, F_OK));
		inomd = utils::files::inode(mdfname);
		inosum = utils::files::inode(sumfname);

		// Get another metadata
		ensure(scanner.next(md));
		origSource = md.source;
		size = datasize(md);

		df.append(md);
		// The new data is there
		ensure_equals(filesize(fname), totsize + size);
		ensure_equals(md.source, Item<types::Source>(types::source::Blob::create("grib1", fname, totsize, size)));
		// Metadata and summaries don't get touched
		ensure_equals(utils::files::inode(mdfname), inomd);
		ensure_equals(utils::files::inode(sumfname), inosum);
	}

	// After Datafile is destroyed, metadata and summaries are flushed
	ensure(utils::files::inode(mdfname) != inomd);
	ensure(utils::files::inode(sumfname) != inosum);
}

// Test remove and pack
template<> template<>
void to::test<2>()
{
#if 0
	appendData();

	ensure(fs::access("testdatafile/testfile.grib1", F_OK));
	ensure(!fs::access("testdatafile/testfile.grib1.new", F_OK));
	ensure(fs::access("testdatafile/testfile.grib1.metadata", F_OK));
	ensure(!fs::access("testdatafile/testfile.grib1.metadata.new", F_OK));
	ensure(fs::access("testdatafile/testfile.grib1.summary", F_OK));
	ensure(!hasRebuildFlagfile("testdatafile/testfile.grib1"));
	ensure(!hasPackFlagfile("testdatafile/testfile.grib1"));

	size_t origsize = filesize("testdatafile/testfile.grib1");
	scan::Grib scanner;
	scanner.open("inbound/test.grib1");
	Metadata md;
	ensure(scanner.next(md));
	size_t firstsize = datasize(md);

	df->remove(0);

	ensure(fs::access("testdatafile/testfile.grib1", F_OK));
	ensure(!fs::access("testdatafile/testfile.grib1.new", F_OK));
	ensure(fs::access("testdatafile/testfile.grib1.metadata", F_OK));
	ensure(!fs::access("testdatafile/testfile.grib1.metadata.new", F_OK));
	ensure(!fs::access("testdatafile/testfile.grib1.summary", F_OK));
	ensure(hasRebuildFlagfile("testdatafile/testfile.grib1"));
	ensure(hasPackFlagfile("testdatafile/testfile.grib1"));

	flush();

	ensure(fs::access("testdatafile/testfile.grib1", F_OK));
	ensure(!fs::access("testdatafile/testfile.grib1.new", F_OK));
	ensure(fs::access("testdatafile/testfile.grib1.metadata", F_OK));
	ensure(!fs::access("testdatafile/testfile.grib1.metadata.new", F_OK));
	ensure(fs::access("testdatafile/testfile.grib1.summary", F_OK));
	ensure(!hasRebuildFlagfile("testdatafile/testfile.grib1"));
	ensure(hasPackFlagfile("testdatafile/testfile.grib1"));

	// The file size must be the same
	ensure_equals(filesize("testdatafile/testfile.grib1"), origsize);

	// Check what is in the metadata file
	vector<Metadata> allmd = Metadata::readFile("testdatafile/testfile.grib1.metadata");
	ensure_equals(allmd.size(), 3u);

	// The first metadata should be marked as deleted
	ensure(allmd[0].deleted);
	ensure(!allmd[1].deleted);
	ensure(!allmd[2].deleted);
#endif
}

}

// vim:set ts=4 sw=4:
