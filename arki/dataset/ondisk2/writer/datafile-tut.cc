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
#include <arki/dataset/ondisk2/writer/datafile.h>
#include <arki/metadata.h>
#include <arki/scan/grib.h>
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
using namespace arki::utils;
using namespace arki::dataset::ondisk2;
using namespace arki::dataset::ondisk2::writer;
using namespace wibble::sys;

namespace {
inline size_t datasize(const Metadata& md)
{
	return md.source.upcast<types::source::Blob>()->size;
}
}

struct arki_dataset_ondisk2_writer_datafile_shar {
	string fname;

	arki_dataset_ondisk2_writer_datafile_shar()
		: fname("testfile.grib")
	{
		system(("rm -f " + fname).c_str());
	}

#if 0
	void appendData()
	{
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");
		Metadata md;
		size_t count = 0;
		while (scanner.next(md))
		{
			df->append(md);
			++count;
		}
		ensure_equals(count, 3u);
		flush();
	}

	void flush()
	{
		rootDir->flush();
		df = rootDir->file("testfile.grib1");
	}
#endif
};
TESTGRP(arki_dataset_ondisk2_writer_datafile);

// Try to append some data
template<> template<>
void to::test<1>()
{
	scan::Grib scanner;

	Metadata md;
	size_t totsize = 0;
	off_t ofs;
	scanner.open("inbound/test.grib1");

	Datafile df(fname);

	// It should exist but be empty
	ensure(files::exists(fname));
	ensure_equals(files::size(fname), 0u);

	// Get a metadata
	ensure(scanner.next(md));
	Item<types::Source> origSource = md.source;
	size_t size = datasize(md);

	{
		// Start the append transaction, nothing happens until commit
		Pending p = df.append(md, &ofs);
		ensure_equals(ofs, 0u);
		ensure_equals(files::size(fname), 0u);
		ensure_equals(md.source, origSource);

		// After commit, data is appended
		p.commit();
		ensure_equals(files::size(fname), size);
		ensure_equals(md.source, Item<types::Source>(types::source::Blob::create("grib1", fname, 0, size)));
	}

	totsize += size;

	// Get another metadata
	ensure(scanner.next(md));
	origSource = md.source;
	size = datasize(md);

	{
		// Start the append transaction, nothing happens until commit
		Pending p = df.append(md, &ofs);
		ensure_equals(ofs, (off_t)totsize);
		ensure_equals(files::size(fname), totsize);
		ensure_equals(md.source, origSource);

		// After rollback, nothing has changed
		p.rollback();
		ensure_equals(files::size(fname), totsize);
		ensure_equals(md.source, origSource);
	}

	// Get another metadata
	ensure(scanner.next(md));
	origSource = md.source;
	size = datasize(md);

	{
		// Start the append transaction, nothing happens until commit
		Pending p = df.append(md, &ofs);
		ensure_equals(ofs, totsize);
		ensure_equals(files::size(fname), totsize);
		ensure_equals(md.source, origSource);

		// After commit, data is appended
		p.commit();
		ensure_equals(files::size(fname), totsize + size);
		ensure_equals(md.source, Item<types::Source>(types::source::Blob::create("grib1", fname, totsize, size)));
	}

	// When pending is out of scope, everything is normal
	ensure_equals(files::size(fname), totsize + size);
	ensure_equals(md.source, Item<types::Source>(types::source::Blob::create("grib1", fname, totsize, size)));
}

// Test replace
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

	size_t origsize = files::size("testdatafile/testfile.grib1");

	// Get the second grib
	scan::Grib scanner;
	scanner.open("inbound/test.grib1");
	Metadata md;
	ensure(scanner.next(md));
	size_t firstsize = datasize(md);
	ensure(scanner.next(md));
	size_t size = datasize(md);

	// Replace the first with another copy of the second
	df->remove(0);
	df->append(md);

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

	// The file must have grown with the new data
	ensure_equals(files::size("testdatafile/testfile.grib1"), origsize + size);

	// Check what is in the metadata file
	vector<Metadata> allmd = Metadata::readFile("testdatafile/testfile.grib1.metadata");
	ensure_equals(allmd.size(), 4u);

	// The first metadata should be marked as deleted
	ensure(allmd[0].deleted);
	ensure(!allmd[1].deleted);
	ensure(!allmd[2].deleted);
	ensure(!allmd[3].deleted);

	// Not really needed, but it's a good place to test countDeletedMetadata itself
	ensure_equals(countDeletedMetadata("testdatafile/testfile.grib1.metadata"), 1u);
#endif
}

// Test remove and pack
template<> template<>
void to::test<3>()
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

	size_t origsize = files::size("testdatafile/testfile.grib1");
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
	ensure_equals(files::size("testdatafile/testfile.grib1"), origsize);

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
