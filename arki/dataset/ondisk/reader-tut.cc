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

#include <arki/tests/test-utils.h>
#include <arki/dataset/ondisk.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/scan/grib.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>
#include <wibble/stream/posix.h>

#include <unistd.h>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::dataset::ondisk;
using namespace arki::utils::files;

struct MetadataCollector : public vector<Metadata>, public MetadataConsumer
{
	bool operator()(Metadata& md)
	{
		push_back(md);
		return true;
	}
};

struct arki_dataset_ondisk_reader_shar {
	ConfigFile config;

	arki_dataset_ondisk_reader_shar()
	{
		// Cleanup the test datasets
		system("rm -rf testds/*");

		// In-memory dataset configuration
		string conf =
			"[testds]\n"
			"type = test\n"
			"step = daily\n"
			"filter = origin: GRIB1,200\n"
			"index = origin, reftime\n"
			"name = testds\n"
			"path = testds\n"
			"postprocess = testcountbytes\n";
		stringstream incfg(conf);
		config.parse(incfg, "(memory)");

		// Import all data from test.grib1
		Metadata md;
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

		dataset::ondisk::Writer testds(*config.section("testds"));
		size_t count = 0;
		while (scanner.next(md))
		{
			ensure(testds.acquire(md) == WritableDataset::ACQ_OK);
			++count;
		}
		ensure_equals(count, 3u);
		testds.flush();
	}
};
TESTGRP(arki_dataset_ondisk_reader);

// Test querying the datasets
template<> template<>
void to::test<1>()
{
	ondisk::Reader testds(*config.section("testds"));
	MetadataCollector mdc;

	ensure(testds.hasWorkingIndex());

	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::BLOB);
	ensure_equals(source->format, "grib1");
	UItem<source::Blob> blob = source.upcast<source::Blob>();
	ensure_equals(blob->filename, sys::fs::abspath("testds/2007/07-08.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 7218u);

	mdc.clear();
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), false), mdc);
	ensure_equals(mdc.size(), 1u);
}

// Ensure that rebuild datafiles are not read when using the index
template<> template<>
void to::test<2>()
{
	createRebuildFlagfile("testds/2007/07-08.grib1");
	ondisk::Reader testds(*config.section("testds"));
	ensure(testds.hasWorkingIndex());

	// If the last update looks incomplete, the data will be skipped
	MetadataCollector mdc;
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 0u);
}

// Ensure that if the index does not exists, it does not try to use it
template<> template<>
void to::test<3>()
{
	system("rm testds/index.sqlite");
	ondisk::Reader testds(*config.section("testds"));

	// There should be no working index
	ensure(!testds.hasWorkingIndex());

	// However, we should still get good results
	MetadataCollector mdc;
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::BLOB);
	ensure_equals(source->format, "grib1");
	UItem<source::Blob> blob = source.upcast<source::Blob>();
	ensure_equals(blob->filename, sys::fs::abspath("testds/2007/07-08.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 7218u);
}

// Ensure that if the index flagfile exists, the index is not used
template<> template<>
void to::test<4>()
{
	createIndexFlagfile("testds");
	ondisk::Reader testds(*config.section("testds"));

	// There should be no working index
	ensure(!testds.hasWorkingIndex());

	// However, we should still get good results
	MetadataCollector mdc;
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::BLOB);
	ensure_equals(source->format, "grib1");
	UItem<source::Blob> blob = source.upcast<source::Blob>();
	ensure_equals(blob->filename, sys::fs::abspath("testds/2007/07-08.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 7218u);
}

// Ensure that rebuild datafiles are not read when not using the index
template<> template<>
void to::test<5>()
{
	createIndexFlagfile("testds");
	createRebuildFlagfile("testds/2007/07-08.grib1");
	ondisk::Reader testds(*config.section("testds"));
	ensure(!testds.hasWorkingIndex());

	// If the last update looks incomplete, the data will be skipped
	MetadataCollector mdc;
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 0u);
}

// Test querying with postprocessing
template<> template<>
void to::test<6>()
{
	ondisk::Reader testds(*config.section("testds"));
	ensure(testds.hasWorkingIndex());

	// Use dup() because PosixBuf will close its file descriptor at destruction
	// time
	stream::PosixBuf pb(dup(2));
	ostream os(&pb);
	dataset::ByteQuery bq;
	bq.setPostprocess(Matcher::parse("origin:GRIB1,200"), "testcountbytes");
	testds.queryBytes(bq, os);

	string out = utils::readFile("testcountbytes.out");
	ensure_equals(out, "7540\n");
}

// Test querying with data only
template<> template<>
void to::test<7>()
{
	ondisk::Reader testds(*config.section("testds"));
	ensure(testds.hasWorkingIndex());

	std::stringstream os;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse("origin:GRIB1,200"));
	testds.queryBytes(bq, os);

	ensure_equals(os.str().substr(0, 4), "GRIB");
}

// Test querying with inline data
template<> template<>
void to::test<8>()
{
	ondisk::Reader testds(*config.section("testds"));
	MetadataCollector mdc;

	ensure(testds.hasWorkingIndex());

	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::INLINE);
	ensure_equals(source->format, "grib1");
	UItem<source::Inline> isource = source.upcast<source::Inline>();
	ensure_equals(isource->size, 7218u);

	wibble::sys::Buffer buf = mdc[0].getData();
	ensure_equals(buf.size(), isource->size);

	mdc.clear();
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), true), mdc);
	ensure_equals(mdc.size(), 1u);
}

}

// vim:set ts=4 sw=4:
