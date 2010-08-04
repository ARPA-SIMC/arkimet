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

#include <arki/tests/test-utils.h>
#include <arki/metadata/collection.h>
#include <arki/utils.h>
#include <arki/utils/accounting.h>
#include <arki/types/source.h>
#include <arki/summary.h>
#include <arki/scan/any.h>
#include <arki/runtime/io.h>
#include <wibble/sys/fs.h>
#include <cstring>
#include <config.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::metadata;

struct arki_metadata_collection_shar {
	Collection c;

	arki_metadata_collection_shar()
	{
	}

	void acquireSamples()
	{
		scan::scan("inbound/test.grib1", c);
	}
};
TESTGRP(arki_metadata_collection);

// Test querying
template<> template<>
void to::test<1>()
{
	acquireSamples();

	Collection mdc;

	c.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::BLOB);
	ensure_equals(source->format, "grib1");
	UItem<source::Blob> blob = source.upcast<source::Blob>();
	ensure_equals(blob->filename, sys::fs::abspath("inbound/test.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 7218u);

	mdc.clear();

	c.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	source = mdc[0].source;
	ensure_equals(source->style(), Source::BLOB);
	ensure_equals(source->format, "grib1");
	blob = source.upcast<source::Blob>();
	ensure_equals(blob->filename, sys::fs::abspath("inbound/test.grib1"));
	ensure_equals(blob->offset, 7218u);
	ensure_equals(blob->size, 34960u);

	mdc.clear();
	c.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), false), mdc);
	ensure_equals(mdc.size(), 1u);
	source = mdc[0].source;
	ensure_equals(source->style(), Source::BLOB);
	ensure_equals(source->format, "grib1");
	blob = source.upcast<source::Blob>();
	ensure_equals(blob->filename, sys::fs::abspath("inbound/test.grib1"));
	ensure_equals(blob->offset, 42178u);
	ensure_equals(blob->size, 2234u);
}

// Test querying the summary
template<> template<>
void to::test<2>()
{
	acquireSamples();
	Summary summary;
	c.querySummary(Matcher::parse("origin:GRIB1,200"), summary);
	ensure_equals(summary.count(), 1u);
}

// Test querying the summary by reftime
template<> template<>
void to::test<3>()
{
	acquireSamples();
	Summary summary;
	//system("bash");
	c.querySummary(Matcher::parse("reftime:>=2007-07"), summary);
	ensure_equals(summary.count(), 3u);
}

// Test compression
template<> template<>
void to::test<4>()
{
#ifdef HAVE_DBALLE
	using namespace utils;

	static const int repeats = 1024;

	// Create a test file with `repeats` BUFR entries
	std::string bufr = utils::readFile("inbound/test.bufr");
	ensure(bufr.size() > 0);
	bufr = bufr.substr(0, 194);

	runtime::Tempfile tf(".");
	tf.unlink_on_exit(false);
	for (int i = 0; i < repeats; ++i)
		tf.stream().write(bufr.data(), bufr.size());
	tf.stream().flush();

	// Create metadata for the big BUFR file
	scan::scan(tf.name(), "bufr", c);
	ensure_equals(c.size(), (size_t)repeats);

	// Compress the data file
	c.compressDataFile(127, "temp BUFR " + tf.name());
	// Remove the original file
	tf.unlink();
	Metadata::flushDataReaders();
	for (Collection::iterator i = c.begin(); i != c.end(); ++i)
		i->dropCachedData();

	// Ensure that all data can still be read
	utils::acct::gzip_data_read_count.reset();
	utils::acct::gzip_forward_seek_bytes.reset();
	utils::acct::gzip_idx_reposition_count.reset();
	for (int i = 0; i < repeats; ++i)
	{
		sys::Buffer b = c[i].getData();
		ensure_equals(b.size(), bufr.size());
		ensure(memcmp(b.data(), bufr.data(), bufr.size()) == 0);
	}
	// We read linearly, so there are no seeks or repositions
	ensure_equals(utils::acct::gzip_data_read_count.val(), (size_t)repeats);
	ensure_equals(utils::acct::gzip_forward_seek_bytes.val(), 0u);
	ensure_equals(utils::acct::gzip_idx_reposition_count.val(), 1u);

	Metadata::flushDataReaders();
	for (Collection::iterator i = c.begin(); i != c.end(); ++i)
		i->dropCachedData();

	// Try to read backwards to avoid sequential reads
	utils::acct::gzip_data_read_count.reset();
	utils::acct::gzip_forward_seek_bytes.reset();
	utils::acct::gzip_idx_reposition_count.reset();
	for (int i = repeats-1; i >= 0; --i)
	{
		sys::Buffer b = c[i].getData();
		ensure_equals(b.size(), bufr.size());
		ensure(memcmp(b.data(), bufr.data(), bufr.size()) == 0);
	}
	ensure_equals(utils::acct::gzip_data_read_count.val(), (size_t)repeats);
	ensure_equals(utils::acct::gzip_forward_seek_bytes.val(), 12446264u);
	ensure_equals(utils::acct::gzip_idx_reposition_count.val(), 9u);

	Metadata::flushDataReaders();
	for (Collection::iterator i = c.begin(); i != c.end(); ++i)
		i->dropCachedData();

	// Read each other one
	utils::acct::gzip_data_read_count.reset();
	utils::acct::gzip_forward_seek_bytes.reset();
	utils::acct::gzip_idx_reposition_count.reset();
	for (int i = 0; i < repeats; i += 2)
	{
		sys::Buffer b = c[i].getData();
		ensure_equals(b.size(), bufr.size());
		ensure(memcmp(b.data(), bufr.data(), bufr.size()) == 0);
	}
	ensure_equals(utils::acct::gzip_data_read_count.val(), (size_t)repeats / 2);
	ensure_equals(utils::acct::gzip_forward_seek_bytes.val(), 194u * 511u);
	ensure_equals(utils::acct::gzip_idx_reposition_count.val(), 1u);
#endif
}

// Test compression when the data don't compress
template<> template<>
void to::test<5>()
{
	using namespace utils;

#ifdef HAVE_DBALLE
	// Create a collector with only one small metadata inside
	c.clear();
	scan::scan("inbound/test.bufr", c);
	ensure_equals(c.size(), 3u);
	c.pop_back();
	c.pop_back();
	ensure_equals(c.size(), 1u);

	c.writeAtomically("test.md");

	metadata::Collection c1;
	Metadata::readFile("test.md", c1);
	ensure_equals(c.size(), 1u);
	ensure(c[0] ==  c1[0]);
#endif
}


}

// vim:set ts=4 sw=4:
