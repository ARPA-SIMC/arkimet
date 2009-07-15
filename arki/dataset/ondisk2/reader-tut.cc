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
#include <arki/dataset/ondisk2.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/scan/grib.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>
#include <wibble/stream/posix.h>
#include <wibble/grcal/grcal.h>

#include <unistd.h>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::dataset::ondisk2;
using namespace arki::utils::files;

struct MetadataCollector : public vector<Metadata>, public MetadataConsumer
{
	bool operator()(Metadata& md)
	{
		push_back(md);
		return true;
	}
};

struct arki_dataset_ondisk2_reader_shar {
	ConfigFile config;

	arki_dataset_ondisk2_reader_shar()
	{
		// Cleanup the test datasets
		system("rm -rf testds");
		system("mkdir testds");

		// In-memory dataset configuration
		string conf =
			"[testds]\n"
			"type = ondisk2\n"
			"step = daily\n"
			"filter = origin: GRIB1,200\n"
			"name = testds\n"
			"path = testds\n";
		stringstream incfg(conf);
		config.parse(incfg, "(memory)");

		// Import all data from test.grib1
		Metadata md;
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

		dataset::ondisk2::Writer testds(*config.section("testds"));
		size_t count = 0;
		while (scanner.next(md))
		{
			ensure(testds.acquire(md) == WritableDataset::ACQ_OK);
			++count;
		}
		ensure_equals(count, 3u);
		testds.flush();
	}

	std::string days_since(int year, int month, int day)
	{
		// Data are from 07, 08, 10 2007
		int threshold[6] = { year, month, day, 0, 0, 0 };
		int now[6];
		grcal::date::now(now);
		long long int duration = grcal::date::duration(threshold, now);

		//cerr << str::fmt(duration/(3600*24)) + " days";
		return str::fmt(duration/(3600*24));
	}
};
TESTGRP(arki_dataset_ondisk2_reader);

// Test querying the datasets
template<> template<>
void to::test<1>()
{
	ondisk2::Reader testds(*config.section("testds"));
	MetadataCollector mdc;

	ensure(testds.hasWorkingIndex());

	testds.queryMetadata(Matcher::parse("origin:GRIB1,200"), false, mdc);
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
	testds.queryMetadata(Matcher::parse("origin:GRIB1,80"), false, mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	testds.queryMetadata(Matcher::parse("origin:GRIB1,98"), false, mdc);
	ensure_equals(mdc.size(), 1u);
}

// Ensure that rebuild datafiles are not read when using the index
template<> template<>
void to::test<2>()
{
	// TODO
#if 0
	createRebuildFlagfile("testds/2007/07-08.grib1");
	ondisk2::Reader testds(*config.section("testds"));
	ensure(testds.hasWorkingIndex());

	// If the last update looks incomplete, the data will be skipped
	MetadataCollector mdc;
	testds.queryMetadata(Matcher::parse("origin:GRIB1,200"), false, mdc);
	ensure_equals(mdc.size(), 0u);
#endif
}

// Ensure that if the index does not exists, it does not try to use it
template<> template<>
void to::test<3>()
{
	// TODO
#if 0
	system("rm testds/index.sqlite");
	ondisk2::Reader testds(*config.section("testds"));

	// There should be no working index
	ensure(!testds.hasWorkingIndex());

	// However, we should still get good results
	MetadataCollector mdc;
	testds.queryMetadata(Matcher::parse("origin:GRIB1,200"), false, mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::BLOB);
	ensure_equals(source->format, "grib1");
	UItem<source::Blob> blob = source.upcast<source::Blob>();
	ensure_equals(blob->filename, sys::fs::abspath("testds/2007/07-08.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 7218u);
#endif
}

// Ensure that if the index flagfile exists, the index is not used
template<> template<>
void to::test<4>()
{
	// TODO
#if 0
	createIndexFlagfile("testds");
	ondisk2::Reader testds(*config.section("testds"));

	// There should be no working index
	ensure(!testds.hasWorkingIndex());

	// However, we should still get good results
	MetadataCollector mdc;
	testds.queryMetadata(Matcher::parse("origin:GRIB1,200"), false, mdc);
	ensure_equals(mdc.size(), 1u);

	// Check that the source record that comes out is ok
	UItem<Source> source = mdc[0].source;
	ensure_equals(source->style(), Source::BLOB);
	ensure_equals(source->format, "grib1");
	UItem<source::Blob> blob = source.upcast<source::Blob>();
	ensure_equals(blob->filename, sys::fs::abspath("testds/2007/07-08.grib1"));
	ensure_equals(blob->offset, 0u);
	ensure_equals(blob->size, 7218u);
#endif
}

// Ensure that rebuild datafiles are not read when not using the index
template<> template<>
void to::test<5>()
{
	// TODO
#if 0
	createIndexFlagfile("testds");
	createRebuildFlagfile("testds/2007/07-08.grib1");
	ondisk2::Reader testds(*config.section("testds"));
	ensure(!testds.hasWorkingIndex());

	// If the last update looks inhomplete, the data will be skipped
	MetadataCollector mdc;
	testds.queryMetadata(Matcher::parse("origin:GRIB1,200"), false, mdc);
	ensure_equals(mdc.size(), 0u);
#endif
}

// Test querying with postprocessing
template<> template<>
void to::test<6>()
{
	ondisk2::Reader testds(*config.section("testds"));
	ensure(testds.hasWorkingIndex());

	// Use dup() because PosixBuf will close its file descriptor at destruction
	// time
	stream::PosixBuf pb(dup(2));
	ostream os(&pb);
	testds.queryBytes(Matcher::parse("origin:GRIB1,200"), os, ReadonlyDataset::BQ_POSTPROCESS, "testcountbytes");

	string out = utils::readFile("testcountbytes.out");
	ensure_equals(out, "7218\n");
}

// Test querying with data only
template<> template<>
void to::test<7>()
{
	ondisk2::Reader testds(*config.section("testds"));
	ensure(testds.hasWorkingIndex());

	std::stringstream os;
	testds.queryBytes(Matcher::parse("origin:GRIB1,200"), os, ReadonlyDataset::BQ_DATA);

	ensure_equals(os.str().substr(0, 4), "GRIB");
}

// Test querying with inline data
template<> template<>
void to::test<8>()
{
	ondisk2::Reader testds(*config.section("testds"));
	MetadataCollector mdc;

	ensure(testds.hasWorkingIndex());

	testds.queryMetadata(Matcher::parse("origin:GRIB1,200"), true, mdc);
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
	testds.queryMetadata(Matcher::parse("origin:GRIB1,80"), true, mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	testds.queryMetadata(Matcher::parse("origin:GRIB1,98"), true, mdc);
	ensure_equals(mdc.size(), 1u);
}

// Test querying with archived data
template<> template<>
void to::test<9>()
{
	{
		config.section("testds")->setValue("archive age", days_since(2007, 9, 1));
		dataset::ondisk2::Writer w(*config.section("testds"));
		stringstream out;
		removeDontpackFlagfile("testds");
		w.repack(out, true);
		ensure_equals(out.str(),
			"testds: archived 2007/07-07.grib1\n"
			"testds: archived 2007/07-08.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 2 files archived, 27352 bytes reclaimed on the index, 27352 total bytes freed.\n");
	}


	ondisk2::Reader testds(*config.section("testds"));
	MetadataCollector mdc;

	ensure(testds.hasWorkingIndex());

	testds.queryMetadata(Matcher::parse(""), true, mdc);
	ensure_equals(mdc.size(), 3u);
	mdc.clear();

	testds.queryMetadata(Matcher::parse("origin:GRIB1,200"), true, mdc);
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
	testds.queryMetadata(Matcher::parse("reftime:=2007-07-08"), true, mdc);
	ensure_equals(mdc.size(), 1u);
	isource = mdc[0].source.upcast<source::Inline>();
	ensure_equals(isource->size, 7218u);

	mdc.clear();
	testds.queryMetadata(Matcher::parse("origin:GRIB1,80"), true, mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	testds.queryMetadata(Matcher::parse("origin:GRIB1,98"), true, mdc);
	ensure_equals(mdc.size(), 1u);

	// Query bytes
	stringstream out;
	testds.queryBytes(Matcher::parse(""), out, ReadonlyDataset::BQ_DATA);
	ensure_equals(out.str().size(), 44412u);

	out.str(string());
	testds.queryBytes(Matcher::parse("origin:GRIB1,200"), out, ReadonlyDataset::BQ_DATA);
	ensure_equals(out.str().size(), 7218u);

	out.str(string());
	testds.queryBytes(Matcher::parse("reftime:=2007-07-08"), out, ReadonlyDataset::BQ_DATA);
	ensure_equals(out.str().size(), 7218u);

	/* TODO
		case BQ_POSTPROCESS: {
		case BQ_REP_METADATA: {
		case BQ_REP_SUMMARY: {
	virtual void querySummary(const Matcher& matcher, Summary& summary);
	*/

	// Query summary
	Summary s;
	testds.querySummary(Matcher::parse(""), s);
	ensure_equals(s.count(), 3u);
	ensure_equals(s.size(), 44412u);

	s.clear();
	testds.querySummary(Matcher::parse("origin:GRIB1,200"), s);
	ensure_equals(s.count(), 1u);
	ensure_equals(s.size(), 7218u);

	s.clear();
	testds.querySummary(Matcher::parse("reftime:=2007-07-08"), s);
	ensure_equals(s.count(), 1u);
	ensure_equals(s.size(), 7218u);
}

// Test that summary files are not created for all the extent of the query, but
// only for data present in the dataset
template<> template<>
void to::test<10>()
{
	ondisk2::Reader testds(*config.section("testds"));

	Summary s;
	testds.querySummary(Matcher::parse("reftime:=2007"), s);
	ensure_equals(s.count(), 3u);
	ensure_equals(s.size(), 44412u);

	// Global summary is not built because we only query specific months
	ensure(!sys::fs::access("testds/.summaries/all.summary", F_OK));

	ensure(!sys::fs::access("testds/.summaries/2007-01.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-02.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-03.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-04.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-05.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-06.summary", F_OK));
	ensure(sys::fs::access("testds/.summaries/2007-07.summary", F_OK));
	// Summary caches corresponding to DB holes are still created and used
	ensure(sys::fs::access("testds/.summaries/2007-08.summary", F_OK));
	ensure(sys::fs::access("testds/.summaries/2007-09.summary", F_OK));
	ensure(sys::fs::access("testds/.summaries/2007-10.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-11.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2007-12.summary", F_OK));
	ensure(!sys::fs::access("testds/.summaries/2008-01.summary", F_OK));
}

}

// vim:set ts=4 sw=4:
