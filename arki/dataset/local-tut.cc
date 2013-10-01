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

#include "config.h"

#include <arki/dataset/test-utils.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/dataset/local.h>
#include <arki/dataset/maintenance.h>
#include <arki/scan/any.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble::tests;
using namespace arki::tests;
using namespace arki::dataset;
using namespace arki::utils;
using namespace wibble;

struct arki_dataset_local_shar : public DatasetTest {
	arki_dataset_local_shar()
	{
		cfg.setValue("path", "testds");
		cfg.setValue("name", "testds");
		cfg.setValue("type", "simple");
		cfg.setValue("step", "daily");
	}

};
TESTGRP(arki_dataset_local);

// Test moving into archive data that have been compressed
template<> template<>
void to::test<1>()
{
	ConfigFile cfg = this->cfg;
	cfg.setValue("archive age", str::fmt(days_since(2007, 9, 1)));

	// Import and compress all the files
	clean_and_import(&cfg);
	scan::compress("testds/2007/07-07.grib1");
	scan::compress("testds/2007/07-08.grib1");
	scan::compress("testds/2007/10-09.grib1");
	sys::fs::deleteIfExists("testds/2007/07-07.grib1");
	sys::fs::deleteIfExists("testds/2007/07-08.grib1");
	sys::fs::deleteIfExists("testds/2007/10-09.grib1");

	// Test that querying returns all items
	{
		std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));

		metadata::Counter counter;
		reader->queryData(dataset::DataQuery(Matcher::parse(""), false), counter);
		ensure_equals(counter.count, 3u);
	}

	// Check if files to archive are detected
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));

		MaintenanceCollector c;
		writer->maintenance(c);

		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(COUNTED_OK), 1u);
		ensure_equals(c.count(COUNTED_TO_ARCHIVE), 2u);
		ensure_equals(c.remaining(), "");
		ensure(not c.isClean());
	}

	// Perform packing and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": archived 2007/07-07.grib1");
		s.ensure_line_contains(": archived 2007/07-08.grib1");
		s.ensure_line_contains(": archive cleaned up");
		s.ensure_line_contains(": 2 files archived");
		s.ensure_all_lines_seen();
	}

	// Check that the files have been moved to the archive
	ensure(!sys::fs::exists("testds/.archive/last/2007/07-07.grib1"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-07.grib1.gz"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-07.grib1.gz.idx"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-07.grib1.metadata"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-07.grib1.summary"));
	ensure(!sys::fs::exists("testds/.archive/last/2007/07-08.grib1"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-08.grib1.gz"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-08.grib1.gz.idx"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-08.grib1.metadata"));
	ensure(sys::fs::exists("testds/.archive/last/2007/07-08.grib1.summary"));
	ensure(!sys::fs::exists("testds/2007/07-07.grib1"));
	ensure(!sys::fs::exists("testds/2007/07-07.grib1.gz"));
	ensure(!sys::fs::exists("testds/2007/07-07.grib1.gz.idx"));
	ensure(!sys::fs::exists("testds/2007/07-07.grib1.metadata"));
	ensure(!sys::fs::exists("testds/2007/07-07.grib1.summary"));
	ensure(!sys::fs::exists("testds/2007/07-08.grib1"));
	ensure(!sys::fs::exists("testds/2007/07-08.grib1.gz"));
	ensure(!sys::fs::exists("testds/2007/07-08.grib1.gz.idx"));
	ensure(!sys::fs::exists("testds/2007/07-08.grib1.metadata"));
	ensure(!sys::fs::exists("testds/2007/07-08.grib1.summary"));

	// Maintenance should now show a normal situation
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		MaintenanceCollector c;
		writer->maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 1u);
		ensure_equals(c.count(COUNTED_ARC_OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Perform full maintenance and check that things are still ok afterwards
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		stringstream s;
		s.str(std::string());
		writer->check(s, true, true);
		ensure_equals(s.str(), string()); // Nothing should have happened

		MaintenanceCollector c;
		c.clear();
		writer->maintenance(c);
		ensure_equals(c.count(COUNTED_OK), 1u);
		ensure_equals(c.count(COUNTED_ARC_OK), 2u);
		ensure_equals(c.remaining(), "");
		ensure(c.isClean());
	}

	// Test that querying returns all items
	{
		std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));

		metadata::Counter counter;
		reader->queryData(dataset::DataQuery(Matcher::parse(""), false), counter);
		ensure_equals(counter.count, 3u);
	}
}

// Test querying the datasets
template<> template<>
void to::test<2>()
{
	using namespace arki::types;

	clean_and_import();
	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	metadata::Collection mdc;
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 1u);

    // Check that the source record that comes out is ok
    UItem<Source> source = mdc[0].source;
    wassert(actual(mdc[0].source).sourceblob_is("grib1", sys::fs::abspath("testds"), "2007/07-08.grib1", 0, 7218));

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), false), mdc);
	ensure_equals(mdc.size(), 1u);
}

// Test querying with data only
template<> template<>
void to::test<3>()
{
	clean_and_import();
	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	std::stringstream os;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse("origin:GRIB1,200"));
	reader->queryBytes(bq, os);

	ensure_equals(os.str().substr(0, 4), "GRIB");
}

// Test querying with inline data
template<> template<>
void to::test<4>()
{
	using namespace arki::types;

	clean_and_import();
	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	metadata::Collection mdc;
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), true), mdc);
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
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), true), mdc);
	ensure_equals(mdc.size(), 1u);
}

// Test querying with archived data
template<> template<>
void to::test<5>()
{
	using namespace arki::types;

	ConfigFile cfg = this->cfg;
	cfg.setValue("archive age", str::fmt(days_since(2007, 9, 1)));
	clean_and_import(&cfg);
	{
		auto_ptr<WritableLocal> writer(makeLocalWriter(&cfg));
		OutputChecker s;
		writer->repack(s, true);
		s.ensure_line_contains(": archived 2007/07-07.grib1");
		s.ensure_line_contains(": archived 2007/07-08.grib1");
		s.ignore_line_containing("archive cleaned up");
		s.ensure_line_contains(": 2 files archived");
		s.ensure_all_lines_seen();
	}

	std::auto_ptr<ReadonlyDataset> reader(makeReader(&cfg));
	metadata::Collection mdc;

	reader->queryData(dataset::DataQuery(Matcher::parse(""), true), mdc);
	ensure_equals(mdc.size(), 3u);
	mdc.clear();

	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), true), mdc);
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
	reader->queryData(dataset::DataQuery(Matcher::parse("reftime:=2007-07-08"), true), mdc);
	ensure_equals(mdc.size(), 1u);
	isource = mdc[0].source.upcast<source::Inline>();
	ensure_equals(isource->size, 7218u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	reader->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	// Query bytes
	stringstream out;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse(""));
	reader->queryBytes(bq, out);
	ensure_equals(out.str().size(), 44412u);

	out.str(string());
	bq.matcher = Matcher::parse("origin:GRIB1,200");
	reader->queryBytes(bq, out);
	ensure_equals(out.str().size(), 7218u);

	out.str(string());
	bq.matcher = Matcher::parse("reftime:=2007-07-08");
	reader->queryBytes(bq, out);
	ensure_equals(out.str().size(), 7218u);

	/* TODO
		case BQ_POSTPROCESS: {
		case BQ_REP_METADATA: {
		case BQ_REP_SUMMARY: {
	virtual void querySummary(const Matcher& matcher, Summary& summary);
	*/

	// Query summary
	Summary s;
	reader->querySummary(Matcher::parse(""), s);
	ensure_equals(s.count(), 3u);
	ensure_equals(s.size(), 44412u);

	s.clear();
	reader->querySummary(Matcher::parse("origin:GRIB1,200"), s);
	ensure_equals(s.count(), 1u);
	ensure_equals(s.size(), 7218u);

	s.clear();
	reader->querySummary(Matcher::parse("reftime:=2007-07-08"), s);
	ensure_equals(s.count(), 1u);
	ensure_equals(s.size(), 7218u);
}

// Tolerate empty dirs
template<> template<>
void to::test<6>()
{
	// Start with an empty dir
	system("rm -rf testds");
	system("mkdir testds");

	std::auto_ptr<ReadonlyDataset> reader(makeReader());

	metadata::Collection mdc;
	reader->queryData(dataset::DataQuery(Matcher::parse(""), false), mdc);
	ensure(mdc.empty());

	Summary s;
	reader->querySummary(Matcher::parse(""), s);
	ensure_equals(s.count(), 0u);

	std::stringstream os;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse(""));
	reader->queryBytes(bq, os);
	ensure(os.str().empty());
}

template<> template<> void to::test<7>() { ForceSqlite fs; to::test<1>(); }
template<> template<> void to::test<8>() { ForceSqlite fs; to::test<2>(); }
template<> template<> void to::test<9>() { ForceSqlite fs; to::test<3>(); }
template<> template<> void to::test<10>() { ForceSqlite fs; to::test<4>(); }
template<> template<> void to::test<11>() { ForceSqlite fs; to::test<5>(); }
template<> template<> void to::test<12>() { ForceSqlite fs; to::test<6>(); }

template<> template<> void to::test<13>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<1>(); }
template<> template<> void to::test<14>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<2>(); }
template<> template<> void to::test<15>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<3>(); }
template<> template<> void to::test<16>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<4>(); }
template<> template<> void to::test<17>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<5>(); }
template<> template<> void to::test<18>() { TempConfig tc(cfg, "type", "ondisk2"); to::test<6>(); }



}

// vim:set ts=4 sw=4:
