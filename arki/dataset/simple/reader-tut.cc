/*
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/dataset/test-utils.h>
#include <arki/dataset/simple/index.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/simple/writer.h>
#include <arki/types/assigneddataset.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/utils/metadata.h>
#include <arki/utils/files.h>
#include <arki/scan/grib.h>
#include <wibble/sys/fs.h>
#include <wibble/stream/posix.h>
#include <wibble/grcal/grcal.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::utils;
using namespace arki::dataset::simple;

struct ForceSqlite
{
	bool old;

	ForceSqlite(bool val = true) : old(Manifest::get_force_sqlite())
	{
		Manifest::set_force_sqlite(val);
	}
	~ForceSqlite()
	{
		Manifest::set_force_sqlite(old);
	}
};

struct arki_dataset_simple_reader_shar : public dataset::maintenance::MaintFileVisitor {
	// Little dirty hack: implement MaintFileVisitor so we can conveniently
	// access State

	ConfigFile cfg;

	arki_dataset_simple_reader_shar()
	{
		system("rm -rf testds");
		system("mkdir testds");

		cfg.setValue("type", "simple");
		cfg.setValue("step", "daily");
		cfg.setValue("path", "testds");
		cfg.setValue("name", "testds");

		// Import all data from test.grib1
		Metadata md;
		scan::Grib scanner;
		scanner.open("inbound/test.grib1");

		dataset::simple::Writer testds(cfg);
		size_t count = 0;
		while (scanner.next(md))
		{
			ensure(testds.acquire(md) == WritableDataset::ACQ_OK);
			++count;
		}
		ensure_equals(count, 3u);
		testds.flush();
	}

	virtual void operator()(const std::string& file, State state) {}

	std::string idxfname() const
	{
		return Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
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
TESTGRP(arki_dataset_simple_reader);


// Test querying the datasets
template<> template<>
void to::test<1>()
{
	simple::Reader testds(cfg);
	metadata::Collector mdc;

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
	UItem<AssignedDataset> asd = mdc[0].get<types::AssignedDataset>();
	ensure_equals(asd->name, "testds");
	ensure_equals(asd->id, "");

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
	// TODO
#if 0
	createRebuildFlagfile("testds/2007/07-08.grib1");
	simple::Reader testds(*config.section("testds"));
	ensure(testds.hasWorkingIndex());

	// If the last update looks incomplete, the data will be skipped
	metadata::Collector mdc;
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
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
	simple::Reader testds(*config.section("testds"));

	// There should be no working index
	ensure(!testds.hasWorkingIndex());

	// However, we should still get good results
	metadata::Collector mdc;
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
#endif
}

// Ensure that if the index flagfile exists, the index is not used
template<> template<>
void to::test<4>()
{
	// TODO
#if 0
	createIndexFlagfile("testds");
	simple::Reader testds(*config.section("testds"));

	// There should be no working index
	ensure(!testds.hasWorkingIndex());

	// However, we should still get good results
	metadata::Collector mdc;
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
	simple::Reader testds(*config.section("testds"));
	ensure(!testds.hasWorkingIndex());

	// If the last update looks inhomplete, the data will be skipped
	metadata::Collector mdc;
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200"), false), mdc);
	ensure_equals(mdc.size(), 0u);
#endif
}

// Test querying with postprocessing
template<> template<>
void to::test<6>()
{
	simple::Reader testds(cfg);

	// Use dup() because PosixBuf will close its file descriptor at destruction
	// time
	stream::PosixBuf pb(dup(2));
	ostream os(&pb);
	dataset::ByteQuery bq;
	bq.setPostprocess(Matcher::parse("origin:GRIB1,200"), "testcountbytes");
	testds.queryBytes(bq, os);

	string out = utils::readFile("testcountbytes.out");
	ensure_equals(out, "7399\n");
}

// Test querying with data only
template<> template<>
void to::test<7>()
{
	simple::Reader testds(cfg);

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
	simple::Reader testds(cfg);
	metadata::Collector mdc;

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

// Test querying with archived data
template<> template<>
void to::test<9>()
{
	// TODO: archives are not implemented yet
#if 0
	{
		cfg.setValue("archive age", days_since(2007, 9, 1));
		simple::Writer w(cfg);
		stringstream out;
		files::removeDontpackFlagfile("testds");
		w.repack(out, true);
		ensure_equals(out.str(),
			"testds: archived 2007/07-07.grib1\n"
			"testds: archived 2007/07-08.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 2 files archived, 27352 bytes reclaimed on the index, 27352 total bytes freed.\n");
	}


	simple::Reader testds(cfg);
	metadata::Collector mdc;

	testds.queryData(dataset::DataQuery(Matcher::parse(""), true), mdc);
	ensure_equals(mdc.size(), 3u);
	mdc.clear();

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
	testds.queryData(dataset::DataQuery(Matcher::parse("reftime:=2007-07-08"), true), mdc);
	ensure_equals(mdc.size(), 1u);
	isource = mdc[0].source.upcast<source::Inline>();
	ensure_equals(isource->size, 7218u);

	mdc.clear();
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	testds.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), true), mdc);
	ensure_equals(mdc.size(), 1u);

	// Query bytes
	stringstream out;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse(""));
	testds.queryBytes(bq, out);
	ensure_equals(out.str().size(), 44412u);

	out.str(string());
	bq.matcher = Matcher::parse("origin:GRIB1,200");
	testds.queryBytes(bq, out);
	ensure_equals(out.str().size(), 7218u);

	out.str(string());
	bq.matcher = Matcher::parse("reftime:=2007-07-08");
	testds.queryBytes(bq, out);
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
#endif
}

// Tolerate empty dirs
template<> template<>
void to::test<10>()
{
	// Start with an empty dir
	system("rm -rf testds");
	system("mkdir testds");

	simple::Reader testds(cfg);

	metadata::Collector mdc;
	testds.queryData(dataset::DataQuery(Matcher::parse(""), false), mdc);
	ensure(mdc.empty());

	Summary s;
	testds.querySummary(Matcher::parse(""), s);
	ensure_equals(s.count(), 0u);

	std::stringstream os;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse(""));
	testds.queryBytes(bq, os);
	ensure(os.str().empty());
}


#if 0
// Acquire and query
template<> template<>
void to::test<1>()
{
	Archive arc("testds/.archive/last");
	arc.openRW();

	// Acquire
	system("cp inbound/test.grib1 testds/.archive/last/");
	arc.acquire("test.grib1");
	ensure(sys::fs::access("testds/.archive/last/test.grib1", F_OK));
	ensure(sys::fs::access("testds/.archive/last/test.grib1.metadata", F_OK));
	ensure(sys::fs::access("testds/.archive/last/test.grib1.summary", F_OK));
	ensure(sys::fs::access("testds/.archive/last/" + idxfname(), F_OK));

	metadata::Collector mdc;
	Metadata::readFile("testds/.archive/last/test.grib1.metadata", mdc);
	ensure_equals(mdc.size(), 3u);
	ensure_equals(mdc[0].source.upcast<source::Blob>()->filename, "test.grib1");

	// Query
	mdc.clear();
	arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 3u);

	// Maintenance should show it's all ok
	MaintenanceCollector c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	//c.dump(cerr);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());
}
#endif

#if 0
// Test maintenance scan on non-indexed files
template<> template<>
void to::test<2>()
{
	Archive arc("testds/.archive/last");
	arc.openRW();
	system("cp inbound/test.grib1 testds/.archive/last/");

	// Query now is ok
	metadata::Collector mdc;
	arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 0u);

	// Maintenance should show one file to index
	MaintenanceCollector c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_TO_INDEX), 1u);
	ensure_equals(c.remaining(), string());
	ensure(not c.isClean());

	{
		Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_INDEX), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(), 
				"testds: rescanned in archive last/test.grib1\n"
				"testds: archive cleaned up\n"
				"testds: database cleaned up\n"
				"testds: rebuild summary cache\n"
				"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	c.clear();
	arc.maintenance(c);
	//cerr << c.remaining() << endl;
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());
}

// Test maintenance scan on missing metadata
template<> template<>
void to::test<3>()
{
	Archive arc("testds/.archive/last");
	arc.openRW();
	system("cp inbound/test.grib1 testds/.archive/last/");
	arc.acquire("test.grib1");
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.metadata");
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.summary");
	ensure(sys::fs::access("testds/.archive/last/test.grib1", F_OK));
	ensure(!sys::fs::access("testds/.archive/last/test.grib1.metadata", F_OK));
	ensure(!sys::fs::access("testds/.archive/last/test.grib1.summary", F_OK));
	ensure(sys::fs::access("testds/.archive/last/" + idxfname(), F_OK));

	// Query now is ok
	metadata::Collector mdc;
	arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 3u);

	// Maintenance should show one file to rescan
	MaintenanceCollector c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_TO_RESCAN), 1u);
	ensure_equals(c.remaining(), string());
	ensure(not c.isClean());

	{
		Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	c.clear();
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());
}

// Test maintenance scan on missing summary
template<> template<>
void to::test<4>()
{
	Archive arc("testds/.archive/last");
	arc.openRW();
	system("cp inbound/test.grib1 testds/.archive/last/");
	arc.acquire("test.grib1");
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.summary");
	ensure(sys::fs::access("testds/.archive/last/test.grib1", F_OK));
	ensure(sys::fs::access("testds/.archive/last/test.grib1.metadata", F_OK));
	ensure(!sys::fs::access("testds/.archive/last/test.grib1.summary", F_OK));
	ensure(sys::fs::access("testds/.archive/last/" + idxfname(), F_OK));

	// Query now is ok
	metadata::Collector mdc;
	arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 3u);

	// Maintenance should show one file to rescan
	MaintenanceCollector c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_TO_RESCAN), 1u);
	ensure_equals(c.remaining(), string());
	ensure(not c.isClean());

	{
		Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	c.clear();
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());
}

// Test maintenance scan on missing metadata
template<> template<>
void to::test<5>()
{
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/last/1.grib1");
		system("cp inbound/test.grib1 testds/.archive/last/2.grib1");
		system("cp inbound/test.grib1 testds/.archive/last/3.grib1");
		arc.acquire("1.grib1");
		arc.acquire("2.grib1");
		arc.acquire("3.grib1");
	}
	sys::fs::deleteIfExists("testds/.archive/last/2.grib1.metadata");
	ensure(sys::fs::access("testds/.archive/last/2.grib1", F_OK));
	ensure(!sys::fs::access("testds/.archive/last/2.grib1.metadata", F_OK));
	ensure(sys::fs::access("testds/.archive/last/2.grib1.summary", F_OK));
	ensure(sys::fs::access("testds/.archive/last/" + idxfname(), F_OK));

	// Query now is ok
	{
		metadata::Collector mdc;
		Archive arc("testds/.archive/last");
		arc.openRO();
		arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
		ensure_equals(mdc.size(), 9u);
	}

	// Maintenance should show one file to rescan
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		MaintenanceCollector c;
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());
	}

	{
		Writer writer(cfg);

		MaintenanceCollector c;
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.count(ARC_OK), 2u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/2.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(ARC_OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}

	// Everything should be fine now
	{
		Archive arc("testds/.archive/last");
		arc.openRW();
		MaintenanceCollector c;
		arc.maintenance(c);
		ensure_equals(c.fileStates.size(), 3u);
		ensure_equals(c.count(ARC_OK), 3u);
		ensure_equals(c.remaining(), string());
		ensure(c.isClean());
	}
}

// Test maintenance scan on compressed archives
template<> template<>
void to::test<6>()
{
	// Import a file
	Archive arc("testds/.archive/last");
	arc.openRW();
	system("cp inbound/test.grib1 testds/.archive/last/");
	arc.acquire("test.grib1");

	// Compress it
	metadata::Collector mdc;
	Metadata::readFile("testds/.archive/last/test.grib1.metadata", mdc);
	ensure_equals(mdc.size(), 3u);
	mdc.compressDataFile(1024, "metadata file testds/.archive/last/test.grib1.metadata");
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1");

	ensure(!sys::fs::access("testds/.archive/last/test.grib1", F_OK));
	ensure(sys::fs::access("testds/.archive/last/test.grib1.gz", F_OK));
	ensure(sys::fs::access("testds/.archive/last/test.grib1.gz.idx", F_OK));
	ensure(sys::fs::access("testds/.archive/last/test.grib1.metadata", F_OK));
	ensure(sys::fs::access("testds/.archive/last/test.grib1.summary", F_OK));
	ensure(sys::fs::access("testds/.archive/last/" + idxfname(), F_OK));

	// Query now is ok
	mdc.clear();
	arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 3u);

	// Maintenance should show that everything is ok now
	MaintenanceCollector c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());

	// Try removing summary and metadata
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.metadata");
	sys::fs::deleteIfExists("testds/.archive/last/test.grib1.summary");

	// Cannot query anymore
	mdc.clear();
	try {
		arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
		ensure(false);
	} catch (std::exception& e) {
		ensure(str::startsWith(e.what(), "file needs to be manually decompressed before scanning."));
	}

	// Maintenance should show one file to rescan
	c.clear();
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_TO_RESCAN), 1u);
	ensure_equals(c.remaining(), string());
	ensure(not c.isClean());

	{
		Writer writer(cfg);

		c.clear();
		writer.maintenance(c);
		ensure_equals(c.fileStates.size(), 1u);
		ensure_equals(c.count(ARC_TO_RESCAN), 1u);
		ensure_equals(c.remaining(), string());
		ensure(not c.isClean());

		stringstream s;

		// Check should reindex the file
		writer.check(s, true, true);
		ensure_equals(s.str(),
			"testds: rescanned in archive last/test.grib1\n"
			"testds: archive cleaned up\n"
			"testds: database cleaned up\n"
			"testds: rebuild summary cache\n"
			"testds: 1 file rescanned, 3616 bytes reclaimed cleaning the index.\n");

		// Repack should do nothing
		s.str(std::string());
		writer.repack(s, true);
		ensure_equals(s.str(), string()); // Nothing should have happened
	}
}

// Test handling of empty archive dirs (such as last with everything moved away)
template<> template<>
void to::test<7>()
{
	// Import a file in a secondary archive
	{
		system("mkdir testds/.archive/foo");
		Archive arc("testds/.archive/foo");
		arc.openRW();
		system("cp inbound/test.grib1 testds/.archive/foo/");
		arc.acquire("test.grib1");
	}

	// Instantiate an archive group
	Archives arc("testds/.archive");

	// Query now is ok
	metadata::Collector mdc;
	arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 3u);

	// Maintenance should show that everything is ok now
	MaintenanceCollector c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());
}

// Tolerate empty dirs
template<> template<>
void to::test<8>()
{
	// Start with an empty dir
	system("rm -rf testds");
	system("mkdir testds");

	simple::Reader testds(cfg);

	MetadataCollector mdc;
	testds.queryData(dataset::DataQuery(Matcher::parse(""), false), mdc);
	ensure(mdc.empty());

	Summary s;
	testds.querySummary(Matcher::parse(""), s);
	ensure_equals(s.count(), 0u);

	std::stringstream os;
	dataset::ByteQuery bq;
	bq.setData(Matcher::parse(""));
	testds.queryBytes(bq, os);
	ensure(os.str().empty());
}


// Retest with sqlite
template<> template<> void to::test<9>() { ForceSqlite fs; test<1>(); }
template<> template<> void to::test<10>() { ForceSqlite fs; test<2>(); }
template<> template<> void to::test<11>() { ForceSqlite fs; test<3>(); }
template<> template<> void to::test<12>() { ForceSqlite fs; test<4>(); }
template<> template<> void to::test<13>() { ForceSqlite fs; test<5>(); }
template<> template<> void to::test<14>() { ForceSqlite fs; test<6>(); }
template<> template<> void to::test<15>() { ForceSqlite fs; test<7>(); }
template<> template<> void to::test<16>() { ForceSqlite fs; test<8>(); }

#endif


}
