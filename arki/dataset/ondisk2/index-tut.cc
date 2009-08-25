/*
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>
#include <arki/dataset/ondisk2/index.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/ensemble.h>
#include <arki/scan/any.h>
#include <arki/utils/metadata.h>
#include <arki/configfile.h>
#include <arki/matcher.h>
#include <arki/summary.h>

#include <wibble/sys/childprocess.h>
#include <wibble/sys/mutex.h>

#include <memory>
#include <sstream>
#include <fstream>
#include <iostream>
#include <unistd.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::dataset::ondisk2;
using namespace arki::types;
using namespace arki::utils;

// Create a dataset index gived its configuration
template<typename INDEX>
static inline auto_ptr<INDEX> createIndex(const std::string& config)
{
	stringstream confstream(config);
	ConfigFile cfg;
	cfg.parse(confstream, "(memory)");
	return auto_ptr<INDEX>(new INDEX(cfg));
}

struct arki_dataset_ondisk2_index_shar {
	Metadata md;
	Metadata md1;

	ValueBag testArea;
	ValueBag testEnsemble;

	arki_dataset_ondisk2_index_shar()
	{
		testArea.set("foo", Value::createInteger(5));
		testArea.set("bar", Value::createInteger(5000));
		testArea.set("baz", Value::createInteger(-200));
		testArea.set("moo", Value::createInteger(0x5ffffff));
		testArea.set("antani", Value::createInteger(-1));
		testArea.set("blinda", Value::createInteger(0));
		testArea.set("supercazzola", Value::createInteger(-1234567));
		testArea.set("pippo", Value::createString("pippo"));
		testArea.set("pluto", Value::createString("12"));
		testEnsemble = testArea;

		md.create();
		md.source = source::Blob::create("grib", "antani", 10, 2000);
		md.set(origin::GRIB1::create(200, 10, 100));
		md.set(product::GRIB1::create(3, 4, 5));
		md.set(level::GRIB1::create(1, 2));
		md.set(timerange::GRIB1::create(4, 5, 6, 7));
		md.set(reftime::Position::create(types::Time::create(2006, 5, 4, 3, 2, 1)));
		md.set(area::GRIB::create(testArea));
		md.set(ensemble::GRIB::create(testEnsemble));
		md.notes.push_back(types::Note::create("this is a test"));
		ofstream out("test-md.metadata");
		if (out.fail()) throw wibble::exception::File("test-md.metadata", "opening file");
		md.write(out, "test-md.metadata");
		out.close();

		md1.create();
		md1.source = source::Blob::create("grib", "blinda", 20, 40000);
		md1.set(origin::GRIB1::create(201, 11, 3));
		md1.set(product::GRIB1::create(102, 103, 104));
		md1.set(level::GRIB1::create(1, 3));
		md1.set(timerange::GRIB1::create(4, 6, 6, 6));
		md1.set(reftime::Position::create(types::Time::create(2003, 4, 5, 6, 7, 8)));
		md1.set(area::GRIB::create(testArea));
		md1.set(ensemble::GRIB::create(testEnsemble));
		// Index one without notes
		//md1.notes.push_back(types::Note::create("this is another test"));
		out.open("test-md1.metadata");
		if (out.fail()) throw wibble::exception::File("test-md1.metadata", "opening file");
		md1.write(out, "test-md1.metadata");
		out.close();
	}
};
TESTGRP(arki_dataset_ondisk2_index);

// Trying indexing a few metadata
template<> template<>
void to::test<1>()
{
	auto_ptr<WIndex> test = createIndex<WIndex>(
		"type = ondisk2\n"
		"path = \n"
		"indexfile = :memory:\n"
		"index = origin, product, level\n"
		"unique = origin, product, level, timerange, area, ensemble, reftime\n"
	);
	ensure(test.get() != 0);
	Pending p;

	test->open();
	p = test->beginTransaction();
	
	// Index a metadata
	int id = test->id(md);
	ensure_equals(id, -1);
	test->index(md, "test-md", 0);
	ensure_equals(test->id(md), 1);

	// Index a second one
	id = test->id(md1);
	ensure_equals(id, -1);
	test->index(md1, "test-md1", 0);
	ensure_equals(test->id(md1), 2);

	// Query various kinds of metadata
	metadata::Collector mdc;
	test->query(Matcher::parse("origin:GRIB1,200"), mdc);
	ensure_equals(mdc.size(), 1u);
	ensure_equals(mdc[0].notes.size(), 1u);
	ensure_equals(mdc[0].notes[0]->content, "this is a test");

	mdc.clear();
	test->query(Matcher::parse("product:GRIB1,3"), mdc);
	ensure_equals(mdc.size(), 1u);

	// TODO: level, timerange, area, ensemble, reftime
	p.commit();
}

// See if remove works
template<> template<>
void to::test<2>()
{
	auto_ptr<WIndex> test = createIndex<WIndex>(
		"type = ondisk2\n"
		"path = .\n"
		"indexfile = :memory:\n"
		"unique = origin, product, level, timerange, area, ensemble, reftime\n"
		"index = origin, product, level, timerange, area, ensemble, reftime\n"
	);
	ensure(test.get() != 0);
	Pending p;

	test->open();
	p = test->beginTransaction();
	
	// Index a metadata
	test->index(md, "test-md", 0);
	int id = test->id(md);

	// Index it again and ensure that it fails
	try {
		test->index(md, "test-md", 0);
		ensure(false);
	} catch (dataset::index::DuplicateInsert& e) {
	}

	// Index a second one
	test->index(md1, "test-md1", 0);
	int id1 = test->id(md1);

	// Ensure that we have two items
	metadata::Collector mdc;
	test->query(Matcher::parse("origin:GRIB1"), mdc);
	ensure_equals(mdc.size(), 2u);
	mdc.clear();

	// Remove a nonexisting item and see that it fails
	string file;
	try {
		test->remove(100, file);
		ensure(false);
	} catch (wibble::exception::Consistency& e) {
	}

	// Remove the first item
	test->remove(id, file);
	ensure((bool)p);
	ensure_equals(file, "test-md");
	p.commit();

	// There should be only one result now
	test->query(Matcher::parse("origin:GRIB1"), mdc);
	ensure_equals(mdc.size(), 1u);

	// It should be the second item we inserted
	ensure_equals(test->id(mdc[0]), id1);
	mdc.clear();

	// Replace it with a different one
	test->replace(md1, "test-md", 0);

	// See that it changed
	test->query(Matcher::parse("origin:GRIB1"), mdc);
	ensure_equals(mdc.size(), 1u);
	Item<source::Blob> blob = mdc[0].source.upcast<source::Blob>();
	ensure_equals(blob->filename, "test-md");

	p.commit();
}

namespace {
struct ReadHang : public sys::ChildProcess, public MetadataConsumer
{
	ConfigFile cfg;
	int commfd;

	ReadHang(const std::string& cfgstr)
	{
		stringstream confstream(cfgstr);
		cfg.parse(confstream, "(memory)");
	}

	virtual bool operator()(Metadata& md)
	{
		cout << "H" << endl;
		usleep(100000);
		return true;
	}

	virtual int main()
	{
		try {
			RIndex idx(cfg);
			idx.open();
			idx.query(Matcher::parse("origin:GRIB1"), *this);
		} catch (std::exception& e) {
			cerr << e.what() << endl;
			cout << "E" << endl;
			return 1;
		}
		return 0;
	}

	void start()
	{
		forkAndRedirect(0, &commfd);
	}

	char waitUntilHung()
	{
		char buf[2];
		if (read(commfd, buf, 1) != 1)
			throw wibble::exception::System("reading 1 byte from child process");
		return buf[0];
	}
};
}

// Test concurrent read and update
template<> template<>
void to::test<3>()
{
	string cfg = 
		"type = ondisk2\n"
		"path = .\n"
		"indexfile = file1\n"
		"unique = origin, product, level, timerange, area, ensemble, reftime\n"
		"index = origin, product, level, timerange, area, ensemble, reftime\n";

	// Remove index if it exists
	unlink("file1");

	// Create the index and index two metadata
	{
		auto_ptr<WIndex> test1 = createIndex<WIndex>(cfg);
		test1->open();

		Pending p = test1->beginTransaction();
		test1->index(md, "test-md", 0);
		test1->index(md1, "test-md1", 0);
		p.commit();
	}

	// Query the index and hang
	ReadHang readHang(cfg);
	readHang.start();
	ensure_equals(readHang.waitUntilHung(), 'H');

	// Now try to index another element
	Metadata md3;
	md3.create();
	md3.source = source::Blob::create("grib", "antani3", 10, 2000);
	md3.set(origin::GRIB1::create(202, 12, 102));
	md3.set(product::GRIB1::create(3, 4, 5));
	md3.set(level::GRIB1::create(1, 2));
	md3.set(timerange::GRIB1::create(4, 5, 6, 7));
	md3.set(reftime::Position::create(types::Time::create(2006, 5, 4, 3, 2, 1)));
	md3.set(area::GRIB::create(testArea));
	md3.set(ensemble::GRIB::create(testEnsemble));
	md3.notes.push_back(types::Note::create("this is a test"));
	{
		auto_ptr<WIndex> test1 = createIndex<WIndex>(cfg);
		test1->open();
		Pending p = test1->beginTransaction();
		test1->index(md3, "test-md1", 0);
		p.commit();
	}

	readHang.kill(9);
	readHang.wait();
}

// Test getting the metadata corresponding to a file
template<> template<>
void to::test<4>()
{
	// Remove index if it exists
	unlink("file1");

	auto_ptr<WIndex> test = createIndex<WIndex>(
		"type = ondisk2\n"
		"path = .\n"
		"indexfile = file1\n"
		"index = origin, product, level\n"
		"unique = origin, product, level, timerange, area, ensemble, reftime\n"
	);
	ensure(test.get() != 0);
	Pending p;

	metadata::Collector src;
	scan::scan("inbound/test.grib1", src);
	ensure_equals(src.size(), 3u);

	test->open();
	p = test->beginTransaction();
	
	// Index two metadata in one file
	test->index(md, "test-md", 0);
	test->index(md1, "test-md", 10);

	// Index three other metadata in a separate file
	test->index(src[0], "test-md1", 0);
	test->index(src[1], "test-md1", 10);
	test->index(src[2], "test-md1", 20);

	p.commit();

	// Get the metadata corresponding to one file
	metadata::Collector mdc;
	test->scan_file("test-md", mdc);

	ensure_equals(mdc.size(), 2u);

	// Check that the metadata came out fine
	mdc[0].unset(types::TYPE_ASSIGNEDDATASET);
	mdc[0].source = md.source;
	ensure(mdc[0] == md);

	mdc[1].unset(types::TYPE_ASSIGNEDDATASET);
	mdc[1].source = md1.source;
	ensure(mdc[1] == md1);
}

// Try a summary query that used to be badly generated
template<> template<>
void to::test<5>()
{
	// Remove index if it exists
	unlink("file1");

	auto_ptr<WIndex> test = createIndex<WIndex>(
		"type = ondisk2\n"
		"path = \n"
		"indexfile = file1\n"
		"index = origin, product, level\n"
		"unique = origin, product, level, timerange, area, ensemble, reftime\n"
	);
	ensure(test.get() != 0);
	Pending p;

	test->open();
	p = test->beginTransaction();
	
	// Index some metadata
	test->index(md, "test-md", 0);
	test->index(md1, "test-md1", 0);
	Metadata md2;
	md2.create();
	md2.source = source::Blob::create("grib", "antani3", 10, 2000);
	md2.set(origin::GRIB1::create(202, 12, 102));
	md2.set(product::GRIB1::create(3, 4, 5));
	md2.set(level::GRIB1::create(1, 2));
	md2.set(timerange::GRIB1::create(4, 5, 6, 7));
	md2.set(reftime::Position::create(types::Time::create(2005, 1, 15, 12, 0, 0)));
	md2.set(area::GRIB::create(testArea));
	md2.set(ensemble::GRIB::create(testEnsemble));
	test->index(md2, "test-md1", 0);

	p.commit();

	// Query an interval with a partial month only
	Summary summary;
	test->querySummary(Matcher::parse("reftime:>=2005-01-10,<=2005-01-25"), summary);
	ensure_equals(summary.count(), 1u);

	// Query an interval with a partial month and a full month
	summary.clear();
	test->querySummary(Matcher::parse("reftime:>=2004-12-10,<=2005-01-31"), summary);
	ensure_equals(summary.count(), 1u);

	// Query an interval with a full month and a partial month
	summary.clear();
	test->querySummary(Matcher::parse("reftime:>=2005-01-01,<=2005-02-15"), summary);
	ensure_equals(summary.count(), 1u);

	// Query an interval with a partial month, a full month and a partial month
	summary.clear();
	test->querySummary(Matcher::parse("reftime:>=2004-12-10,<=2005-02-15"), summary);
	ensure_equals(summary.count(), 1u);

}

// Trying indexing a few metadata in a large file
template<> template<>
void to::test<6>()
{
	// Pretend the data is in a very big file
	md.source = source::Blob::create("grib", "antani", 0x100000000, 2000);
	md1.source = source::Blob::create("grib", "blinda", 0xFFFFffffFFFF0000, 0xFFFF);

	// Remove index if it exists
	unlink("file1");

	auto_ptr<WIndex> test = createIndex<WIndex>(
		"type = ondisk2\n"
		"path = .\n"
		"indexfile = file1\n"
		"index = origin, product, level\n"
		"unique = origin, product, level, timerange, area, ensemble, reftime\n"
	);
	ensure(test.get() != 0);
	Pending p;

	test->open();
	p = test->beginTransaction();
	
	// Index the two metadata
	test->index(md, "test-md", md.source.upcast<source::Blob>()->offset);
	test->index(md1, "test-md1", md1.source.upcast<source::Blob>()->offset);

	// Query various kinds of metadata
	metadata::Collector mdc;
	test->query(Matcher::parse(""), mdc);

	Item<source::Blob> s = mdc[0].source.upcast<source::Blob>();
	ensure_equals(s->offset, 0xFFFFffffFFFF0000u);
	ensure_equals(s->size, 0xFFFFu);

	s = mdc[1].source.upcast<source::Blob>();
	ensure_equals(s->offset, 0x100000000u);
	ensure_equals(s->size, 2000u);

	// TODO: level, timerange, area, ensemble, reftime
	p.commit();
}

}

// vim:set ts=4 sw=4:
