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
#include <arki/configfile.h>
#include <arki/matcher.h>

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
using namespace arki::dataset;
using namespace arki::types;

#if 0
// Create a dataset index gived its configuration
template<typename INDEX>
auto_ptr<INDEX> createIndex(const std::string& config)
{
	stringstream confstream(config);
	ConfigFile cfg;
	cfg.parse(confstream, "(memory)");
	return auto_ptr<INDEX>(new INDEX(cfg));
}
#endif

struct arki_dataset_ondisk2_index_shar {
#if 0
	Metadata md;
	Metadata md1;

	ValueBag testArea;
	ValueBag testEnsemble;

	arki_dsindex_shar()
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
		md1.notes.push_back(types::Note::create("this is another test"));
		out.open("test-md1.metadata");
		if (out.fail()) throw wibble::exception::File("test-md1.metadata", "opening file");
		md1.write(out, "test-md1.metadata");
		out.close();
	}
#endif
};
TESTGRP(arki_dataset_ondisk2_index);

#if 0
struct MetadataCollector : public vector<Metadata>, public MetadataConsumer
{
	bool operator()(Metadata& md)
	{
		push_back(md);
		return true;
	}
};
#endif

// Trying indexing a few metadata
template<> template<>
void to::test<1>()
{
#if 0
	auto_ptr<WIndex> test = createIndex<WIndex>(
		"type = test\n"
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
	string id = test->id(md);
	ensure(id != "");
	test->index(md, "test-md", 0);

	// Index a second one
	id = test->id(md1);
	ensure(id != "");
	test->index(md1, "test-md1", 0);

	// Query various kinds of metadata
	MetadataCollector mdc;
	test->query(Matcher::parse("origin:GRIB1,200"), mdc);
	ensure_equals(mdc.size(), 1u);

	mdc.clear();
	test->query(Matcher::parse("product:GRIB1,3"), mdc);
	ensure_equals(mdc.size(), 1u);

	// TODO: level, timerange, area, ensemble, reftime
	p.commit();
#endif
}

// See if remove works
template<> template<>
void to::test<2>()
{
#if 0
	auto_ptr<WIndex> test = createIndex<WIndex>(
		"type = test\n"
		"path = .\n"
		"indexfile = file1\n"
		"unique = origin, product, level, timerange, area, ensemble, reftime\n"
		"index = origin, product, level, timerange, area, ensemble, reftime\n"
	);
	ensure(test.get() != 0);
	Pending p;

	test->open();
	p = test->beginTransaction();
	
	// Index a metadata
	string id1 = test->id(md);
	test->index(md, "test-md", 0);

	// Index it again and ensure that it fails
	try {
		test->index(md, "test-md", 0);
		ensure(false);
	} catch (Index::DuplicateInsert& e) {
	}

	// Index a second one
	string id2 = test->id(md1);
	test->index(md1, "test-md1", 0);

	// Ensure that we have two items
	MetadataCollector mdc;
	test->query(Matcher::parse("origin:GRIB1"), mdc);
	ensure_equals(mdc.size(), 2u);
	mdc.clear();

	// Remove a nonexisting item and see that it fails
	string file;
	size_t ofs;
	test->remove("not a valid id", file, ofs);

	// Remove the first item
	test->remove(id1, file, ofs);
	ensure((bool)p);
	ensure_equals(file, "test-md");
	ensure_equals(ofs, 0u);
	p.commit();

	// There should be only one result now
	test->query(Matcher::parse("origin:GRIB1"), mdc);
	ensure_equals(mdc.size(), 1u);

	// It should be the second item we inserted
	ensure_equals(test->id(mdc[0]), id2);
	mdc.clear();

	// Replace it with a different one
	test->replace(md1, "test-md", 0);

	// See that it changed
	test->query(Matcher::parse("origin:GRIB1"), mdc);
	ensure_equals(mdc.size(), 1u);
	ensure_equals(test->id(mdc[0]), id1);

	p.commit();
#endif
}

#if 0
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

// Test concurrent read and update
template<> template<>
void to::test<3>()
{
	string cfg = 
		"type = test\n"
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
}
#endif

}

// vim:set ts=4 sw=4:
