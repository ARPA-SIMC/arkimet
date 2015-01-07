/*
 * Copyright (C) 2007--2015 ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types/tests.h>
#include <arki/metadata/tests.h>
#include <arki/dataset.h>
#include <arki/dataset/index/contents.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/types/source/blob.h>
#include <arki/scan/any.h>
#include <arki/configfile.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/iotrace.h>

#include <wibble/sys/process.h>
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
using namespace wibble::tests;
using namespace arki;
using namespace arki::dataset::index;
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

struct arki_dataset_index_contents_shar {
	Metadata md;
	Metadata md1;

    arki_dataset_index_contents_shar()
    {
        iotrace::init();

        md.set_source(Source::createBlob("grib", "", "antani", 10, 2000));
        md.set("origin", "GRIB1(200, 10, 100)");
        md.set("product", "GRIB1(3, 4, 5)");
        md.set("level", "GRIB1(1, 2)");
        md.set("timerange", "GRIB1(4, 5s, 6s)");
        md.set("reftime", "2006-05-04T03:02:01Z");
        md.set("area", "GRIB(foo=5,bar=5000,baz=-200)");
        md.set("proddef", "GRIB(foo=5,bar=5000,baz=-200)");
        md.add_note("this is a test");
		ofstream out("test-md.metadata");
		if (out.fail()) throw wibble::exception::File("test-md.metadata", "opening file");
		md.write(out, "test-md.metadata");
		out.close();

        md1.set_source(Source::createBlob("grib", "", "blinda", 20, 40000));
        md1.set("origin", "GRIB1(201, 11, 3)");
        md1.set("product", "GRIB1(102, 103, 104)");
        md1.set("level", "GRIB1(1, 3)");
        md1.set("timerange", "GRIB1(4, 6s, 6s)");
        md1.set("reftime", "2003-04-05T06:07:08Z");
        md1.set("area", "GRIB(foo=5,bar=5000,baz=-200)");
        md1.set("proddef", "GRIB(foo=5,bar=5000,baz=-200)");
		// Index one without notes
		//md1.notes.push_back("this is another test");
		out.open("test-md1.metadata");
		if (out.fail()) throw wibble::exception::File("test-md1.metadata", "opening file");
		md1.write(out, "test-md1.metadata");
		out.close();
	}
};
TESTGRP(arki_dataset_index_contents);

// Trying indexing a few metadata
template<> template<>
void to::test<1>()
{
	auto_ptr<WContents> test = createIndex<WContents>(
		"type = ondisk2\n"
		"path = .\n"
		"indexfile = :memory:\n"
		"index = origin, product, level\n"
		"unique = origin, product, level, timerange, area, proddef, reftime\n"
	);
	ensure(test.get() != 0);
	Pending p;

	test->open();
	p = test->beginTransaction();
	
	// Index a metadata
	int id = test->id(md);
	ensure_equals(id, -1);
	test->index(md, "test-md", 0, &id);
	ensure_equals(id, 1);
	ensure_equals(test->id(md), 1);

	// Index a second one
	id = test->id(md1);
	ensure_equals(id, -1);
	test->index(md1, "test-md1", 0, &id);
	ensure_equals(id, 2);
	ensure_equals(test->id(md1), 2);

	// Query various kinds of metadata
	metadata::Collection mdc;
	test->query(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
	ensure_equals(mdc.size(), 1u);
	ensure_equals(mdc[0].notes().size(), 1u);
	ensure_equals(mdc[0].notes()[0].content, "this is a test");

	mdc.clear();
	test->query(dataset::DataQuery(Matcher::parse("product:GRIB1,3")), mdc);
	ensure_equals(mdc.size(), 1u);

	// TODO: level, timerange, area, proddef, reftime
	p.commit();
}

// See if remove works
template<> template<>
void to::test<2>()
{
	auto_ptr<WContents> test = createIndex<WContents>(
		"type = ondisk2\n"
		"path = .\n"
		"indexfile = :memory:\n"
		"unique = origin, product, level, timerange, area, proddef, reftime\n"
		"index = origin, product, level, timerange, area, proddef, reftime\n"
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
	} catch (utils::sqlite::DuplicateInsert& e) {
	}

	// Index a second one
	test->index(md1, "test-md1", 0);
	int id1 = test->id(md1);

	// Ensure that we have two items
	metadata::Collection mdc;
	test->query(dataset::DataQuery(Matcher::parse("origin:GRIB1")), mdc);
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
	test->query(dataset::DataQuery(Matcher::parse("origin:GRIB1")), mdc);
	ensure_equals(mdc.size(), 1u);

	// It should be the second item we inserted
	ensure_equals(test->id(mdc[0]), id1);
	mdc.clear();

	// Replace it with a different one
	test->replace(md1, "test-md", 0);

	// See that it changed
	test->query(dataset::DataQuery(Matcher::parse("origin:GRIB1")), mdc);
	ensure_equals(mdc.size(), 1u);
    const source::Blob& blob = mdc[0].sourceBlob();
    ensure_equals(blob.filename, "test-md");

	p.commit();
}

namespace {
struct ReadHang : public sys::ChildProcess, public metadata::Consumer
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
			RContents idx(cfg);
			idx.open();
			idx.query(dataset::DataQuery(Matcher::parse("origin:GRIB1")), *this);
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
		"unique = origin, product, level, timerange, area, proddef, reftime\n"
		"index = origin, product, level, timerange, area, proddef, reftime\n";

	// Remove index if it exists
	unlink("file1");

	// Create the index and index two metadata
	{
		auto_ptr<WContents> test1 = createIndex<WContents>(cfg);
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
    md3.set_source(Source::createBlob("grib", "", "antani3", 10, 2000));
    md3.set("origin", "GRIB1(202, 12, 102)");
    md3.set("product", "GRIB1(3, 4, 5)");
    md3.set("level", "GRIB1(1, 2)");
    md3.set("timerange", "GRIB1(4, 5s, 6s)");
    md3.set("reftime", "2006-05-04T03:02:01Z");
    md3.set("area", "GRIB(foo=5,bar=5000,baz=-200)");
    md3.set("proddef", "GRIB(foo=5,bar=5000,baz=-200)");
    md3.add_note("this is a test");
	{
		auto_ptr<WContents> test1 = createIndex<WContents>(cfg);
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

	auto_ptr<WContents> test = createIndex<WContents>(
		"type = ondisk2\n"
		"path = .\n"
		"indexfile = file1\n"
		"index = origin, product, level\n"
		"unique = origin, product, level, timerange, area, proddef, reftime\n"
	);
	ensure(test.get() != 0);
	Pending p;

	metadata::Collection src;
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
	metadata::Collection mdc;
	test->scan_file("test-md", mdc);

	ensure_equals(mdc.size(), 2u);

    // Check that the metadata came out fine
    mdc[0].unset(TYPE_ASSIGNEDDATASET);
    mdc[0].set_source(auto_ptr<Source>(md.source().clone()));
    ensure(mdc[0] == md);

    mdc[1].unset(TYPE_ASSIGNEDDATASET);
    mdc[1].set_source(auto_ptr<Source>(md1.source().clone()));
    ensure(mdc[1] == md1);
}

// Try a summary query that used to be badly generated
template<> template<>
void to::test<5>()
{
	// Remove index if it exists
	unlink("file1");

	auto_ptr<WContents> test = createIndex<WContents>(
		"type = ondisk2\n"
		"path = \n"
		"indexfile = file1\n"
		"index = origin, product, level\n"
		"unique = origin, product, level, timerange, area, proddef, reftime\n"
	);
	ensure(test.get() != 0);
	Pending p;

	test->open();
	p = test->beginTransaction();

    // Index some metadata
    test->index(md, "test-md", 0);
    test->index(md1, "test-md1", 0);
    Metadata md2;
    md2.set_source(Source::createBlob("grib", "", "antani3", 10, 2000));
    md2.set("origin", "GRIB1(202, 12, 102)");
    md2.set("product", "GRIB1(3, 4, 5)");
    md2.set("level", "GRIB1(1, 2)");
    md2.set("timerange", "GRIB1(4, 5s, 6s)");
    md2.set("reftime", "2005-01-15T12:00:00Z");
    md2.set("area", "GRIB(foo=5,bar=5000,baz=-200)");
    md2.set("proddef", "GRIB(foo=5,bar=5000,baz=-200)");
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
    md.set_source(Source::createBlob("grib", "", "antani", 0x100000000LLU, 2000));
    md1.set_source(Source::createBlob("grib", "", "blinda", 0xFFFFffffFFFF0000LLU, 0xFFFF));

	// Remove index if it exists
	unlink("file1");

	auto_ptr<WContents> test = createIndex<WContents>(
		"type = ondisk2\n"
		"path = .\n"
		"indexfile = file1\n"
		"index = origin, product, level\n"
		"unique = origin, product, level, timerange, area, proddef, reftime\n"
	);
	ensure(test.get() != 0);
	Pending p;

	test->open();
	p = test->beginTransaction();

    // Index the two metadata
    test->index(md, "test-md", md.sourceBlob().offset);
    test->index(md1, "test-md1", md1.sourceBlob().offset);

	// Query various kinds of metadata
	metadata::Collection mdc;
	test->query(dataset::DataQuery(Matcher::parse("")), mdc);

    const source::Blob& s0 = mdc[0].sourceBlob();
    ensure_equals(s0.offset, 0xFFFFffffFFFF0000LLU);
    ensure_equals(s0.size, 0xFFFFu);

    const source::Blob& s1 = mdc[1].sourceBlob();
    ensure_equals(s1.offset, 0x100000000LLU);
    ensure_equals(s1.size, 2000u);

    // TODO: level, timerange, area, proddef, reftime
    p.commit();
}

// Test smallfiles support
template<> template<>
void to::test<7>()
{
    metadata::Collection src;
    scan::scan("inbound/test.vm2", src);

    // Remove index if it exists
    unlink("file1");

    {
        iotrace::Collector collector;

        // An index without large files
        auto_ptr<WContents> test = createIndex<WContents>(
                "type = ondisk2\n"
                "path = .\n"
                "indexfile = file1\n"
                "index = origin, product, level\n"
                "unique = origin, product, level, timerange, area, proddef, reftime\n"
        );
        ensure(test.get() != 0);
        test->open();

        // Insert a metadata
        Pending p = test->beginTransaction();
        test->index(src[0], "inbound/test.vm2", src[0].sourceBlob().offset);
        p.commit();

        // Query it back
        metadata::Collection mdc;
        test->query(dataset::DataQuery(Matcher::parse("")), mdc);

        // 'value' should not have been preserved
        wassert(actual(mdc.size()) == 1u);
        wassert(actual_type(mdc[0].source()).is_source_blob("vm2", sys::process::getcwd(), "inbound/test.vm2", 0, 34));
        wassert(actual(mdc[0]).contains("product", "VM2(227)"));
        wassert(actual(mdc[0]).contains("reftime", "1987-10-31T00:00:00Z"));
        wassert(actual(mdc[0]).contains("area", "VM2(1)"));
        wassert(actual(mdc[0]).is_not_set("value"));

        // I/O should happen here
        mdc[0].source().dropCachedData();
        sys::Buffer buf = mdc[0].getData();
        wassert(actual(string((const char*)buf.data(), buf.size())) == "198710310000,1,227,1.2,,,000000000");
        wassert(actual(collector.events.size()) == 1u);
        wassert(actual(collector.events[0].filename()).endswith("inbound/test.vm2"));
    }

    // Remove index if it exists
    unlink("file1");

    {
        iotrace::Collector collector;

        // An index without large files
        auto_ptr<WContents> test = createIndex<WContents>(
                "type = ondisk2\n"
                "path = .\n"
                "indexfile = file1\n"
                "index = origin, product, level\n"
                "unique = origin, product, level, timerange, area, proddef, reftime\n"
                "smallfiles = yes\n"
        );
        ensure(test.get() != 0);
        test->open();

        // Insert a metadata
        Pending p = test->beginTransaction();
        test->index(src[0], "inbound/test.vm2", src[0].sourceBlob().offset);
        p.commit();

        // Query it back
        metadata::Collection mdc;
        test->query(dataset::DataQuery(Matcher::parse("")), mdc);

        // 'value' should have been preserved
        wassert(actual(mdc.size()) == 1u);
        wassert(actual_type(mdc[0].source()).is_source_blob("vm2", sys::process::getcwd(), "inbound/test.vm2", 0, 34));
        wassert(actual(mdc[0]).contains("product", "VM2(227)"));
        wassert(actual(mdc[0]).contains("reftime", "1987-10-31T00:00:00Z"));
        wassert(actual(mdc[0]).contains("area", "VM2(1)"));
        wassert(actual(mdc[0]).contains("value", "1.2,,,000000000"));

        // No I/O should happen here
        mdc[0].source().dropCachedData();
        sys::Buffer buf = mdc[0].getData();
        wassert(actual(string((const char*)buf.data(), buf.size())) == "198710310000,1,227,1.2,,,000000000");
        wassert(actual(collector.events.size()) == 0u);
    }
}

}

// vim:set ts=4 sw=4:
