#include "arki/metadata/tests.h"
#include "arki/exceptions.h"
#include "arki/dataset.h"
#include "arki/dataset/index/contents.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/configfile.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/iotrace.h"
#include "arki/utils/sys.h"
#include "arki/wibble/sys/process.h"
#include "arki/wibble/sys/childprocess.h"
#include <memory>
#include <sys/fcntl.h>
#include <unistd.h>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::dataset::index;
using namespace arki::types;
using namespace arki::utils;

// Create a dataset index gived its configuration
template<typename INDEX>
static inline unique_ptr<INDEX> createIndex(const std::string& config)
{
    ConfigFile cfg;
    cfg.parse(config);
    return unique_ptr<INDEX>(new INDEX(cfg));
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
        {
            File out("test-md.metadata", O_WRONLY | O_CREAT | O_TRUNC, 0666);
            md.write(out);
            out.close();
        }

        md1.set_source(Source::createBlob("grib", "", "blinda", 20, 40000));
        md1.set("origin", "GRIB1(201, 11, 3)");
        md1.set("product", "GRIB1(102, 103, 104)");
        md1.set("level", "GRIB1(1, 3)");
        md1.set("timerange", "GRIB1(4, 6s, 6s)");
        md1.set("reftime", "2003-04-05T06:07:08Z");
        md1.set("area", "GRIB(foo=5,bar=5000,baz=-200)");
        md1.set("proddef", "GRIB(foo=5,bar=5000,baz=-200)");
        {
            // Index one without notes
            File out("test-md1.metadata", O_WRONLY | O_CREAT | O_TRUNC, 0666);
            md1.write(out);
            out.close();
        }
    }
};
TESTGRP(arki_dataset_index_contents);

void query_index(WContents& idx, const dataset::DataQuery& q, metadata::Collection& dest)
{
    idx.query_data(q, dest.inserter_func());
}

// Trying indexing a few metadata
def_test(1)
{
	unique_ptr<WContents> test = createIndex<WContents>(
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
    query_index(*test, Matcher::parse("origin:GRIB1,200"), mdc);
    ensure_equals(mdc.size(), 1u);
    ensure_equals(mdc[0].notes().size(), 1u);
    ensure_equals(mdc[0].notes()[0].content, "this is a test");

    mdc.clear();
    query_index(*test, Matcher::parse("product:GRIB1,3"), mdc);
    ensure_equals(mdc.size(), 1u);

    // TODO: level, timerange, area, proddef, reftime
    p.commit();
}

// See if remove works
def_test(2)
{
	unique_ptr<WContents> test = createIndex<WContents>(
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
    query_index(*test, Matcher::parse("origin:GRIB1"), mdc);
    ensure_equals(mdc.size(), 2u);
    mdc.clear();

    // Remove a nonexisting item and see that it fails
    try {
        test->remove("test-md1", 1);
        ensure(false);
    } catch (std::runtime_error) {
    }

    // Remove the first item
    test->remove("test-md", 0);
    ensure((bool)p);
    p.commit();

    // There should be only one result now
    query_index(*test, Matcher::parse("origin:GRIB1"), mdc);
    ensure_equals(mdc.size(), 1u);

	// It should be the second item we inserted
	ensure_equals(test->id(mdc[0]), id1);
	mdc.clear();

	// Replace it with a different one
	test->replace(md1, "test-md", 0);

    // See that it changed
    query_index(*test, Matcher::parse("origin:GRIB1"), mdc);
    ensure_equals(mdc.size(), 1u);
    const source::Blob& blob = mdc[0].sourceBlob();
    ensure_equals(blob.filename, "test-md");

	p.commit();
}

namespace {
struct ReadHang : public wibble::sys::ChildProcess
{
	ConfigFile cfg;
	int commfd;

    ReadHang(const std::string& cfgstr)
    {
        cfg.parse(cfgstr);
    }

    int main() override
    {
        try {
            RContents idx(cfg);
            idx.open();
            idx.query_data(Matcher::parse("origin:GRIB1"), [&](unique_ptr<Metadata> md) {
                fputs("H\n", stdout);
                usleep(100000);
                return true;
            });
        } catch (std::exception& e) {
            fprintf(stderr, "%s\n", e.what());
            fputs("E\n", stdout);
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
            throw_system_error("reading 1 byte from child process");
        return buf[0];
    }
};
}

// Test concurrent read and update
def_test(3)
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
		unique_ptr<WContents> test1 = createIndex<WContents>(cfg);
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
		unique_ptr<WContents> test1 = createIndex<WContents>(cfg);
		test1->open();
		Pending p = test1->beginTransaction();
		test1->index(md3, "test-md1", 0);
		p.commit();
	}

	readHang.kill(9);
	readHang.wait();
}

// Test getting the metadata corresponding to a file
def_test(4)
{
	// Remove index if it exists
	unlink("file1");

	unique_ptr<WContents> test = createIndex<WContents>(
		"type = ondisk2\n"
		"path = .\n"
		"indexfile = file1\n"
		"index = origin, product, level\n"
		"unique = origin, product, level, timerange, area, proddef, reftime\n"
	);
	ensure(test.get() != 0);
	Pending p;

    metadata::Collection src;
    scan::scan("inbound/test.grib1", src.inserter_func());
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
    test->scan_file("test-md", mdc.inserter_func());
    ensure_equals(mdc.size(), 2u);

    // Check that the metadata came out fine
    mdc[0].unset(TYPE_ASSIGNEDDATASET);
    mdc[0].set_source(unique_ptr<Source>(md.source().clone()));
    ensure(mdc[0] == md);

    mdc[1].unset(TYPE_ASSIGNEDDATASET);
    mdc[1].set_source(unique_ptr<Source>(md1.source().clone()));
    ensure(mdc[1] == md1);
}

// Try a summary query that used to be badly generated
def_test(5)
{
	// Remove index if it exists
	unlink("file1");

	unique_ptr<WContents> test = createIndex<WContents>(
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
    test->query_summary(Matcher::parse("reftime:>=2005-01-10,<=2005-01-25"), summary);
    ensure_equals(summary.count(), 1u);

    // Query an interval with a partial month and a full month
    summary.clear();
    test->query_summary(Matcher::parse("reftime:>=2004-12-10,<=2005-01-31"), summary);
    ensure_equals(summary.count(), 1u);

    // Query an interval with a full month and a partial month
    summary.clear();
    test->query_summary(Matcher::parse("reftime:>=2005-01-01,<=2005-02-15"), summary);
    ensure_equals(summary.count(), 1u);

    // Query an interval with a partial month, a full month and a partial month
    summary.clear();
    test->query_summary(Matcher::parse("reftime:>=2004-12-10,<=2005-02-15"), summary);
    ensure_equals(summary.count(), 1u);

}

// Trying indexing a few metadata in a large file
def_test(6)
{
    // Pretend the data is in a very big file
    md.set_source(Source::createBlob("grib", "", "antani", 0x100000000LLU, 2000));
    md1.set_source(Source::createBlob("grib", "", "blinda", 0xFFFFffffFFFF0000LLU, 0xFFFF));

	// Remove index if it exists
	unlink("file1");

	unique_ptr<WContents> test = createIndex<WContents>(
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
    query_index(*test, Matcher(), mdc);

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
def_test(7)
{
    metadata::Collection src;
    scan::scan("inbound/test.vm2", src.inserter_func());

    // Remove index if it exists
    unlink("file1");

    {
        iotrace::Collector collector;

        // An index without large files
        unique_ptr<WContents> test = createIndex<WContents>(
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
        query_index(*test, Matcher(), mdc);

        // 'value' should not have been preserved
        wassert(actual(mdc.size()) == 1u);
        wassert(actual_type(mdc[0].source()).is_source_blob("vm2", wibble::sys::process::getcwd(), "inbound/test.vm2", 0, 34));
        wassert(actual(mdc[0]).contains("product", "VM2(227)"));
        wassert(actual(mdc[0]).contains("reftime", "1987-10-31T00:00:00Z"));
        wassert(actual(mdc[0]).contains("area", "VM2(1)"));
        wassert(actual(mdc[0]).is_not_set("value"));

        // I/O should happen here
        mdc[0].drop_cached_data();
        const auto& buf = mdc[0].getData();
        wassert(actual(string((const char*)buf.data(), buf.size())) == "198710310000,1,227,1.2,,,000000000");
        wassert(actual(collector.events.size()) == 1u);
        wassert(actual(collector.events[0].filename()).endswith("inbound/test.vm2"));
    }

    // Remove index if it exists
    unlink("file1");

    {
        iotrace::Collector collector;

        // An index without large files
        unique_ptr<WContents> test = createIndex<WContents>(
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
        query_index(*test, Matcher(), mdc);

        // 'value' should have been preserved
        wassert(actual(mdc.size()) == 1u);
        wassert(actual_type(mdc[0].source()).is_source_blob("vm2", wibble::sys::process::getcwd(), "inbound/test.vm2", 0, 34));
        wassert(actual(mdc[0]).contains("product", "VM2(227)"));
        wassert(actual(mdc[0]).contains("reftime", "1987-10-31T00:00:00Z"));
        wassert(actual(mdc[0]).contains("area", "VM2(1)"));
        wassert(actual(mdc[0]).contains("value", "1.2,,,000000000"));

        // No I/O should happen here
        mdc[0].drop_cached_data();
        const auto& buf = mdc[0].getData();
        wassert(actual(string((const char*)buf.data(), buf.size())) == "198710310000,1,227,1.2,,,000000000");
        wassert(actual(collector.events.size()) == 0u);
    }
}

}
