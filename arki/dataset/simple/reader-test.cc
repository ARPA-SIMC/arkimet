#include "config.h"

#include <arki/dataset/tests.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/simple/writer.h>
#include <arki/types/assigneddataset.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/sys.h>
#include <arki/wibble/grcal/grcal.h>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::dataset::simple;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type = simple
            step = daily
            postprocess = testcountbytes
        )");

        clean_and_import();
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test_plain("arki_dataset_simple_reader_plain");
Tests test_sqlite("arki_dataset_simple_reader_sqlite", "index_type=sqlite");

void Tests::register_tests() {

// Test querying with postprocessing
add_method("postprocess", [](Fixture& f) {
    unique_ptr<simple::Reader> reader(f.makeSimpleReader());

    dataset::ByteQuery bq;
    bq.setPostprocess(Matcher::parse("origin:GRIB1,200"), "testcountbytes");
    reader->query_bytes(bq, 2);

    string out = sys::read_file("testcountbytes.out");
    ensure_equals(out, "7399\n");
});

#if 0
// Acquire and query
def_test(1)
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

	metadata::Collection mdc;
	Metadata::readFile("testds/.archive/last/test.grib1.metadata", mdc);
	ensure_equals(mdc.size(), 3u);
	ensure_equals(mdc[0].source.upcast<source::Blob>()->filename, "test.grib1");

	// Query
	mdc.clear();
	arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 3u);

	// Maintenance should show it's all ok
	MaintenanceCollection c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	//c.dump(cerr);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());
}
#endif

#if 0
// Test handling of empty archive dirs (such as last with everything moved away)
def_test(7)
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
	metadata::Collection mdc;
	arc.queryData(dataset::DataQuery(Matcher(), false), mdc);
	ensure_equals(mdc.size(), 3u);

	// Maintenance should show that everything is ok now
	MaintenanceCollection c;
	arc.maintenance(c);
	ensure_equals(c.fileStates.size(), 1u);
	ensure_equals(c.count(ARC_OK), 1u);
	ensure_equals(c.remaining(), string());
	ensure(c.isClean());
}

// Tolerate empty dirs
def_test(8)
{
	// Start with an empty dir
	system("rm -rf testds");
	system("mkdir testds");

	simple::Reader testds(cfg);

	MetadataCollection mdc;
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

#endif

}
}
