#include "config.h"
#include <arki/dataset/tests.h>
#include <arki/dataset/iseg/reader.h>
#include <arki/dataset/iseg/writer.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/sys.h>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::dataset::iseg;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type = iseg
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

Tests test_plain("arki_dataset_iseg_reader_plain");

void Tests::register_tests() {

// Add here only iseg-specific tests that are not convered by tests in dataset-reader-test.cc

add_method("empty", [](Fixture& f) {
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

	iseg::Reader testds(cfg);

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
