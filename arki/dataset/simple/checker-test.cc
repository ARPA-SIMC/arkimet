#include "arki/dataset/tests.h"
#include "arki/core/file.h"
#include "arki/dataset/query.h"
#include "arki/dataset/simple/checker.h"
#include "arki/dataset/simple/writer.h"
#include "arki/dataset/simple/reader.h"
#include "arki/types/source/blob.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;
using namespace arki::tests;
using namespace arki::utils;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type=simple
            step=daily
        )");
    }

    // Recreate the dataset importing data into it
    void clean_and_import(const std::string& testfile="inbound/test.grib1")
    {
        DatasetTest::clean_and_import(testfile);
        wassert(ensure_localds_clean(3, 3));
    }
};


class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_dataset_simple_checker");

void Tests::register_tests() {

// Add here only simple-specific tests that are not convered by tests in dataset-writer-test.cc

// Test maintenance scan on missing summary
add_method("scan_missing_summary", [](Fixture& f) {
    struct Setup {
        void operator() ()
        {
            std::filesystem::remove("testds/2007/07-08.grib.summary");
            wassert(actual_file("testds/2007/07-08.grib").exists());
            wassert(actual_file("testds/2007/07-08.grib.metadata").exists());
            wassert(actual_file("testds/2007/07-08.grib.summary").not_exists());
        }
    } setup;

    f.clean_and_import();
    setup();
    wassert(actual_file("testds/" + f.idxfname()).exists());

    // Query is ok
    {
        metadata::Collection mdc(*f.makeSimpleReader(), Matcher());
        wassert(actual(mdc.size()) == 3u);
    }

    // Maintenance should show one file to rescan
    {
        auto state = f.scan_state();
        wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::SEGMENT_UNALIGNED);
        wassert(actual(state.count(segment::SEGMENT_OK)) == 2u);
        wassert(actual(state.count(segment::SEGMENT_UNALIGNED)) == 1u);
        wassert(actual(state.size()) == 3u);
    }

    // Fix the dataset
    {
        auto checker = f.makeSimpleChecker();

        // Check should reindex the file
        ReporterExpected e;
        e.rescanned.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(*checker).check(e, true, true));

        // Repack should do nothing
        wassert(actual(*checker).repack_clean(true));
    }

    // Everything should be fine now
    wassert(f.ensure_localds_clean(3, 3));
    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/07-08.grib.metadata").exists());
    wassert(actual_file("testds/2007/07-08.grib.summary").exists());
    wassert(actual_file("testds/" + f.idxfname()).exists());


    // Restart again
    f.clean_and_import();
    setup();
    files::removeDontpackFlagfile("testds");
    wassert(actual_file("testds/" + f.idxfname()).exists());

    // Repack here should act as if the dataset were empty
    wassert(actual(*f.makeSimpleChecker()).repack_clean(true));

    // And repack should have changed nothing
    {
        auto reader = f.makeSimpleReader();
        metadata::Collection mdc(*reader, Matcher());
        wassert(actual(mdc.size()) == 3u);
    }
    wassert(actual_file("testds/2007/07-08.grib").exists());
    wassert(actual_file("testds/2007/07-08.grib.metadata").exists());
    wassert(actual_file("testds/2007/07-08.grib.summary").not_exists());
    wassert(actual_file("testds/" + f.idxfname()).exists());
});

// Test maintenance scan on compressed archives
add_method("scan_compressed", [](Fixture& f) {
    auto setup = [&] {
        auto checker = f.makeSegmentedChecker();
        checker->segment("2007/07-08.grib")->compress(512);
    };

    auto removemd = []{
        std::filesystem::remove("testds/2007/07-08.grib.metadata");
        std::filesystem::remove("testds/2007/07-08.grib.summary");
        wassert(actual_file("testds/2007/07-08.grib.metadata").not_exists());
        wassert(actual_file("testds/2007/07-08.grib.summary").not_exists());
    };

    f.clean_and_import();
    setup();
    wassert(actual_file("testds/2007/07-08.grib").not_exists());
    wassert(actual_file("testds/2007/07-08.grib.gz").exists());
    // Index was not created because there is only one group in the compressed file
    wassert(actual_file("testds/2007/07-08.grib.gz.idx").not_exists());
    wassert(actual_file("testds/2007/07-08.grib.metadata").exists());
    wassert(actual_file("testds/2007/07-08.grib.summary").exists());
    wassert(actual_file("testds/" + f.idxfname()).exists());

    // Query is ok
    wassert(f.ensure_localds_clean(3, 3));

    // Try removing summary and metadata
    removemd();

    // We can still query
    {
        metadata::Collection mdc;
        auto reader = f.makeSimpleReader();
        mdc.add(*reader, Matcher());
        wassert(actual(mdc.size()) == 2u);
    }

    // Maintenance should show one file to rescan
    {
        auto state = f.scan_state();
        wassert(actual(state.get("testds:2007/07-08.grib").state) == segment::SEGMENT_UNALIGNED);
        wassert(actual(state.count(segment::SEGMENT_OK)) == 2u);
        wassert(actual(state.count(segment::SEGMENT_UNALIGNED)) == 1u);
        wassert(actual(state.size()) == 3u);
    }

    // Fix the dataset
    {
        auto checker = f.makeSimpleChecker();

        // Check should reindex the file
        ReporterExpected e;
        e.rescanned.emplace_back("testds", "2007/07-08.grib");
        wassert(actual(*checker).check(e, true, true));

        // Repack should do nothing
        wassert(actual(*checker).repack_clean(true));
    }

    // Everything should be fine now
    wassert(f.ensure_localds_clean(3, 3));
    wassert(actual_file("testds/2007/07-08.grib").not_exists());
    wassert(actual_file("testds/2007/07-08.grib.gz").exists());
    // Index was not created because there is only one group in the compressed file
    wassert(actual_file("testds/2007/07-08.grib.gz.idx").not_exists());
    wassert(actual_file("testds/2007/07-08.grib.metadata").exists());
    wassert(actual_file("testds/2007/07-08.grib.summary").exists());
    wassert(actual_file("testds/" + f.idxfname()).exists());


    // Restart again
    f.clean_and_import();
    setup();
    files::removeDontpackFlagfile("testds");
    wassert(actual_file("testds/" + f.idxfname()).exists());
    removemd();

    // Repack here should act as if the dataset were empty
    {
        // Repack should find nothing to repack
        auto checker = f.makeSimpleChecker();
        wassert(actual(*checker).repack_clean(true));
    }

    // And repack should have changed nothing
    {
        metadata::Collection mdc;
        auto reader = f.makeSimpleReader();
        mdc.add(*reader, Matcher());
        wassert(actual(mdc.size()) == 2u);
    }
    wassert(actual_file("testds/2007/07-08.grib").not_exists());
    wassert(actual_file("testds/2007/07-08.grib.gz").exists());
    // Index was not created because there is only one group in the compressed file
    wassert(actual_file("testds/2007/07-08.grib.gz.idx").not_exists());
    wassert(actual_file("testds/2007/07-08.grib.metadata").not_exists());
    wassert(actual_file("testds/2007/07-08.grib.summary").not_exists());
    wassert(actual_file("testds/" + f.idxfname()).exists());
});

}

}
