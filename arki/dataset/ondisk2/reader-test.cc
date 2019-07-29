#include <arki/dataset/tests.h>
#include <arki/dataset/ondisk2.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/sys.h>
#include <arki/iotrace.h>
#include <unistd.h>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type = ondisk2
            step = daily
        )");

        clean_and_import();
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_dataset_ondisk2_reader");

void Tests::register_tests() {

// Add here only simple-specific tests that are not convered by tests in dataset-reader-test.cc

// Test that summary files are not created for all the extent of the query, but
// only for data present in the dataset
add_method("summaries", [](Fixture& f) {
    // Empty the summary cache
    //sys::rmtree("testds/.summaries");
    system("rm -f testds/.summaries/*");

    auto reader = f.makeSegmentedReader();

    Summary s;
    reader->query_summary(Matcher::parse("reftime:=2007"), s);
    wassert(actual(s.count()) == 3u);
    wassert(actual(s.size()) == 44412u);

    // Global summary is not built because we only query specific months
    wassert(actual_file("testds/.summaries/all.summary").not_exists());

    wassert(actual_file("testds/.summaries/2007-01.summary").not_exists());
    wassert(actual_file("testds/.summaries/2007-02.summary").not_exists());
    wassert(actual_file("testds/.summaries/2007-03.summary").not_exists());
    wassert(actual_file("testds/.summaries/2007-04.summary").not_exists());
    wassert(actual_file("testds/.summaries/2007-05.summary").not_exists());
    wassert(actual_file("testds/.summaries/2007-06.summary").not_exists());
    wassert(actual_file("testds/.summaries/2007-07.summary").exists());
    // Summary caches corresponding to DB holes are still created and used
    wassert(actual_file("testds/.summaries/2007-08.summary").exists());
    wassert(actual_file("testds/.summaries/2007-09.summary").exists());
    wassert(actual_file("testds/.summaries/2007-10.summary").exists());
    wassert(actual_file("testds/.summaries/2007-11.summary").not_exists());
    wassert(actual_file("testds/.summaries/2007-12.summary").not_exists());
    wassert(actual_file("testds/.summaries/2008-01.summary").not_exists());
});

}

}
