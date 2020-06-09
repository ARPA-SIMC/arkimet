#include "tests.h"
#include "arki/matcher.h"
#include "arki/dataset/query.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::dataset;
using namespace arki::utils;

struct Fixture : public DatasetTest
{
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type = discard
        )");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test("arki_dataset_empty");

void Tests::register_tests() {

// Test accessing the data
add_method("read", [](Fixture& f) {
    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, "origin:GRIB1 or BUFR or GRIB2");
    wassert(actual(mdc.size()) == 0u);
});

// Test acquiring the data
add_method("write", [](Fixture& f) {
    // Import data into the datasets
    f.clean_and_import();

    // Ensure that nothing can be read back
    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, "origin:GRIB1 or BUFR or GRIB2");
    wassert(actual(mdc.size()) == 0u);
});

}

}
