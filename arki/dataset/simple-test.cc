#include "tests.h"
#include "arki/matcher.h"

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
            type = simple
            step = daily
        )");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test("arki_dataset_esimple");

void Tests::register_tests() {

#if 0
// Test accessing the data
add_method("read", [](Fixture& f) {
    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, Matcher::parse("origin:GRIB1 or BUFR or GRIB2"));
    wassert(actual(mdc.size()) == 0u);
});

// Test acquiring the data
add_method("write", [](Fixture& f) {
    // Import data into the datasets
    f.clean_and_import();

    // Ensure that nothing can be read back
    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, Matcher::parse("origin:GRIB1 or BUFR or GRIB2"));
    wassert(actual(mdc.size()) == 0u);
});
#endif

}

}

