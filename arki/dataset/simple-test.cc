#include "tests.h"
#include "arki/nag.h"

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

Tests test("arki_dataset_simple");

void Tests::register_tests() {

add_method("warn_index_type_sqlite", [](Fixture& f) {
    f.cfg->set("index_type", "sqlite");
    nag::CollectHandler nag_messages;
    nag_messages.install();
    f.config();
    wassert(actual(nag_messages.collected.size()) == 1u);
    wassert(actual(nag_messages.collected[0]).matches("W:testds: dataset has index_type=sqlite. It is now ignored, and automatically converted to plain MANIFEST"));
    nag_messages.clear();
});

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

