#include "tests.h"
#include "arki/matcher.h"
#include "arki/query.h"

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
            type = testlarge
        )");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test("arki_dataset_testlarge");

void Tests::register_tests() {

// Test accessing the data
add_method("read", [](Fixture& f) {
    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, "reftime:=2016-01-01");
    wassert(actual(mdc.size()) == 4u);
});

}

}
