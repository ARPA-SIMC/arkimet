#include "arki/dataset/tests.h"

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::types;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type=ondisk2
            step=daily
        )");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests tests("arki_dataset_ondisk2");


void Tests::register_tests() {

// Test acquiring data
add_method("deprecation", [](Fixture& f) {
    auto ds = f.dataset_config();

    auto e1 = wassert_throws(std::runtime_error, ds->create_reader());
    wassert(actual(e1.what()).contains("ondisk2 datasets are not supported anymore"));

    auto e2 = wassert_throws(std::runtime_error, ds->create_writer());
    wassert(actual(e2.what()).contains("ondisk2 datasets are not supported anymore"));

    auto e3 = wassert_throws(std::runtime_error, ds->create_checker());
    wassert(actual(e3.what()).contains("ondisk2 datasets are not supported anymore"));
});

}
}
