#include "arki/dataset/tests.h"
#include "arki/dataset/iseg/checker.h"
#include "arki/dataset/iseg/reader.h"
#include "arki/dataset/reporter.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"

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
            type=iseg
            step=daily
            format=grib
        )");
    }
};


class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_dataset_iseg_checker");

void Tests::register_tests() {

add_method("empty", [](Fixture& f) {
});

}

}

