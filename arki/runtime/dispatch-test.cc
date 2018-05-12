#include "dispatch.h"
#include "arki/dataset/tests.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::runtime;
using namespace arki::utils;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type = ondisk2
            step = daily
            index = origin, reftime
            unique = reftime, origin, product, level, timerange, area
        )");

        GRIBData fixture;
        wassert(import_all(fixture.mds));
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_runtime_dispatch");

void Tests::register_tests() {

add_method("empty", [](Fixture& f) {
});

}

}
