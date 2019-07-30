#include "arki/dataset/tests.h"
#include "arki/types/reftime.h"
#include "arki/metadata/data.h"
#include "arki/sort.h"
#include "querymacro.h"
#include "summary.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::types;

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    core::cfg::Sections dispatch_cfg;

    void test_setup()
    {
        DatasetTest::test_setup("type=ondisk2\nstep=daily\nunique=origin,reftime\n");
        dispatch_cfg.emplace("testds", cfg);
    }

    void import()
    {
        auto writer(config().create_writer());
        metadata::TestCollection mdc("inbound/test.grib1");
        for (const auto& const_md: mdc)
        {
            Metadata md = *const_md;
            for (unsigned i = 7; i <= 9; ++i)
            {
                md.set(Reftime::createPosition(Time(2009, 8, i, 0, 0, 0)));
                wassert(actual(*writer).import(md));
            }
        }
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_querymacro");

void Tests::register_tests() {

// Lua script that simply passes through the queries
add_method("empty", [](Fixture& f) {
});

}

}
