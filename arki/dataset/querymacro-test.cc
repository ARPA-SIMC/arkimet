#include "arki/dataset/tests.h"
#include "arki/types/reftime.h"
#include "querymacro.h"

namespace {
using namespace arki;
using namespace arki::tests;
using namespace arki::types;

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    core::cfg::Sections dispatch_cfg;

    void test_setup()
    {
        DatasetTest::test_setup("type=iseg\nformat=grib\nstep=daily\nunique=origin,reftime\n");
        dispatch_cfg.emplace("testds", cfg);
    }

    void import()
    {
        auto writer(config().create_writer());
        metadata::TestCollection mdc("inbound/test.grib1");
        for (const auto& const_md: mdc)
        {
            std::shared_ptr<Metadata> md(const_md->clone());
            for (unsigned i = 7; i <= 9; ++i)
            {
                md->test_set(Reftime::createPosition(core::Time(2009, 8, i, 0, 0, 0)));
                wassert(actual(*writer).import(*md));
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

add_method("empty", [](Fixture& f) noexcept {
});

}

}
