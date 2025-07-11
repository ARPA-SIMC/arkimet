#include "arki/matcher.h"
#include "arki/query.h"
#include "arki/scan.h"
#include "fromfunction.h"
#include "tests.h"

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
            type = fromfunction
        )");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test("arki_dataset_fromfunction");

void Tests::register_tests()
{

    // Test accessing the data
    add_method("read", [](Fixture& f) {
        auto reader = f.config().create_reader();
        fromfunction::Reader* ff_reader =
            dynamic_cast<fromfunction::Reader*>(reader.get());
        ff_reader->generator = [&](metadata_dest_func dest) {
            metadata::TestCollection mds("inbound/test.grib1");
            return mds.move_to(dest);
        };
        metadata::Collection mdc(*reader, "origin:GRIB1 or BUFR or GRIB2");
        wassert(actual(mdc.size()) == 3u);
    });

    add_method("query", [](Fixture& f) {
        auto reader = f.config().create_reader();
        fromfunction::Reader* ff_reader =
            dynamic_cast<fromfunction::Reader*>(reader.get());
        ff_reader->generator = [&](metadata_dest_func dest) {
            metadata::TestCollection mds("inbound/test.grib1");
            return mds.move_to(dest);
        };
        metadata::Collection mdc(*reader, "origin:GRIB1,200");
        wassert(actual(mdc.size()) == 1u);
    });
}

} // namespace
