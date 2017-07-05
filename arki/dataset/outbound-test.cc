#include "config.h"
#include <arki/dataset/tests.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/dataset/outbound.h>
#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::tests;

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type=outbound
            step=daily
        )");
    }
};


class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_dataset_outbound");


void Tests::register_tests() {

// Test acquiring the data
add_method("import", [](Fixture& f) {
    // Import data into the datasets
    metadata::Collection mdc("inbound/test.grib1");

    auto writer = f.config().create_writer();
    wassert(actual(writer->acquire(mdc[0])) == dataset::Writer::ACQ_OK);
    wassert(actual(writer->acquire(mdc[1])) == dataset::Writer::ACQ_OK);
    wassert(actual(writer->acquire(mdc[2])) == dataset::Writer::ACQ_OK);
});

add_method("testacquire", [](Fixture& f) {
    metadata::Collection mdc("inbound/test.grib1");
    stringstream ss;
    wassert(actual(outbound::Writer::testAcquire(f.cfg, mdc[0], ss)) == dataset::Writer::ACQ_OK);

    f.cfg.setValue("archive age", "1");
    wassert(actual(outbound::Writer::testAcquire(f.cfg, mdc[0], ss)) == dataset::Writer::ACQ_ERROR);

    f.cfg.setValue("delete age", "1");
    wassert(actual(outbound::Writer::testAcquire(f.cfg, mdc[0], ss)) == dataset::Writer::ACQ_OK);
});

}

}
