#include "config.h"
#include <arki/dataset/tests.h>
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
    metadata::TestCollection mdc("inbound/test.grib1");

    auto writer = f.config().create_writer();
    wassert(actual(*writer).import(mdc[0]));
    wassert(actual(*writer).import(mdc[1]));
    wassert(actual(*writer).import(mdc[2]));
});

add_method("testacquire", [](Fixture& f) {
    metadata::TestCollection mdc("inbound/test.grib1");
    while (mdc.size() > 1) mdc.pop_back();

    auto batch = mdc.make_import_batch();
    wassert(outbound::Writer::test_acquire(f.cfg, batch));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");

    f.cfg.set("archive age", "1");
    wassert(outbound::Writer::test_acquire(f.cfg, batch));
    wassert(actual(batch[0]->result) == dataset::ACQ_ERROR);
    wassert(actual(batch[0]->dataset_name) == "");

    f.cfg.set("delete age", "1");
    wassert(outbound::Writer::test_acquire(f.cfg, batch));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");
});

}

}
