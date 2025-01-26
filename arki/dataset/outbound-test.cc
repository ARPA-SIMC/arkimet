#include <arki/dataset/tests.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/dataset/outbound.h>

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
    wassert(actual(writer).acquire_ok(mdc));
});

add_method("testacquire", [](Fixture& f) {
    metadata::TestCollection mdc("inbound/test.grib1");
    while (mdc.size() > 1) mdc.pop_back();

    auto batch = mdc.make_batch();
    wassert(outbound::Writer::test_acquire(f.session(), *f.cfg, batch));
    wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
    wassert(actual(batch[0]->destination) == "testds");

    f.cfg->set("archive age", "1");
    wassert(outbound::Writer::test_acquire(f.session(), *f.cfg, batch));
    wassert(actual(batch[0]->result) == metadata::Inbound::Result::ERROR);
    wassert(actual(batch[0]->destination) == "");

    f.cfg->set("delete age", "1");
    wassert(outbound::Writer::test_acquire(f.session(), *f.cfg, batch));
    wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
    wassert(actual(batch[0]->destination) == "testds");
});

}

}
