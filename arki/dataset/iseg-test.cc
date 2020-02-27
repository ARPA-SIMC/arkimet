#include "tests.h"
#include "iseg/writer.h"
#include "arki/scan.h"
#include "arki/types/source/blob.h"
#include "arki/dataset/query.h"

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
            type = iseg
            step = daily
            format = grib
        )");
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test("arki_dataset_iseg");

void Tests::register_tests() {

// Test acquiring data with replace=1
add_method("acquire_replace", [](Fixture& f) {
    metadata::TestCollection mdc("inbound/test.grib1");

    // Import once
    {
        auto writer = f.makeIsegWriter();
        for (auto& md: mdc)
            wassert(actual(writer->acquire(*md)) == dataset::ACQ_OK);
        writer->flush();
    }

    // Import again, make sure they're all duplicates
    {
        auto writer = f.makeIsegWriter();
        for (auto& md: mdc)
            wassert(actual(writer->acquire(*md)) == dataset::ACQ_ERROR_DUPLICATE);
        writer->flush();
    }

    // Import again with replace=true, make sure they're all ok
    {
        auto cfg(f.cfg);
        cfg.set("replace", "true");
        auto config = std::make_shared<dataset::iseg::Dataset>(f.session(), cfg);
        auto writer = config->create_writer();
        for (auto& md: mdc)
            wassert(actual(writer->acquire(*md)) == dataset::ACQ_OK);
        writer->flush();
    }

    // Test querying the dataset
    {
        metadata::Collection mdc = f.query(Matcher());
        wassert(actual(mdc.size()) == 3u);

        // Make sure we're not getting the deleted element
        wassert(actual(mdc[0].sourceBlob().offset) > 0u);
        wassert(actual(mdc[1].sourceBlob().offset) > 0u);
        wassert(actual(mdc[2].sourceBlob().offset) > 0u);
    }
});


// Test Update Sequence Number replacement strategy
add_method("acquire_replace_usn", [](Fixture& f) {
    f.reset_test("type=iseg\nstep=daily\nformat=bufr");
    auto writer = f.makeIsegWriter();

    metadata::TestCollection mdc("inbound/synop-gts.bufr");
    metadata::TestCollection mdc_upd("inbound/synop-gts-usn2.bufr");

    wassert(actual(mdc.size()) == 1u);

    // Acquire
    wassert(actual(writer->acquire(mdc[0])) == dataset::ACQ_OK);

    // Acquire again: it fails
    wassert(actual(writer->acquire(mdc[0])) == dataset::ACQ_ERROR_DUPLICATE);

    // Acquire again: it fails even with a higher USN
    wassert(actual(writer->acquire(mdc_upd[0])) == dataset::ACQ_ERROR_DUPLICATE);

    // Acquire with replace: it works
    wassert(actual(writer->acquire(mdc[0], dataset::REPLACE_ALWAYS)) == dataset::ACQ_OK);

    // Acquire with USN: it works, since USNs the same as the existing ones do overwrite
    wassert(actual(writer->acquire(mdc[0], dataset::REPLACE_HIGHER_USN)) == dataset::ACQ_OK);

    // Acquire with a newer USN: it works
    wassert(actual(writer->acquire(mdc_upd[0], dataset::REPLACE_HIGHER_USN)) == dataset::ACQ_OK);

    // Acquire with the lower USN: it fails
    wassert(actual(writer->acquire(mdc[0], dataset::REPLACE_HIGHER_USN)) == dataset::ACQ_ERROR_DUPLICATE);

    // Acquire with the same high USN: it works, since USNs the same as the existing ones do overwrite
    wassert(actual(writer->acquire(mdc_upd[0], dataset::REPLACE_HIGHER_USN)) == dataset::ACQ_OK);

    // Try to query the element and see if it is the right one
    {
        metadata::Collection mdc_read = f.query(dataset::DataQuery("origin:BUFR", true));
        wassert(actual(mdc_read.size()) == 1u);
        int usn;
        wassert(actual(scan::Scanner::update_sequence_number(mdc_read[0], usn)).istrue());
        wassert(actual(usn) == 2);
    }
});

}

}

