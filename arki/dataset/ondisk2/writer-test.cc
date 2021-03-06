#include "arki/dataset/ondisk2/test-utils.h"
#include "arki/dataset/ondisk2/writer.h"
#include "arki/dataset/ondisk2/reader.h"
#include "arki/dataset/ondisk2/index.h"
#include "arki/dataset/query.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/session.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/core/file.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/summary.h"
#include "arki/nag.h"
#include <algorithm>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::dataset;
using namespace arki::utils;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type = ondisk2
            step = daily
            unique = origin, reftime
        )");
    }
};


class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_dataset_ondisk2_writer");

void Tests::register_tests() {

// Ondisk2 specific tests that are not convered by tests in dataset-writer-test.cc

// Test that the summary cache is properly invalidated on import
add_method("summary_invalidate", [](Fixture& f) {
    // Perform maintenance on empty dir, creating an empty summary cache
    wassert(f.ensure_localds_clean(0, 0));

    // Query the summary, there should be no data
    {
        auto reader = f.makeOndisk2Reader();
        wassert_true(reader->hasWorkingIndex());
        Summary s;
        reader->query_summary(Matcher(), s);
        wassert(actual(s.count()) == 0u);
    }

    // Acquire files
    f.import();

    // Query the summary again, there should be data
    {
        auto reader = f.makeOndisk2Reader();
        wassert_true(reader->hasWorkingIndex());
        Summary s;
        reader->query_summary(Matcher(), s);
        wassert(actual(s.count()) == 3u);
    }
});

// Try to reproduce a bug where two conflicting BUFR files were not properly
// imported with USN handling
add_method("regression_0", [](Fixture& f) {
    skip_unless_bufr();
    core::cfg::Section cfg;
    cfg.set("path", "gts_temp");
    cfg.set("name", "gts_temp");
    cfg.set("type", "ondisk2");
    cfg.set("step", "daily");
    cfg.set("unique", "origin, reftime, proddef");
    cfg.set("filter", "product:BUFR:t=temp");
    cfg.set("replace", "USN");

    auto writer = f.session()->dataset(cfg)->create_writer();

    metadata::TestCollection mds("inbound/conflicting-temp-same-usn.bufr");
    wassert(actual(*writer).import(mds));
});

// Test removal of VM2 data
add_method("issue57", [](Fixture& f) {
    skip_unless_vm2();
    f.cfg->set("unique", "reftime, area, product");

    // Import the sample file
    sys::write_file("issue57.vm2", "201610050000,12626,139,70,,,000000000\n");
    metadata::TestCollection input("issue57.vm2");
    {
        auto writer = f.makeOndisk2Writer();
        wassert(actual(*writer).import(input[0]));
    }

    // Query back the data
    metadata::Collection queried(*f.makeOndisk2Reader(), Matcher());
    wassert(actual(queried.size()) == 1u);

    // Delete the data
    {
        auto writer = f.makeOndisk2Writer();
        writer->remove(queried[0]);
    }

    // Query back the data
    metadata::Collection after_delete(*f.makeOndisk2Reader(), Matcher());
    wassert(actual(after_delete.size()) == 0u);
});

add_method("testacquire", [](Fixture& f) {
    {
        // Create the dataset
        auto writer = f.makeOndisk2Writer();
    }
    metadata::TestCollection mdc("inbound/test.grib1");
    while (mdc.size() > 1) mdc.pop_back();

    auto batch = mdc.make_import_batch();
    wassert(ondisk2::Writer::test_acquire(f.session(), *f.cfg, batch));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");

    f.cfg->set("archive age", "1");
    wassert(ondisk2::Writer::test_acquire(f.session(), *f.cfg, batch));
    wassert(actual(batch[0]->result) == dataset::ACQ_ERROR);
    wassert(actual(batch[0]->dataset_name) == "");

    f.cfg->set("delete age", "1");
    wassert(ondisk2::Writer::test_acquire(f.session(), *f.cfg, batch));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");
});

}

}
