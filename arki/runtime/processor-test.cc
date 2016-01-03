#include "config.h"
#include "processor.h"
#include "io.h"
#include <arki/dataset.h>
#include <arki/dataset/tests.h>
#include <arki/scan/any.h>
#include <arki/utils/sys.h>

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

        testdata::GRIBData fixture;
        wassert(import_all(fixture));
    }

    void run_maker(ProcessorMaker& pm, Matcher matcher=Matcher())
    {
        auto ds(makeReader());

        if (sys::exists("pm-out"))
            sys::unlink("pm-out");

        runtime::File out("pm-out");
        unique_ptr<DatasetProcessor> dp(pm.make(matcher, out));
        dp->process(*ds, "testds");
        dp->end();
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_processor");

void Tests::register_tests() {

// Export only binary metadata (the default)
add_method("metadata_binary", [](Fixture& f) {
    ProcessorMaker pm;
    wassert(f.run_maker(pm));

    metadata::Collection mdc;
    mdc.read_from_file("pm-out");

    wassert(actual(mdc.size()) == 3u);
    wassert(actual_type(mdc[0].source()).is_source_blob("grib1", ".", sys::abspath("testds/2007/07-07.grib1"), 0, 34960));
    wassert(actual_type(mdc[1].source()).is_source_blob("grib1", ".", sys::abspath("testds/2007/07-08.grib1"), 0, 7218));
    wassert(actual_type(mdc[2].source()).is_source_blob("grib1", ".", sys::abspath("testds/2007/10-09.grib1"), 0, 2234));
});

// Export inline data
add_method("metadata_binary_inline", [](Fixture& f) {
    ProcessorMaker pm;
    pm.data_inline = true;
    wassert(f.run_maker(pm));

    metadata::Collection mdc;
    mdc.read_from_file("pm-out");

    wassert(actual(mdc.size()) == 3u);
    wassert(actual_type(mdc[0].source()).is_source_inline("grib1", 34960));
    wassert(actual_type(mdc[1].source()).is_source_inline("grib1", 7218));
    wassert(actual_type(mdc[2].source()).is_source_inline("grib1", 2234));
    wassert(actual(mdc[0].getData().size()) == 34960u);
    wassert(actual(mdc[1].getData().size()) == 7218u);
    wassert(actual(mdc[2].getData().size()) == 2234u);
});

// Export data only
add_method("data_binary", [](Fixture& f) {
    ProcessorMaker pm;
    pm.data_only = true;
    wassert(f.run_maker(pm));

    metadata::Collection mdc;
    scan::scan("pm-out", mdc.inserter_func(), "grib");

    wassert(actual(mdc.size()) == 3u);
    wassert(actual_type(mdc[0].source()).is_source_blob("grib1", sys::abspath("."), "pm-out", 0, 34960));
    wassert(actual_type(mdc[1].source()).is_source_blob("grib1", sys::abspath("."), "pm-out", 34960, 7218));
    wassert(actual_type(mdc[2].source()).is_source_blob("grib1", sys::abspath("."), "pm-out", 34960 + 7218, 2234));
});

}

}
