#include "config.h"
#include "processor.h"
#include "io.h"
#include "arki/dataset.h"
#include "arki/dataset/tests.h"
#include "arki/types/source.h"
#include "arki/scan/any.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::tests;
using namespace arki::core;
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
        auto ds(config().create_reader());

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
    wassert(actual_type(mdc[0].source()).is_source_blob("grib", sys::abspath("."), sys::abspath("testds/2007/07-07.grib"), 0, 34960));
    wassert(actual_type(mdc[1].source()).is_source_blob("grib", sys::abspath("."), sys::abspath("testds/2007/07-08.grib"), 0, 7218));
    wassert(actual_type(mdc[2].source()).is_source_blob("grib", sys::abspath("."), sys::abspath("testds/2007/10-09.grib"), 0, 2234));
});

// Export inline data
add_method("metadata_binary_inline", [](Fixture& f) {
    ProcessorMaker pm;
    pm.data_inline = true;
    wassert(f.run_maker(pm));

    metadata::Collection mdc;
    mdc.read_from_file("pm-out");

    wassert(actual(mdc.size()) == 3u);
    wassert(actual_type(mdc[0].source()).is_source_inline("grib", 34960));
    wassert(actual_type(mdc[1].source()).is_source_inline("grib", 7218));
    wassert(actual_type(mdc[2].source()).is_source_inline("grib", 2234));
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
    wassert(actual_type(mdc[0].source()).is_source_blob("grib", sys::abspath("."), "pm-out", 0, 34960));
    wassert(actual_type(mdc[1].source()).is_source_blob("grib", sys::abspath("."), "pm-out", 34960, 7218));
    wassert(actual_type(mdc[2].source()).is_source_blob("grib", sys::abspath("."), "pm-out", 34960 + 7218, 2234));
});

// Reproduce https://github.com/ARPAE-SIMC/arkimet/issues/26
add_method("start_hook", [](Fixture& f) {
    auto start_hook = [](NamedFileDescriptor& out) { out.write_all_or_throw(string("test")); };
    ProcessorMaker pm;
    pm.data_only = true;
    pm.data_start_hook = start_hook;
    wassert(f.run_maker(pm));

    string content = sys::read_file("pm-out");
    wassert(actual(content).startswith("testGRIB"));
});

}

}
