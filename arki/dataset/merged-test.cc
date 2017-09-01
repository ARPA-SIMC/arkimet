#include "config.h"
#include <arki/tests/tests.h>
#include <arki/dataset/merged.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/scan/grib.h>
#include <arki/dispatcher.h>
#include <sstream>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::tests;

namespace {

struct Fixture : public arki::utils::tests::Fixture
{
    ConfigFile config;
    dataset::Merged ds;

    Fixture()
    {
        // Cleanup the test datasets
        system("rm -rf test200/*");
        system("rm -rf test80/*");
        system("rm -rf error/*");

        // In-memory dataset configuration
        string conf =
            "[test200]\n"
            "type = ondisk2\n"
            "step = daily\n"
            "filter = origin: GRIB1,200\n"
            "index = origin, reftime\n"
            "name = test200\n"
            "path = test200\n"
            "\n"
            "[test80]\n"
            "type = ondisk2\n"
            "step = daily\n"
            "filter = origin: GRIB1,80\n"
            "index = origin, reftime\n"
            "name = test80\n"
            "path = test80\n"
            "\n"
            "[error]\n"
            "type = error\n"
            "step = daily\n"
            "name = error\n"
            "path = error\n";
        config.parse(conf, "(memory)");

        // Import data into the datasets
        Metadata md;
        scan::Grib scanner;
        RealDispatcher dispatcher(config);
        scanner.open("inbound/test.grib1");
        ensure(scanner.next(md));
        wassert(actual(dispatcher.dispatch(md)) == Dispatcher::DISP_OK);
        ensure(scanner.next(md));
        wassert(actual(dispatcher.dispatch(md)) == Dispatcher::DISP_OK);
        ensure(scanner.next(md));
        wassert(actual(dispatcher.dispatch(md)) == Dispatcher::DISP_ERROR);
        ensure(!scanner.next(md));
        dispatcher.flush();

        ds.add_dataset(*config.section("test200"));
        ds.add_dataset(*config.section("test80"));
        ds.add_dataset(*config.section("error"));
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_dataset_merged");

void Tests::register_tests() {

// Test querying the datasets
add_method("query", [](Fixture& f) {
    metadata::Collection mdc(f.ds, Matcher());
    wassert(actual(mdc.size()) == 3u);
});

}

}