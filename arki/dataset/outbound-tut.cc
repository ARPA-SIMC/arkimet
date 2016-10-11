#include "config.h"
#include <arki/dataset/tests.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/scan/grib.h>
#include <arki/dispatcher.h>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;

struct arki_dataset_outbound_shar {
    ConfigFile config;

    arki_dataset_outbound_shar()
    {
        // Cleanup the test datasets
        system("rm -rf test200/*");
        system("rm -rf test80/*");
        system("rm -rf test200o/*");
        system("rm -rf test80o/*");
        system("rm -rf error/*");

        // In-memory dataset configuration
        string conf =
            // We need both normal ondisk and outbound, as dispatch only to
            // outbound will cause a failure since the message has not been
            // stored in any archival dataset
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
            "[testout]\n"
            "type = outbound\n"
            "step = daily\n"
            "filter = origin: GRIB1,200\n"
            "name = test200o\n"
            "path = test200o\n"
            "\n"
            "[testout1]\n"
            "type = outbound\n"
            "step = daily\n"
            "filter = origin: GRIB1,80\n"
            "name = test80o\n"
            "path = test80o\n"
            "\n"
            "[error]\n"
            "type = error\n"
            "step = daily\n"
            "name = error\n"
            "path = error\n";
        config.parse(conf, "(memory)");
    }
};
TESTGRP(arki_dataset_outbound);

namespace {
void dispatches(Dispatcher& dispatcher, Metadata& md)
{
    Dispatcher::Outcome res = dispatcher.dispatch(md);
    // If dispatch fails, print the notes
    if (res != Dispatcher::DISP_OK)
    {
        cerr << "Failed dispatch notes:" << endl;
        for (const auto& n: md.notes())
            cerr << "   " << n << endl;
    }
    wassert(actual(res) == Dispatcher::DISP_OK);
}
}

// Test acquiring the data
def_test(1)
{
    // Import data into the datasets
    Metadata md;
    metadata::Collection mdc;
    scan::Grib scanner;
    RealDispatcher dispatcher(config);
    dispatcher.outbound_md_dest = mdc.inserter_func();
    scanner.open("inbound/test.grib1");

    wassert(actual(scanner.next(md)).istrue());
    wassert(dispatches(dispatcher, md));
    wassert(actual(dispatcher.outboundFailures()) == 0u);
    wassert(actual(mdc.size()) == 1u);

    wassert(actual(scanner.next(md)).istrue());
    wassert(dispatches(dispatcher, md));
    wassert(actual(dispatcher.outboundFailures()) == 0u);
    wassert(actual(mdc.size()) == 2u);

    wassert(actual(scanner.next(md)).istrue());
    wassert(actual(dispatcher.dispatch(md)) == Dispatcher::DISP_ERROR);
    wassert(actual(dispatcher.outboundFailures()) == 0u);
    wassert(actual(mdc.size()) == 2u);

    ensure(!scanner.next(md));
}

}
