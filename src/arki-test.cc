/// Test script support
#include "config.h"
#include <arki/wibble/exception.h>
#include <arki/utils/string.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/dataset/test-scenario.h>
#include <arki/runtime.h>

#include <memory>
#include <iostream>
#include <cstdlib>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace arki::utils;

namespace arki {
namespace utils {
namespace commandline {

struct Options : public StandardParserWithManpage
{
//    BoolOption* do_list;
//    BoolOption* do_scan;
//    BoolOption* do_test;
//    BoolOption* do_import;
    BoolOption* do_writelock;
    StringOption* build_scenario;

    Options() : StandardParserWithManpage("arki-test", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
    {
        usage = "[options]";
        description = "Support for arkimet test scripts";

//        do_list = add<BoolOption>("list", 0, "list", "", "list files in the inbound queue");
//        do_scan = add<BoolOption>("scan", 0, "scan", "", "scan files from the inbound queue");
//        do_test = add<BoolOption>("test", 0, "test", "", "test dispatching files from the inbound queue");
//        do_import = add<BoolOption>("import", 0, "import", "", "import files from the inbound queue");
        build_scenario = add<StringOption>("build-scenario", 0, "build-scenario", "NAME", "name of the scenario to build, or 'list' for a list");
        do_writelock = add<BoolOption>("writelock", 0, "writelock", "", "lock a dataset for writing");
    }
};

}
}
}

int main(int argc, const char* argv[])
{
    commandline::Options opts;

    try {
        if (opts.parse(argc, argv))
            return 0;

        runtime::init();

        if (opts.build_scenario->isSet())
        {
            string name = opts.build_scenario->stringValue();
            if (name == "list")
            {
                vector<string> list = dataset::test::Scenario::list();
                for (vector<string>::const_iterator i = list.begin();
                        i != list.end(); ++i)
                    cerr << *i << endl;
            } else {
                dataset::test::Scenario::get(name);
            }
        } else if (opts.do_writelock->boolValue()) {
            string dspath = opts.next();
            ConfigFile cfg;
            dataset::Reader::readConfig(dspath, cfg);
            cout << "Dataset config:" << endl;
            ConfigFile* dsconfig = cfg.sectionBegin()->second;
            dsconfig->output(cout, "stdout");
            unique_ptr<dataset::Writer> ds(dataset::Writer::create(*dsconfig));
            Pending p = ds->test_writelock();
            printf("Press ENTER to unlock %s and quit...", dspath.c_str());
            fflush(stdout);
            getchar();
        } else {
            throw commandline::BadOption("please specify an action with --build-scenario");
        }
        return 0;
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        opts.outputHelp(cerr);
        return 1;
    } catch (std::exception& e) {
        cerr << e.what() << endl;
        return 1;
    }
}
