/// Test script support
#include "config.h"
#include <arki/utils/string.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/runtime.h>
#include <arki/configfile.h>
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
    BoolOption* do_writelock;

    Options() : StandardParserWithManpage("arki-test", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
    {
        usage = "[options]";
        description = "Support for arkimet test scripts";
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

        if (opts.do_writelock->boolValue()) {
            string dspath = opts.next();
            ConfigFile cfg;
            dataset::Reader::read_config(dspath, cfg);
            cout << "Dataset config:" << endl;
            ConfigFile* dsconfig = cfg.section_begin()->second;
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
