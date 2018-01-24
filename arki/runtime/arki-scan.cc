#include "arki/runtime/arki-scan.h"
#include "arki/runtime.h"
#include "arki/utils/commandline/parser.h"
#include <iostream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace runtime {

namespace {

struct Options : public runtime::CommandLine
{
    Options() : runtime::CommandLine("arki-scan")
    {
        usage = "[options] [input...]";
        description =
            "Read one or more files or datasets and process their data "
            "or import them in a dataset.";
        add_scan_options();
    }
};

}

int arki_scan(int argc, const char* argv[])
{
    Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

        runtime::init();

        opts.setupProcessing();

        bool all_successful = true;
        for (const ConfigFile& cfg: opts.inputs)
        {
            unique_ptr<dataset::Reader> ds = opts.openSource(cfg);
            bool success = true;
            try {
                success = opts.processSource(*ds, cfg.value("path"));
            } catch (std::exception& e) {
                // FIXME: this is a quick experiment: a better message can
                // print some of the stats to document partial imports
                cerr << cfg.value("path") << " failed: " << e.what() << endl;
                success = false;
            }

            opts.closeSource(move(ds), success);

            // Take note if something went wrong
            if (!success) all_successful = false;
        }

        opts.doneProcessing();

        if (all_successful)
            return 0;
        else
            return 2;
    } catch (runtime::HandledByCommandLineParser& e) {
        return e.status;
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        opts.outputHelp(cerr);
        return 1;
    } catch (std::exception& e) {
        cerr << e.what() << endl;
        return 1;
    }
}

}
}