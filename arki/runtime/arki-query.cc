#include "arki/runtime/arki-query.h"
#include "arki/utils/commandline/parser.h"
#include "arki/configfile.h"
#include "arki/dataset.h"
#include "arki/dataset/merged.h"
#include "arki/dataset/http.h"
#include "arki/utils.h"
#include "arki/nag.h"
#include "arki/runtime.h"
#include "arki/libconfig.h"
#include <cstring>
#include <iostream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace runtime {

namespace {

struct Options : public runtime::CommandLine
{
    Options() : runtime::CommandLine("arki-query", 1)
    {
        usage = "[options] [expression] [configfile or directory...]";
        description =
            "Query the datasets in the given config file for data matching the"
            " given expression, and output the matching metadata.";
        add_query_options();
    }
};

}

int arki_query(int argc, const char* argv[])
{
    Options opts;

    try {

        if (opts.parse(argc, argv))
            return 0;

        opts.setupProcessing();

        bool all_successful = true;
        if (opts.merged->boolValue())
        {
            dataset::Merged merger;

            // Instantiate the datasets and add them to the merger
            string names;
            for (const auto& cfg: opts.inputs)
            {
                merger.add_dataset(opts.openSource(cfg));
                if (names.empty())
                    names = cfg.value("name");
                else
                    names += "," + cfg.value("name");
            }

            // Perform the query
            all_successful = opts.processSource(merger, names);

            for (size_t i = 0; i < merger.datasets.size(); ++i)
            {
                unique_ptr<dataset::Reader> ds(merger.datasets[i]);
                merger.datasets[i] = 0;
                opts.closeSource(move(ds), all_successful);
            }
        } else if (opts.qmacro->isSet()) {
            // Create the virtual qmacro dataset
            ConfigFile cfg;
            unique_ptr<dataset::Reader> ds = runtime::make_qmacro_dataset(
                    cfg,
                    opts.inputs.as_config(),
                    opts.qmacro->stringValue(),
                    opts.strquery);

            // Perform the query
            all_successful = opts.processSource(*ds, opts.qmacro->stringValue());
        } else {
            // Query all the datasets in sequence
            for (const auto& cfg: opts.inputs)
            {
                unique_ptr<dataset::Reader> ds = opts.openSource(cfg);
                nag::verbose("Processing %s...", cfg.value("path").c_str());
                bool success = opts.processSource(*ds, cfg.value("path"));
                opts.closeSource(move(ds), success);
                if (!success) all_successful = false;
            }
        }

        opts.doneProcessing();

        if (all_successful)
            return 0;
        else
            return 2;
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        opts.outputHelp(cerr);
        return 1;
    }
}

}
}
