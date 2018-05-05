#include "arki/runtime/arki-query.h"
#include "arki/runtime/processor.h"
#include "arki/utils/commandline/parser.h"
#include "arki/configfile.h"
#include "arki/dataset.h"
#include "arki/dataset/merged.h"
#include "arki/dataset/http.h"
#include "arki/utils.h"
#include "arki/nag.h"
#include "arki/runtime.h"
#include "arki/runtime/source.h"
#include "arki/libconfig.h"
#include <cstring>
#include <iostream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace runtime {

namespace {

struct Options : public runtime::QueryCommandLine
{
    Options() : runtime::QueryCommandLine("arki-query", 1)
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

        auto processor = processor::create(opts, opts.query, *opts.output);

        bool all_successful = opts.foreach_source([&](runtime::Source& source) {
            source.process(*processor);
            return true;
        });

        processor->end();

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
