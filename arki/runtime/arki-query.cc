#include "arki/runtime/arki-query.h"
#include "arki/runtime/processor.h"
#include <arki/runtime/inputs.h>
#include <arki/runtime/io.h>
#include "arki/utils/commandline/parser.h"
#include "arki/configfile.h"
#include "arki/dataset.h"
#include "arki/dataset/merged.h"
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
    }
};

}

int arki_query(int argc, const char* argv[])
{
    Options opts;

    try {

        if (opts.parse(argc, argv))
            return 0;

        Inputs inputs(opts);
        auto output = make_output(*opts.outfile);

        Matcher query;
        if (!opts.strquery.empty())
            query = Matcher::parse(inputs.expand_remote_query(opts.strquery));
        else
            query = Matcher::parse(opts.strquery);

        auto processor = processor::create(opts, query, *output);

        bool all_successful = foreach_source(opts, inputs, [&](runtime::Source& source) {
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
