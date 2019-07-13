#include "arki/runtime/arki-query.h"
#include "arki/runtime/processor.h"
#include <arki/runtime/inputs.h>
#include <arki/runtime/io.h>
#include "arki/utils/commandline/parser.h"
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
    }
};

}

ArkiQuery::~ArkiQuery()
{
    delete processor;
}

int ArkiQuery::run(int argc, const char* argv[])
{
    Options opts;

    try {

        if (opts.parse(argc, argv))
            return 0;

        Inputs inputs(this->inputs, opts);
        auto output = make_output(*opts.outfile);

        Matcher query;
        if (!opts.strquery.empty())
            query = Matcher::parse(dataset::http::Reader::expand_remote_query(inputs.merged, opts.strquery));
        else
            query = Matcher::parse(opts.strquery);

        processor = processor::create(opts, query, *output).release();

        auto dest =  [&](runtime::Source& source) {
            source.process(*processor);
            return true;
        };

        bool all_successful = true;
        if (opts.stdin_input->isSet())
        {
            all_successful = foreach_stdin(opts.stdin_input->stringValue(), dest);
        } else if (opts.merged->boolValue()) {
            all_successful = foreach_merged(inputs.merged, dest);
        } else if (opts.qmacro->isSet()) {
            all_successful = foreach_qmacro(opts.qmacro->stringValue(), opts.qmacro_query, this->inputs, dest);
        } else {
            all_successful = foreach_sections(inputs.merged, dest);
        }

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
