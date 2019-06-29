#include "arki/../config.h"
#include "arki/runtime/arki-mergeconf.h"
#include "arki/dataset/http.h"
#include <arki/summary.h>
#include <arki/runtime.h>
#include <arki/runtime/inputs.h>
#include <arki/runtime/config.h>
#include <arki/runtime/io.h>
#include <arki/utils/commandline/parser.h>
#include <arki/utils/geos.h>
#include <arki/utils/string.h>
#include <memory>
#include <iostream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace runtime {

namespace {

using namespace arki::utils::commandline;

struct Options : public StandardParserWithManpage
{
    StringOption* outfile;
    BoolOption* extra;
    StringOption* restr;
    VectorOption<String>* cfgfiles;
    BoolOption* ignore_system_ds;

    Options() : StandardParserWithManpage("arki-mergeconf", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
    {
        usage = "[options] [directories]";
        description =
            "Read dataset configuration from the given directories or config files, "
            " merge them and output the merged config file to standard output";

        outfile = add<StringOption>("output", 'o', "output", "file",
                "write the output to the given file instead of standard output");
        extra = add<BoolOption>("extra", 0, "extra", "",
                "extract extra information from the datasets (such as bounding box) "
                "and include it in the configuration");
        restr = add<StringOption>("restrict", 0, "restrict", "names",
                "restrict operations to only those datasets that allow one of the given (comma separated) names");
        cfgfiles = add< VectorOption<String> >("config", 'C', "config", "file",
                "merge configuration from the given file (can be given more than once)");
        ignore_system_ds = add<BoolOption>("ignore-system-datasets", 0, "ignore-system-datasets", "",
                "ignore error and duplicates datasets");
    }
};

}

int ArkiMergeconf::run(int argc, const char* argv[])
{
    Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;
        runtime::init();

        runtime::Inputs inputs;
        for (const auto& pathname: opts.cfgfiles->values())
            inputs.add_config_file(pathname);
        // Read the config files from the remaining commandline arguments
        while (opts.hasNext())
        {
            std::string path = opts.next();
            if (str::startswith(path, "http://") || str::startswith(path, "https://"))
            {
                auto sections = dataset::http::Reader::load_cfg_sections(path);
                inputs.add_sections(sections);
            }
            else
                inputs.add_pathname(path);
        }
        if (inputs.empty())
            throw commandline::BadOption("you need to specify at least one config file or dataset");

        // Validate the configuration
        bool hasErrors = false;
        for (auto si: inputs.merged)
        {
            // Validate filters
            try {
                Matcher::parse(si.second.value("filter"));
            } catch (std::exception& e) {
                cerr << si.first << ":"
                     << e.what()
                     << endl;
                hasErrors = true;
            }
        }
        if (hasErrors)
        {
            cerr << "Some input files did not validate." << endl;
            return 1;
        }

        // Remove unallowed entries
        if (opts.restr->isSet())
        {
            runtime::Restrict rest(opts.restr->stringValue());
            inputs.remove_unallowed(rest);
        }
        if (opts.ignore_system_ds->boolValue())
        {
            inputs.remove_system_datasets();
        }

        if (inputs.empty())
            throw commandline::BadOption("no useable config files or dataset directories found");

        // If requested, compute extra information
        if (opts.extra->boolValue())
        {
#ifdef HAVE_GEOS
            for (auto si: inputs.merged)
            {
                // Instantiate the dataset
                unique_ptr<dataset::Reader> d(dataset::Reader::create(si.second));
                // Get the summary
                Summary sum;
                d->query_summary(Matcher(), sum);

                // Compute bounding box, and store the WKT in bounding
                auto bbox = sum.getConvexHull();
                if (bbox.get())
                    si.second.set("bounding", bbox->toString());
            }
#endif
        }

        // Output the merged configuration
        std::stringstream ss;
        inputs.merged.write(ss, "memory");
        unique_ptr<sys::NamedFileDescriptor> out(runtime::make_output(*opts.outfile));
        out->write_all_or_throw(ss.str());
        out->close();

        return 0;
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        opts.outputHelp(cerr);
        return 1;
    }
}

}
}
