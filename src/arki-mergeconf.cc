/// Merge arkimet dataset configurations
#include "config.h"
#include <arki/configfile.h>
#include <arki/dataset.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/runtime.h>
#include <arki/utils/commandline/parser.h>
#include <arki/utils/geosdef.h>
#include <arki/utils/string.h>
#include <arki/utils/sys.h>
#include <memory>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::utils;

namespace arki {
namespace utils {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	StringOption* outfile;
	BoolOption* extra;
	StringOption* restr;
	VectorOption<String>* cfgfiles;

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

        runtime::Inputs inputs;
        for (const auto& pathname: opts.cfgfiles->values())
            inputs.add_config_file(pathname);
        while (opts.hasNext())
            inputs.add_config_file(opts.next());
        if (inputs.empty())
            throw commandline::BadOption("you need to specify the config file");

        // Read the config files from the remaining commandline arguments
        while (opts.hasNext())
        {
            string file = opts.next();
            if (!str::startswith(file, "http://") &&
                !str::startswith(file, "https://") &&
                !sys::access(str::joinpath(file, "config"), F_OK))
            {
                cerr << file << " skipped: it does not look like a dataset" << endl;
                continue;
            }
            try {
                inputs.add_config_file(file);
            } catch (std::exception& e) {
                cerr << file << " skipped: " << e.what() << endl;
            }
        }
        if (inputs.empty())
            throw commandline::BadOption("no valid config files or dataset directories found");

        // Validate the configuration
        bool hasErrors = false;
        for (const ConfigFile& cfg: inputs)
        {
            // Validate filters
            try {
                Matcher::parse(cfg.value("filter"));
            } catch (std::exception& e) {
                const auto* fp = cfg.valueInfo("filter");
                if (fp)
                    cerr << fp->pathname << ":" << fp->lineno << ":";
                cerr << e.what();
                cerr << endl;
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

        if (inputs.empty())
            throw commandline::BadOption("no useable config files or dataset directories found");

        // If requested, compute extra information
        if (opts.extra->boolValue())
        {
#ifdef HAVE_GEOS
            ARKI_GEOS_GEOMETRYFACTORY gf;

            for (ConfigFile& cfg: inputs)
            {
                // Instantiate the dataset
                unique_ptr<dataset::Reader> d(dataset::Reader::create(cfg));
                // Get the summary
                Summary sum;
                d->query_summary(Matcher(), sum);

                // Compute bounding box, and store the WKT in bounding
                unique_ptr<ARKI_GEOS_GEOMETRY> bbox = sum.getConvexHull(gf);
                if (bbox.get())
                    cfg.setValue("bounding", bbox->toString());
            }
#endif
        }


        // Output the merged configuration
        string res = inputs.as_config().serialize();
        unique_ptr<sys::NamedFileDescriptor> out(runtime::make_output(*opts.outfile));
        out->write_all_or_throw(res);
        out->close();

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
