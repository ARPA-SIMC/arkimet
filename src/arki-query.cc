#include "config.h"
#include <arki/utils/commandline/parser.h>
#include <arki/configfile.h>
#include <arki/dataset.h>
#include <arki/dataset/merged.h>
#include <arki/dataset/http.h>
#include <arki/utils.h>
#include <arki/nag.h>
#include <arki/runtime.h>
#include <cstring>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::utils;

namespace arki {
namespace utils {
namespace commandline {

struct Options : public arki::runtime::CommandLine
{
	Options() : runtime::CommandLine("arki-query", 1)
	{
		usage = "[options] [expression] [configfile or directory...]";
		description =
		    "Query the datasets in the given config file for data matching the"
			" given expression, and output the matching metadata.";

		addQueryOptions();
	}
};

}
}
}

template<typename T>
struct RAIIArrayDeleter
{
	T*& a;
	RAIIArrayDeleter(T*& a) : a(a) {}
	~RAIIArrayDeleter() { if (a) delete[] a; }
};

int main(int argc, const char* argv[])
{
    commandline::Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

		runtime::init();

		opts.setupProcessing();

        bool all_successful = true;
        if (opts.merged->boolValue())
        {
            dataset::Merged merger;
            size_t dscount = opts.inputs.size();

            // Create an unique_ptr array to take care of memory management
            // It used to be just: unique_ptr<Reader> datasets[dscount];
            // but xlC does not seem to like it
            unique_ptr<dataset::Reader>* datasets = new unique_ptr<dataset::Reader>[dscount];
            RAIIArrayDeleter< unique_ptr<dataset::Reader> > datasets_mman(datasets);

            // Instantiate the datasets and add them to the merger
            int idx = 0;
            string names;
            for (const auto& cfg: opts.inputs)
            {
                datasets[idx] = opts.openSource(cfg);
                merger.addDataset(*datasets[idx]);
                if (names.empty())
                    names = cfg.value("name");
                else
                    names += "," + cfg.value("name");
            }

            // Perform the query
            all_successful = opts.processSource(merger, names);

            for (size_t i = 0; i < dscount; ++i)
                opts.closeSource(move(datasets[i]), all_successful);
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
		//return summary.count() > 0 ? 0 : 1;
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        opts.outputHelp(cerr);
        return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}
