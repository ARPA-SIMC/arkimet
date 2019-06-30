#include "arki/../config.h"
#include "arki/runtime/arki-mergeconf.h"
#include "arki/dataset/http.h"
#include "arki/summary.h"
#include "arki/runtime.h"
#include "arki/runtime/inputs.h"
#include "arki/runtime/config.h"
#include "arki/runtime/io.h"
#include "arki/utils/geos.h"
#include "arki/utils/string.h"
#include <memory>
#include <iostream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace runtime {

core::cfg::Sections ArkiMergeconf::run()
{
    runtime::Inputs inputs;

    for (const auto& pathname: cfgfiles)
        inputs.add_config_file(pathname);

    // Read the config files from the remaining commandline arguments
    for (const auto& path: sources)
    {
        if (str::startswith(path, "http://") || str::startswith(path, "https://"))
        {
            auto sections = dataset::http::Reader::load_cfg_sections(path);
            inputs.add_sections(sections);
        }
        else
            inputs.add_pathname(path);
    }
    if (inputs.empty())
        throw std::runtime_error("you need to specify at least one config file or dataset");

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
        throw std::runtime_error("Some input files did not validate.");

    // Remove unallowed entries
    if (restrict)
    {
        runtime::Restrict rest(restrict_expr);
        inputs.remove_unallowed(rest);
    }

    if (ignore_system_ds)
    {
        inputs.remove_system_datasets();
    }

    if (inputs.empty())
        throw std::runtime_error("no useable config files or dataset directories found");

    // If requested, compute extra information
    if (extra)
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

    return inputs.merged;
}

}
}