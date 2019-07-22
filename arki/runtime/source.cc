#include "source.h"
#include "processor.h"
#include "dispatch.h"
#include "arki/runtime.h"
#include "arki/scan.h"
#include "arki/utils/string.h"
#include "arki/exceptions.h"
#include "arki/dataset/file.h"
#include "arki/dataset/http.h"
#include "arki/dataset/merged.h"
#include "arki/dataset/fromfunction.h"
#include "arki/querymacro.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"

using namespace arki;
using namespace arki::utils;

namespace arki {
namespace runtime {


bool foreach_stdin(const std::string& format, std::function<void(dataset::Reader&)> dest)
{
    auto scanner = scan::Scanner::get_scanner(format);

    core::cfg::Section cfg;
    cfg.set("format", format);
    cfg.set("name", "stdin:" + scanner->name());
    auto config = dataset::fromfunction::Config::create(cfg);

    auto reader = std::make_shared<dataset::fromfunction::Reader>(config);
    reader->generator = [&](metadata_dest_func dest){
        sys::NamedFileDescriptor fd_stdin(0, "stdin");
        return scanner->scan_pipe(fd_stdin, dest);
    };

    bool success = true;
    try {
        dest(*reader);
    } catch (std::exception& e) {
        nag::warning("%s failed: %s", reader->name().c_str(), e.what());
        success = false;
    }
    return success;
}

bool foreach_merged(const core::cfg::Sections& input, std::function<void(dataset::Reader&)> dest)
{
    bool success = true;
    std::shared_ptr<dataset::Merged> reader = std::make_shared<dataset::Merged>();

    // Instantiate the datasets and add them to the merger
    for (auto si: input)
        reader->add_dataset(dataset::Reader::create(si.second));

    try {
        dest(*reader);
    } catch (std::exception& e) {
        nag::warning("%s failed: %s", reader->name().c_str(), e.what());
        success = false;
    }

    return success;
}

bool foreach_qmacro(const std::string& macro_name, const std::string& macro_query, const core::cfg::Sections& inputs, std::function<void(dataset::Reader&)> dest)
{
    bool success = true;

    core::cfg::Section cfg;
    std::shared_ptr<dataset::Reader> reader;

    // Create the virtual qmacro dataset
    std::string baseurl = dataset::http::Reader::allSameRemoteServer(inputs);
    if (baseurl.empty())
    {
        // Create the local query macro
        nag::verbose("Running query macro %s on local datasets", macro_name.c_str());
        reader = std::make_shared<Querymacro>(cfg, inputs, macro_name, macro_query);
    } else {
        // Create the remote query macro
        nag::verbose("Running query macro %s on %s", macro_name.c_str(), baseurl.c_str());
        core::cfg::Section cfg;
        cfg.set("name", macro_name);
        cfg.set("type", "remote");
        cfg.set("path", baseurl);
        cfg.set("qmacro", macro_query);
        reader = dataset::Reader::create(cfg);
    }

    try {
        dest(*reader);
    } catch (std::exception& e) {
        nag::warning("%s failed: %s", reader->name().c_str(), e.what());
        success = false;
    }
    return success;
}

bool foreach_sections(const core::cfg::Sections& inputs, std::function<void(dataset::Reader&)> dest)
{
    bool all_successful = true;
    // Query all the datasets in sequence
    for (auto si: inputs)
    {
        auto reader = dataset::Reader::create(si.second);
        bool success = true;
        try {
            dest(*reader);
        } catch (std::exception& e) {
            nag::warning("%s failed: %s", reader->name().c_str(), e.what());
            success = false;
        }
        if (!success) all_successful = false;
    }
    return all_successful;
}

}
}
