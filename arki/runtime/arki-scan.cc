#include "arki/runtime/arki-scan.h"
#include "arki/runtime/processor.h"
#include "arki/runtime/source.h"
#include "arki/runtime/dispatch.h"
#include <arki/runtime/inputs.h>
#include <arki/runtime/io.h>
#include <arki/nag.h>
#include <iostream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace runtime {

ArkiScan::~ArkiScan()
{
    delete processor;
    delete dispatcher;
}

bool ArkiScan::run_scan_stdin(const std::string& format)
{
    return foreach_stdin(format, [&](runtime::Source& source) {
        processor->process(source.reader(), source.name());
    });
}

bool ArkiScan::run_scan_inputs()
{
    return foreach_sections(inputs, [&](runtime::Source& source) {
        processor->process(source.reader(), source.name());
    });
}

bool ArkiScan::run_dispatch_stdin(const std::string& format)
{
    bool dispatch_ok = true;
    bool res = foreach_stdin(format, [&](runtime::Source& source) {
        auto stats = dispatcher->process(source.reader(), source.name());
        dispatch_ok = stats.success && dispatch_ok;
    });
    return res && dispatch_ok;
}

bool ArkiScan::run_dispatch_inputs(const std::string& moveok, const std::string& moveko, const std::string& movework)
{
    bool dispatch_ok = true;

    bool all_successful = true;
    // Query all the datasets in sequence
    for (auto si: inputs)
    {
        FileSource source(si.second);
        source.movework = movework;
        source.moveok = moveok;
        source.moveko = moveko;

        nag::verbose("Processing %s...", source.name().c_str());
        source.open();
        bool success = true;
        try {
            auto stats = dispatcher->process(source.reader(), source.name());
            dispatch_ok = stats.success && dispatch_ok;
            success = stats.success;
        } catch (std::exception& e) {
            nag::warning("%s failed: %s", source.name().c_str(), e.what());
            success = false;
        }
        source.close(success);
        if (!success) all_successful = false;
    }
    return all_successful && dispatch_ok;
}

}
}
