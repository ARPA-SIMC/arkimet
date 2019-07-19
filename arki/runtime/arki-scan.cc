#include "arki/runtime/arki-scan.h"
#include "arki/runtime/processor.h"
#include "arki/runtime/source.h"
#include "arki/runtime/dispatch.h"
#include <arki/runtime/inputs.h>
#include <arki/runtime/io.h>
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

std::function<bool(runtime::Source& source)> ArkiScan::make_dest_func(bool& dispatch_ok)
{
    if (dispatcher)
    {
        return [&](runtime::Source& source) {
            auto stats = dispatcher->process(source.reader(), source.name());
            dispatch_ok = stats.success && dispatch_ok;
            return true;
        };
    } else {
        return [&](runtime::Source& source) {
            processor->process(source.reader(), source.name());
            return true;
        };
    }
}

bool ArkiScan::run_scan_stdin(const std::string& format)
{
    bool dispatch_ok = true;
    auto dest = make_dest_func(dispatch_ok);
    return foreach_stdin(format, dest) && dispatch_ok;
}

bool ArkiScan::run_scan_inputs(const std::string& moveok, const std::string& moveko, const std::string& movework)
{
    bool dispatch_ok = true;
    auto dest = make_dest_func(dispatch_ok);
    return foreach_sections(inputs, moveok, moveko, movework, dest) && dispatch_ok;
}

}
}
