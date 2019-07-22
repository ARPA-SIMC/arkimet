#include "arki/runtime/arki-scan.h"
#include "arki/runtime/processor.h"
#include "arki/runtime/source.h"
#include "arki/runtime/dispatch.h"
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

}
}
