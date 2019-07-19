#include "config.h"
#include "arki/runtime.h"
#include "arki/runtime/config.h"
#include "arki/runtime/processor.h"
#include "arki/runtime/io.h"
#include "arki/runtime/module.h"
#include "arki/runtime/dispatch.h"
#include "arki/types-init.h"
#include "arki/iotrace.h"
#include "arki/nag.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {
namespace runtime {

void init()
{
    static bool initialized = false;

    if (initialized) return;
    types::init_default_types();
    runtime::readMatcherAliasDatabase();
    iotrace::init();
    initialized = true;
}


BaseCommandLine::BaseCommandLine(const std::string& name, int mansection)
    : StandardParserWithManpage(name, PACKAGE_VERSION, mansection, PACKAGE_BUGREPORT)
{
    using namespace arki::utils::commandline;

    infoOpts = addGroup("Options controlling verbosity");
    debug = infoOpts->add<BoolOption>("debug", 0, "debug", "", "debug output");
    verbose = infoOpts->add<BoolOption>("verbose", 0, "verbose", "", "verbose output");
}

BaseCommandLine::~BaseCommandLine()
{
    for (auto m: modules)
        delete m;
}

bool BaseCommandLine::parse(int argc, const char* argv[])
{
    if (StandardParserWithManpage::parse(argc, argv))
        return true;

    nag::init(verbose->isSet(), debug->isSet());

    for (auto m: modules)
        if (m->handle_parsed_options())
            return true;

    return false;
}

}
}
