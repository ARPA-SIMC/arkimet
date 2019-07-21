#include "config.h"
#include "arki/runtime.h"
#include "arki/runtime/config.h"
#include "arki/types-init.h"
#include "arki/iotrace.h"

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

}
}
