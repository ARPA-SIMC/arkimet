#include "config.h"
#include "arki/init.h"
#include "arki/matcher/aliases.h"
#include "arki/types-init.h"
#include "arki/iotrace.h"

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {

void init()
{
    static bool initialized = false;

    if (initialized) return;
    types::init_default_types();
    matcher::read_matcher_alias_database();
    iotrace::init();
    initialized = true;
}

}
