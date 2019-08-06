#ifndef ARKI_TYPES_CORE_TCC
#define ARKI_TYPES_CORE_TCC

#include "arki/types/core.h"

namespace arki {
namespace types {

template<typename BASE>
void CoreType<BASE>::lua_loadlib(lua_State* L)
{
    /* By default, do not register anything */
}


}
}

#endif

