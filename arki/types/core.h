#ifndef ARKI_TYPES_CORE_H
#define ARKI_TYPES_CORE_H

#include <arki/types.h>

namespace arki {
namespace types {

template<typename BASE>
struct CoreType : public Type
{
    types::Code type_code() const override { return traits<BASE>::type_code; }
    size_t serialisationSizeLength() const override { return traits<BASE>::type_sersize_bytes; }
    std::string tag() const override { return traits<BASE>::type_tag; }
    const char* lua_type_name() const override { return traits<BASE>::type_lua_tag; }
    static void lua_loadlib(lua_State* L);
};

}
}

#endif
