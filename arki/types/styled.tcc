#ifndef ARKI_TYPES_STYLED_TCC
#define ARKI_TYPES_STYLED_TCC

#include "arki/types/styled.h"
#include "arki/binary.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#ifdef HAVE_LUA
#include "arki/utils/lua.h"
#endif
#include <sstream>

namespace arki {
namespace types {

template<typename BASE>
void StyledType<BASE>::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    enc.add_unsigned((unsigned)this->style(), 1);
}

template<typename BASE>
int StyledType<BASE>::compare(const Type& o) const
{
    int res = CoreType<BASE>::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const BASE* v = dynamic_cast<const BASE*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `" << traits<BASE>::type_tag << "', but it is `" << typeid(&o).name() << "' instead";
    }

    return this->compare_local(*v);
}

template<typename BASE>
void StyledType<BASE>::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.type_style, BASE::formatStyle(style()));
}

template<typename BASE>
typename StyledType<BASE>::Style StyledType<BASE>::style_from_structure(const structured::Keys& keys, const structured::Reader& reader)
{
    return BASE::parseStyle(reader.as_string(keys.type_style, "type style"));
}

#ifdef HAVE_LUA
template<typename TYPE>
bool StyledType<TYPE>::lua_lookup(lua_State* L, const std::string& name) const
{
    if (name == "style")
    {
        std::string s = TYPE::formatStyle(style());
        lua_pushlstring(L, s.data(), s.size());
        return true;
    }
    return false;
}
#endif

}
}

#include "arki/types/core.tcc"

#endif
