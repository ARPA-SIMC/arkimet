#ifndef ARKI_TYPES_TCC
#define ARKI_TYPES_TCC

#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include <arki/binary.h>
#include "config.h"
#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

namespace arki {
namespace types {

template<typename BASE>
void CoreType<BASE>::lua_loadlib(lua_State* L)
{
	/* By default, do not register anything */
}

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

	res = this->style() - v->style();
	if (res != 0) return res;
	return this->compare_local(*v);
}

template<typename BASE>
void StyledType<BASE>::serialiseLocal(Emitter& e, const Formatter* f) const
{
    e.add("s", BASE::formatStyle(style()));
}

template<typename BASE>
typename StyledType<BASE>::Style StyledType<BASE>::style_from_mapping(const emitter::memory::Mapping& m)
{
    return BASE::parseStyle(m["s"].want_string("decoding Source style"));
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
#endif
