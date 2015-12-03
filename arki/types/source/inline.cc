#include "inline.h"
#include <arki/utils/codec.h>
#include <arki/utils/lua.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki {
namespace types {
namespace source {

Source::Style Inline::style() const { return Source::INLINE; }

void Inline::encodeWithoutEnvelope(Encoder& enc) const
{
    Source::encodeWithoutEnvelope(enc);
    enc.addVarint(size);
}

std::ostream& Inline::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
             << format << "," << size
             << ")";
}
void Inline::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Source::serialiseLocal(e, f);
    e.add("sz", size);
}
std::unique_ptr<Inline> Inline::decodeMapping(const emitter::memory::Mapping& val)
{
    return Inline::create(
            val["f"].want_string("parsing inline source format"),
            val["sz"].want_int("parsing inline source size"));
}

const char* Inline::lua_type_name() const { return "arki.types.source.inline"; }

#ifdef HAVE_LUA
bool Inline::lua_lookup(lua_State* L, const std::string& name) const
{
    if (name == "size")
        lua_pushnumber(L, size);
    else
        return Source::lua_lookup(L, name);
    return true;
}
#endif

int Inline::compare_local(const Source& o) const
{
    if (int res = Source::compare_local(o)) return res;
    // We should be the same kind, so upcast
    const Inline* v = dynamic_cast<const Inline*>(&o);
    if (!v)
        throw wibble::exception::Consistency(
            "comparing metadata types",
            string("second element claims to be a Inline Source, but is a ") + typeid(&o).name() + " instead");

    return size - v->size;
}
bool Inline::equals(const Type& o) const
{
    const Inline* v = dynamic_cast<const Inline*>(&o);
    if (!v) return false;
    return format == v->format && size == v->size;
}

Inline* Inline::clone() const
{
    return new Inline(*this);
}

std::unique_ptr<Inline> Inline::create(const std::string& format, uint64_t size)
{
    Inline* res = new Inline;
    res->format = format;
    res->size = size;
    return unique_ptr<Inline>(res);
}

}
}
}
