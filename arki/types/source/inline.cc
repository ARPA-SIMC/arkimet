#include "inline.h"
#include "arki/binary.h"
#include "arki/utils/lua.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/exceptions.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {
namespace source {

source::Style Inline::style() const { return source::Style::INLINE; }

void Inline::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Source::encodeWithoutEnvelope(enc);
    enc.add_varint(size);
}

std::ostream& Inline::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
             << format << "," << size
             << ")";
}
void Inline::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Source::serialise_local(e, keys, f);
    e.add(keys.source_size, size);
}

std::unique_ptr<Inline> Inline::decode_structure(const structured::Keys& keys, const structured::Reader& reader)
{
    return Inline::create(
            reader.as_string(keys.source_format, "source format"),
            reader.as_int(keys.source_size, "source size"));
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
        throw_consistency_error(
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
