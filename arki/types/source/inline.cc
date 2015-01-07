/*
 * types/source - Source information
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "inline.h"
#include <arki/utils/codec.h>
#include <arki/utils/lua.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>

using namespace std;
using namespace wibble;
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
std::auto_ptr<Inline> Inline::decodeMapping(const emitter::memory::Mapping& val)
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
    Inline* res = new Inline;
    res->format = format;
    res->size = size;
    if (m_inline_buf.data()) res->setCachedData(m_inline_buf);
    return res;
}

std::auto_ptr<Inline> Inline::create(const std::string& format, uint64_t size)
{
    Inline* res = new Inline;
    res->format = format;
    res->size = size;
    return auto_ptr<Inline>(res);
}

std::auto_ptr<Inline> Inline::create(const std::string& format, const wibble::sys::Buffer& buf)
{
    Inline* res = new Inline;
    res->format = format;
    res->size = buf.size();
    res->setCachedData(buf);
    return auto_ptr<Inline>(res);
}

uint64_t Inline::getSize() const { return size; }

void Inline::dropCachedData() const
{
    // Do nothing: the cached data is the only copy we have
}

wibble::sys::Buffer Inline::loadData() const
{
    return wibble::sys::Buffer();
    //return m_inline_buf;
}


}
}
}
