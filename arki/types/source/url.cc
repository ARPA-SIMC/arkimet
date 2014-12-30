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

#include "url.h"
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

Source::Style URL::style() const { return Source::URL; }

void URL::encodeWithoutEnvelope(Encoder& enc) const
{
	Source::encodeWithoutEnvelope(enc);
	enc.addVarint(url.size());
	enc.addString(url);
}

std::ostream& URL::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << format << "," << url
			 << ")";
}
void URL::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Source::serialiseLocal(e, f);
    e.add("url"); e.add(url);
}
Item<URL> URL::decodeMapping(const emitter::memory::Mapping& val)
{
    return URL::create(
            val["f"].want_string("parsing url source format"),
            val["url"].want_string("parsing url source url"));
}

const char* URL::lua_type_name() const { return "arki.types.source.url"; }

#ifdef HAVE_LUA
bool URL::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "url")
		lua_pushlstring(L, url.data(), url.size());
	else
		return Source::lua_lookup(L, name);
	return true;
}
#endif

int URL::compare_local(const Source& o) const
{
	// We should be the same kind, so upcast
	const URL* v = dynamic_cast<const URL*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a URL Source, but is a ") + typeid(&o).name() + " instead");

	if (url < v->url) return -1;
	if (url > v->url) return 1;
	return 0;
}
bool URL::operator==(const Type& o) const
{
	const URL* v = dynamic_cast<const URL*>(&o);
	if (!v) return false;
	return format == v->format && url == v->url;
}

Item<URL> URL::create(const std::string& format, const std::string& url)
{
	URL* res = new URL;
	res->format = format;
	res->url = url;
	return res;
}

wibble::sys::Buffer URL::loadData() const
{
    throw wibble::exception::Consistency("retrieving data", "retrieving data from URL sources is not yet implemented");
    //return wibble::sys::Buffer();
    /*
    if (m_inline_buf.data())
        return m_inline_buf;
    throw wibble::exception::Consistency("retrieving data", "retrieving data from URL sources is not yet implemented");
    */
}

uint64_t URL::getSize() const
{
    if (m_inline_buf.data())
        return m_inline_buf.size();
    else
        return 0;
}

}
}
}
