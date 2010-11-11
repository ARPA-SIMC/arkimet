/*
 * types/area - Geographical area
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/exception.h>
#include <wibble/string.h>
#include <arki/types/area.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/utils/geosdef.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include <arki/bbox.h>
#include "config.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_AREA
#define TAG "area"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki {
namespace types {

const char* traits<Area>::type_tag = TAG;
const types::Code traits<Area>::type_code = CODE;
const size_t traits<Area>::type_sersize_bytes = SERSIZELEN;
const char* traits<Area>::type_lua_tag = LUATAG_TYPES ".area";

namespace area {
// FIXME: move as a singleton to arki/bbox.cc?
static __thread BBox* bbox = 0;
}

// Style constants
const unsigned char Area::GRIB;
const unsigned char Area::ODIMH5;

Area::Style Area::parseStyle(const std::string& str)
{
	if (str == "GRIB") return GRIB;
	if (str == "ODIMH5") return ODIMH5;
	throw wibble::exception::Consistency("parsing Area style", "cannot parse Area style '"+str+"': only GRIB,ODIMH5 is supported");
}

std::string Area::formatStyle(Area::Style s)
{
	switch (s)
	{
		case Area::GRIB: return "GRIB";
		case Area::ODIMH5: return "ODIMH5";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

Area::Area() : cached_bbox(0)
{
}

const ARKI_GEOS_GEOMETRY* Area::bbox() const
{
#ifdef HAVE_GEOS
	if (!cached_bbox)
	{
		// Create the bbox generator if missing
		if (!area::bbox) area::bbox = new BBox();

		std::auto_ptr<ARKI_GEOS_GEOMETRY> res = (*area::bbox)(*this);
		if (res.get())
			cached_bbox = res.release();
	}
#endif
	return cached_bbox;
}

Item<Area> Area::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	ensureSize(len, 1, "Area");
	Style s = (Style)decodeUInt(buf, 1);
	switch (s)
	{
		case GRIB:
			return area::GRIB::create(ValueBag::decode(buf+1, len-1));
		case ODIMH5:
			return area::ODIMH5::create(ValueBag::decode(buf+1, len-1));
		default:
			throw wibble::exception::Consistency("parsing Area", "style is " + formatStyle(s) + " but we can only decode GRIB");
	}
}

Item<Area> Area::decodeString(const std::string& val)
{
	string inner;
	Area::Style style = outerParse<Area>(val, inner);
	switch (style)
	{
		case Area::GRIB: return area::GRIB::create(ValueBag::parse(inner)); 
		case Area::ODIMH5: return area::ODIMH5::create(ValueBag::parse(inner)); 
		default:
			throw wibble::exception::Consistency("parsing Area", "unknown Area style " + formatStyle(style));
	}
}

Item<Area> Area::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case Area::GRIB: return area::GRIB::decodeMapping(val);
        case Area::ODIMH5: return area::ODIMH5::decodeMapping(val);
        default:
            throw wibble::exception::Consistency("parsing Area", "unknown Area style " + val.get_string());
    }
}

#ifdef HAVE_LUA
static int arkilua_new_grib(lua_State* L)
{
	luaL_checktype(L, 1, LUA_TTABLE);
	ValueBag vals;
	vals.load_lua_table(L);
	area::GRIB::create(vals)->lua_push(L);
	return 1;
}

static int arkilua_new_odimh5(lua_State* L)
{
	luaL_checktype(L, 1, LUA_TTABLE);
	ValueBag vals;
	vals.load_lua_table(L);
	area::ODIMH5::create(vals)->lua_push(L);
	return 1;
}

void Area::lua_loadlib(lua_State* L)
{
	static const struct luaL_reg lib [] = {
		{ "grib", arkilua_new_grib },
		{ "odimh5", arkilua_new_odimh5 },
		{ NULL, NULL }
	};
	luaL_openlib(L, "arki_area", lib, 0);
	lua_pop(L, 1);
}
#endif

namespace area {

static TypeCache<GRIB> cache_grib;
static TypeCache<ODIMH5> cache_odimh5;

GRIB::~GRIB() { /* cache_grib.uncache(this); */ }

Area::Style GRIB::style() const { return Area::GRIB; }

void GRIB::encodeWithoutEnvelope(Encoder& enc) const
{
	Area::encodeWithoutEnvelope(enc);
	m_values.encode(enc);
}
std::ostream& GRIB::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "(" << m_values.toString() << ")";
}
void GRIB::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Area::serialiseLocal(e, f);
    e.add("va");
    m_values.serialise(e);
}
Item<GRIB> GRIB::decodeMapping(const emitter::memory::Mapping& val)
{
    return GRIB::create(ValueBag::parse(val["va"].get_mapping()));
}
std::string GRIB::exactQuery() const
{
    return "GRIB:" + m_values.toString();
}

const char* GRIB::lua_type_name() const { return "arki.types.area.grib"; }

#ifdef HAVE_LUA
bool GRIB::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "val")
		values().lua_push(L);
	else
		return Area::lua_lookup(L, name);
	return true;
}
#endif

int GRIB::compare_local(const Area& o) const
{
	// We should be the same kind, so upcast
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB Area, but is a ") + typeid(&o).name() + " instead");

	return m_values.compare(v->m_values);
}

bool GRIB::operator==(const Type& o) const
{
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v) return false;
	return m_values == v->m_values;
}

Item<GRIB> GRIB::create(const ValueBag& values)
{
	GRIB* res = new GRIB;
	res->m_values = values;
	return cache_grib.intern(res);
}

ODIMH5::~ODIMH5() { /* cache_odimh5.uncache(this); */ }

Area::Style ODIMH5::style() const { return Area::ODIMH5; }

void ODIMH5::encodeWithoutEnvelope(Encoder& enc) const
{
	Area::encodeWithoutEnvelope(enc);
	m_values.encode(enc);
}
std::ostream& ODIMH5::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "(" << m_values.toString() << ")";
}
void ODIMH5::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Area::serialiseLocal(e, f);
    e.add("va");
    m_values.serialise(e);
}
Item<ODIMH5> ODIMH5::decodeMapping(const emitter::memory::Mapping& val)
{
    return ODIMH5::create(ValueBag::parse(val["va"].get_mapping()));
}
std::string ODIMH5::exactQuery() const
{
    return "ODIMH5:" + m_values.toString();
}

const char* ODIMH5::lua_type_name() const { return "arki.types.area.odimh5"; }

#ifdef HAVE_LUA
bool ODIMH5::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "val")
		values().lua_push(L);
	else
		return Area::lua_lookup(L, name);
	return true;
}
#endif

int ODIMH5::compare_local(const Area& o) const
{
	// We should be the same kind, so upcast
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a ODIMH5 Area, but is a ") + typeid(&o).name() + " instead");

	return m_values.compare(v->m_values);
}

bool ODIMH5::operator==(const Type& o) const
{
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v) return false;
	return m_values == v->m_values;
}

Item<ODIMH5> ODIMH5::create(const ValueBag& values)
{
	ODIMH5* res = new ODIMH5;
	res->m_values = values;
	return cache_odimh5.intern(res);
}

static void debug_interns()
{
	fprintf(stderr, "Area GRIB: sz %zd reused %zd\n", cache_grib.size(), cache_grib.reused());
	fprintf(stderr, "Area ODIMH5: sz %zd reused %zd\n", cache_odimh5.size(), cache_odimh5.reused());
}

}

static MetadataType areaType = MetadataType::create<Area>(area::debug_interns);

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
