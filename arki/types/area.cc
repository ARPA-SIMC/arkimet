/*
 * types/area - Geographical area
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

namespace area {
// FIXME: move as a singleton to arki/bbox.cc?
static __thread BBox* bbox = 0;
}

// Style constants
const unsigned char Area::GRIB;

Area::Style Area::parseStyle(const std::string& str)
{
	if (str == "GRIB") return GRIB;
	throw wibble::exception::Consistency("parsing Area style", "cannot parse Area style '"+str+"': only GRIB is supported");
}

std::string Area::formatStyle(Area::Style s)
{
	switch (s)
	{
		case Area::GRIB: return "GRIB";
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

int Area::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Area* v = dynamic_cast<const Area*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Area, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Area::compare(const Area& o) const
{
	return style() - o.style();
}

types::Code Area::serialisationCode() const { return CODE; }
size_t Area::serialisationSizeLength() const { return SERSIZELEN; }
std::string Area::tag() const { return TAG; }

void Area::encodeWithoutEnvelope(Encoder& enc) const
{
	enc.addUInt(style(), 1);
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
		default:
			throw wibble::exception::Consistency("parsing Area", "unknown Area style " + formatStyle(style));
	}
}

//////////////////////////////
#ifdef HAVE_LUA
int Area::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Area reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const Area& v = **(const Area**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "style")
	{
		string s = Area::formatStyle(v.style());
		lua_pushlstring(L, s.data(), s.size());
		return 1;
	}
	else if (name == "grib" && v.style() == Area::GRIB)
	{
		const area::GRIB* v1 = v.upcast<area::GRIB>();
		v1->values.lua_push(L);
		return 1;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_area(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Area::lua_lookup, 2);
	return 1;
}
void Area::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Area** s = (const Area**)lua_newuserdata(L, sizeof(const Area*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_area);
		lua_settable(L, -3);
		/* set the __tostring metamethod */
		lua_pushstring(L, "__tostring");
		lua_pushcfunction(L, utils::lua::tostring_arkitype<Area>);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

namespace area {

Area::Style GRIB::style() const { return Area::GRIB; }

void GRIB::encodeWithoutEnvelope(Encoder& enc) const
{
	Area::encodeWithoutEnvelope(enc);
	values.encode(enc);
}
std::ostream& GRIB::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "(" << values.toString() << ")";
}

int GRIB::compare(const Area& o) const
{
	if (int res = Area::compare(o)) return res;

	// We should be the same kind, so upcast
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB Area, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int GRIB::compare(const GRIB& o) const
{
	return values.compare(o.values);
}

bool GRIB::operator==(const Type& o) const
{
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v) return false;
	return values == v->values;
}

Item<GRIB> GRIB::create(const ValueBag& values)
{
	GRIB* res = new GRIB;
	res->values = values;
	return res;
}

}

static MetadataType areaType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Area::decode),
	(MetadataType::string_decoder)(&Area::decodeString));

}
}
// vim:set ts=4 sw=4:
