/*
 * types/bbox - Bounding box metadata item
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
#include <wibble/regexp.h>
#include <arki/types/bbox.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include "config.h"
#include <iomanip>
#include <sstream>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#ifdef HAVE_GEOS
#include <memory>
#if GEOS_VERSION < 3
#include <geos/geom.h>

using namespace geos;

typedef DefaultCoordinateSequence CoordinateArraySequence;
#else
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Point.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Polygon.h>

using namespace geos::geom;
#endif
#endif

#define CODE types::TYPE_BBOX
#define TAG "bbox"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;
using namespace wibble;

namespace arki {
namespace types {

// Style constants
//const unsigned char BBox::NONE;
const unsigned char BBox::INVALID;
const unsigned char BBox::POINT;
const unsigned char BBox::BOX;
const unsigned char BBox::HULL;

BBox::Style BBox::parseStyle(const std::string& str)
{
	if (str == "INVALID") return INVALID;
	if (str == "POINT") return POINT;
	if (str == "BOX") return BOX;
	if (str == "HULL") return HULL;
	throw wibble::exception::Consistency("parsing BBox style", "cannot parse BBox style '"+str+"': only INVALID and BOX are supported");
}

std::string BBox::formatStyle(BBox::Style s)
{
	switch (s)
	{
		//case BBox::NONE: return "NONE";
		case BBox::INVALID: return "INVALID";
		case BBox::POINT: return "POINT";
		case BBox::BOX: return "BOX";
		case BBox::HULL: return "HULL";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

int BBox::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const BBox* v = dynamic_cast<const BBox*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be an BBox, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int BBox::compare(const BBox& o) const
{
	return style() - o.style();
}

types::Code BBox::serialisationCode() const { return CODE; }
size_t BBox::serialisationSizeLength() const { return SERSIZELEN; }
std::string BBox::tag() const { return TAG; }

void BBox::encodeWithoutEnvelope(Encoder& enc) const
{
	enc.addUInt(style(), 1);
}

Item<BBox> BBox::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	ensureSize(len, 1, "BBox");
	Style s = (Style)decodeUInt(buf, 1);
	switch (s)
	{
		case INVALID:
			return bbox::INVALID::create();
		case POINT:
			ensureSize(len, 9, "BBox");
			decodeFloat(buf+1), decodeFloat(buf+5);
			return bbox::INVALID::create();
		case BOX:
			ensureSize(len, 17, "BBox");
			decodeFloat(buf+1), decodeFloat(buf+5), decodeFloat(buf+9), decodeFloat(buf+13);
			return bbox::INVALID::create();
		case HULL: {
			ensureSize(len, 3, "BBox");
			size_t pointCount = decodeUInt(buf+1, 2);
			ensureSize(len, 3+pointCount*8, "BBox");
			for (size_t i = 0; i < pointCount; ++i)
				decodeFloat(buf+3+i*8), decodeFloat(buf+3+i*8+4);
			return bbox::INVALID::create();
		}
		default:
			throw wibble::exception::Consistency("parsing BBox", "style is " + formatStyle(s) + " but we can only decode INVALID and BOX");
	}
}
    
Item<BBox> BBox::decodeString(const std::string& val)
{
	return bbox::INVALID::create();
}

#ifdef HAVE_LUA
int BBox::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the BBox reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const BBox& v = **(const BBox**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "style")
	{
		string s = BBox::formatStyle(v.style());
		lua_pushlstring(L, s.data(), s.size());
		return 1;
	}
	else if (name == "invalid" && v.style() == BBox::INVALID)
	{
		lua_pushnil(L);
		return 1;
	}
	else if (name == "point" && v.style() == BBox::POINT)
	{
		lua_pushnil(L);
		lua_pushnil(L);
		return 2;
	}
	else if (name == "box" && v.style() == BBox::BOX)
	{
		lua_pushnil(L);
		lua_pushnil(L);
		lua_pushnil(L);
		lua_pushnil(L);
		return 4;
	}
	else if (name == "hull" && v.style() == BBox::HULL)
	{
		lua_newtable(L);
		return 1;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_bbox(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, BBox::lua_lookup, 2);
	return 1;
}
void BBox::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const BBox** s = (const BBox**)lua_newuserdata(L, sizeof(const BBox*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_bbox);
		lua_settable(L, -3);
		/* set the __tostring metamethod */
		lua_pushstring(L, "__tostring");
		lua_pushcfunction(L, utils::lua::tostring_arkitype<BBox>);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

namespace bbox {

BBox::Style INVALID::style() const { return BBox::INVALID; }

void INVALID::encodeWithoutEnvelope(Encoder& enc) const
{
	BBox::encodeWithoutEnvelope(enc);
}
std::ostream& INVALID::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "()";
}

int INVALID::compare(const BBox& o) const
{
	int res = BBox::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const INVALID* v = dynamic_cast<const INVALID*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB1 BBox, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int INVALID::compare(const INVALID& o) const
{
	return 0;
}

bool INVALID::operator==(const Type& o) const
{
	const INVALID* v = dynamic_cast<const INVALID*>(&o);
	if (!v) return false;
	return true;
}

Geometry* INVALID::geometry(const GeometryFactory& gf) const
{
	return 0;
}

Item<INVALID> INVALID::create()
{
	return new INVALID;
}

}

static MetadataType bboxType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&BBox::decode),
	(MetadataType::string_decoder)(&BBox::decodeString));

}
}
// vim:set ts=4 sw=4:
