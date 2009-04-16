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
#include <arki/utils.h>
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

std::string BBox::encodeWithoutEnvelope() const
{
	using namespace utils;
	return encodeUInt(style(), 1);
}

Item<BBox> BBox::decode(const unsigned char* buf, size_t len)
{
	using namespace utils;
	ensureSize(len, 1, "BBox");
	Style s = (Style)decodeUInt(buf, 1);
	switch (s)
	{
		case INVALID:
			return bbox::INVALID::create();
		case POINT:
			ensureSize(len, 9, "BBox");
			return bbox::POINT::create(decodeFloat(buf+1), decodeFloat(buf+5));
		case BOX:
			ensureSize(len, 17, "BBox");
			return bbox::BOX::create(
				decodeFloat(buf+1), decodeFloat(buf+5), decodeFloat(buf+9), decodeFloat(buf+13));
		case HULL: {
			ensureSize(len, 3, "BBox");
			size_t pointCount = decodeUInt(buf+1, 2);
			ensureSize(len, 3+pointCount*8, "BBox");
			vector< pair<float, float> > points;
			for (size_t i = 0; i < pointCount; ++i)
				points.push_back(make_pair(decodeFloat(buf+3+i*8), decodeFloat(buf+3+i*8+4)));
			return bbox::HULL::create(points);
		}
		default:
			throw wibble::exception::Consistency("parsing BBox", "style is " + formatStyle(s) + " but we can only decode INVALID and BOX");
	}
}
    
Item<BBox> BBox::decodeString(const std::string& val)
{
	string inner;
	BBox::Style style = outerParse<BBox>(val, inner);
	switch (style)
	{
		//case BBox::NONE: return BBox();
		case BBox::INVALID: {
			return bbox::INVALID::create();
		}
		case BBox::POINT: {
			FloatList<2> nums(inner, "BBox");
			return bbox::POINT::create(nums.vals[0], nums.vals[1]);
		}
		case BBox::BOX: {
			FloatList<4> nums(inner, "BBox");
			return bbox::BOX::create(nums.vals[0], nums.vals[1], nums.vals[2], nums.vals[3]);
		}
		case BBox::HULL: {
			vector< pair<float, float> > points;
			wibble::Splitter split("[ \t]*,[ \t]*", REG_EXTENDED);
			wibble::Splitter split1("[ \t]+", REG_EXTENDED);
			// First split the , separators
			for (wibble::Splitter::const_iterator i = split.begin(inner); i != split.end(); ++i)
			{
				// Then split the point coordinates separated by space
				vector<string> coords;
				std::copy(split1.begin(*i), split1.end(), back_inserter(coords));
				if (coords.size() != 2)
					throw wibble::exception::Consistency("parsing HULL bbox", "point " + *i + " has " + str::fmt(coords.size()) + " coordinates instead of 2");
				points.push_back(make_pair(strtof(coords[0].c_str(), 0), strtof(coords[1].c_str(), 0)));
			}
			return bbox::HULL::create(points);
		}
		default:
			throw wibble::exception::Consistency("parsing BBox", "unknown BBox style " + formatStyle(style));
	}
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
		const bbox::POINT* v1 = v.upcast<bbox::POINT>();
		lua_pushnumber(L, v1->lat);
		lua_pushnumber(L, v1->lon);
		return 2;
	}
	else if (name == "box" && v.style() == BBox::BOX)
	{
		const bbox::BOX* v1 = v.upcast<bbox::BOX>();
		lua_pushnumber(L, v1->latmin);
		lua_pushnumber(L, v1->latmax);
		lua_pushnumber(L, v1->lonmin);
		lua_pushnumber(L, v1->lonmax);
		return 4;
	}
	else if (name == "hull" && v.style() == BBox::HULL)
	{
		const bbox::HULL* v1 = v.upcast<bbox::HULL>();
		lua_newtable(L);
		for (size_t i = 0; i < v1->points.size(); ++i)
		{
			lua_pushnumber(L, i);
			lua_newtable(L);
			lua_pushnumber(L, 0);
			lua_pushnumber(L, v1->points[i].first);
			lua_rawset(L, -3);
			lua_pushnumber(L, 1);
			lua_pushnumber(L, v1->points[i].second);
			lua_rawset(L, -3);
			lua_rawset(L, -3);
		}
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

std::string INVALID::encodeWithoutEnvelope() const
{
	using namespace utils;
	return BBox::encodeWithoutEnvelope();
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


BBox::Style POINT::style() const { return BBox::POINT; }

std::string POINT::encodeWithoutEnvelope() const
{
	using namespace utils;
	return BBox::encodeWithoutEnvelope()
			+ encodeFloat(lat) + encodeFloat(lon);
}
std::ostream& POINT::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sis(o);
    return o << formatStyle(style()) << "("
			 << setfill('0') << fixed << setprecision(4)
		     << setw(8) << lat << ", " << setw(8) << lon
			 << ")";
}

int POINT::compare(const BBox& o) const
{
	int res = BBox::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const POINT* v = dynamic_cast<const POINT*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a POINT BBox, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int POINT::compare(const POINT& o) const
{
	if (int res = lat - o.lat) return res;
	return lon - o.lon;
}
bool POINT::operator==(const Type& o) const
{
	const POINT* v = dynamic_cast<const POINT*>(&o);
	if (!v) return false;
	return lat == v->lat && lon == v->lon;
}

Geometry* POINT::geometry(const GeometryFactory& gf) const
{
#ifdef HAVE_GEOS
	return gf.createPoint(Coordinate(lon, lat));
#else
	return 0;
#endif
}

Item<POINT> POINT::create(float lat, float lon)
{
	POINT* res = new POINT;
	res->lat = lat;
	res->lon = lon;
	return res;
}


BBox::Style BOX::style() const { return BBox::BOX; }

std::string BOX::encodeWithoutEnvelope() const
{
	using namespace utils;
	return BBox::encodeWithoutEnvelope()
			+ encodeFloat(latmin) + encodeFloat(latmax)
			+ encodeFloat(lonmin) + encodeFloat(lonmax);
}
std::ostream& BOX::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sis(o);
    return o << formatStyle(style()) << "("
			 << setfill('0') << fixed << setprecision(4)
		     << setw(8) << latmin << ", " << setw(8) << latmax << ", "
			 << setw(8) << lonmin << ", " << setw(8) << lonmax
			 << ")";
}

int BOX::compare(const BBox& o) const
{
	int res = BBox::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const BOX* v = dynamic_cast<const BOX*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a BOX BBox, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int BOX::compare(const BOX& o) const
{
	if (int res = latmin - o.latmin) return res;
	if (int res = latmax - o.latmax) return res;
	if (int res = lonmin - o.lonmin) return res;
	return lonmax - o.lonmax;
}
bool BOX::operator==(const Type& o) const
{
	const BOX* v = dynamic_cast<const BOX*>(&o);
	if (!v) return false;
	return latmin == v->latmin && latmax == v->latmax 
		&& lonmin == v->lonmin && lonmax == v->lonmax;
}

Geometry* BOX::geometry(const GeometryFactory& gf) const
{
#ifdef HAVE_GEOS
	CoordinateArraySequence cas;
	cas.add(Coordinate(lonmin, latmin));
	cas.add(Coordinate(lonmax, latmin));
	cas.add(Coordinate(lonmax, latmax));
	cas.add(Coordinate(lonmin, latmax));
	cas.add(Coordinate(lonmin, latmin));
	auto_ptr<LinearRing> lr(gf.createLinearRing(cas));
	return gf.createPolygon(*lr, vector<Geometry*>());
#else
	return 0;
#endif
}

Item<BOX> BOX::create(
			  float latmin, float latmax,
			  float lonmin, float lonmax)
{
	BOX* res = new BOX;
	res->latmin = latmin;
	res->latmax = latmax;
	res->lonmin = lonmin;
	res->lonmax = lonmax;
	return res;
}



BBox::Style HULL::style() const { return BBox::HULL; }

std::string HULL::encodeWithoutEnvelope() const
{
	using namespace utils;
	string res = BBox::encodeWithoutEnvelope();
	res += encodeUInt(points.size(), 2);
	for (size_t i = 0; i < points.size(); ++i)
		res += encodeFloat(points[i].first) + encodeFloat(points[i].second);
	return res;
}
std::ostream& HULL::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sis(o);
    o << formatStyle(style()) << "(" << setfill('0') << fixed << setprecision(4);
	for (size_t i = 0; i < points.size(); ++i)
	{
		if (i) o << ", ";
		o << setw(8) << points[i].first << " " << setw(8) << points[i].second;
	}
	return o << ")";
}

int HULL::compare(const BBox& o) const
{
	int res = BBox::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const HULL* v = dynamic_cast<const HULL*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a HULL BBox, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int HULL::compare(const HULL& o) const
{
	if (int res = points.size() - o.points.size()) return res;

	for (size_t i = 0; i < points.size(); ++i)
	{
		if (int res = points[i].first - o.points[i].first) return res;
		if (int res = points[i].second - o.points[i].second) return res;
	}
	return 0;
}
bool HULL::operator==(const Type& o) const
{
	const HULL* v = dynamic_cast<const HULL*>(&o);
	if (!v) return false;
	return points == v->points;
}

Geometry* HULL::geometry(const GeometryFactory& gf) const
{
#ifdef HAVE_GEOS
	CoordinateArraySequence cas;
	for (size_t i = 0; i < points.size(); ++i)
		cas.add(Coordinate(points[i].second, points[i].first));
	auto_ptr<LinearRing> lr(gf.createLinearRing(cas));
	return gf.createPolygon(*lr, vector<Geometry*>());
#else
	return 0;
#endif
}

Item<HULL> HULL::create(const std::vector< std::pair<float, float> >& points)
{	
	HULL* res = new HULL;
	res->points = points;
	return res;
}


}

static MetadataType bboxType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&BBox::decode),
	(MetadataType::string_decoder)(&BBox::decodeString));

}
}
// vim:set ts=4 sw=4:
