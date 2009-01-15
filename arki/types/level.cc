/*
 * types/level - Vertical level or layer
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/level.h>
#include <arki/types/utils.h>
#include <arki/utils.h>
#include <config.h>
#include <sstream>
#include <iomanip>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils-lua.h>
#endif

#define CODE types::TYPE_LEVEL
#define TAG "level"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

// Style constants
const unsigned char Level::GRIB1;
const unsigned char Level::GRIB2S;
const unsigned char Level::GRIB2D;

// Constants from meteosatlib's libgrib
/// Level codes
typedef enum t_enum_GRIB_LEVELS {
  GRIB_LEVEL_UNKNOWN = -1,
  GRIB_LEVEL_RESERVED = 0,
  GRIB_LEVEL_SURFACE,
  GRIB_LEVEL_CLOUD_BASE,
  GRIB_LEVEL_CLOUD_TOP,
  GRIB_LEVEL_ISOTHERM_0_DEG,
  GRIB_LEVEL_ADIABATIC_CONDENSATION_LIFTED_FROM_SURFACE,
  GRIB_LEVEL_MAXIMUM_WIND,
  GRIB_LEVEL_TROPOPAUSE,
  GRIB_LEVEL_NOMINAL_ATMOSPHERE_TOP,
  GRIB_LEVEL_SEA_BOTTOM,
  GRIB_LEVEL_ISOTHERMAL_K = 20,
  GRIB_LEVEL_SATELLITE_METEOSAT7 = 54,
  GRIB_LEVEL_SATELLITE_METEOSAT8 = 55,
  GRIB_LEVEL_SATELLITE_METEOSAT9 = 56,
  GRIB_LEVEL_ISOBARIC_mb = 100,
  GRIB_LEVEL_LAYER_ISOBARIC_mb,
  GRIB_LEVEL_MEAN_SEA_LEVEL,
  GRIB_LEVEL_ALTITUDE_ABOVE_MSL_m,
  GRIB_LEVEL_LAYER_ALTITUDE_ABOVE_MSL_m,
  GRIB_LEVEL_HEIGHT_ABOVE_GROUND_m,
  GRIB_LEVEL_LAYER_HEIGHT_ABOVE_GROUND_m,
  GRIB_LEVEL_SIGMA,
  GRIB_LEVEL_LAYER_SIGMA,
  GRIB_LEVEL_HYBRID,
  GRIB_LEVEL_LAYER_HYBRID,
  GRIB_LEVEL_DEPTH_BELOW_SURFACE_cm,
  GRIB_LEVEL_LAYER_DEPTH_BELOW_SURFACE_cm,
  GRIB_LEVEL_ISENTROPIC_K,
  GRIB_LEVEL_LAYER_ISENTROPIC_K,
  GRIB_LEVEL_PRESSURE_DIFFERENCE_FROM_GROUND_mb,
  GRIB_LEVEL_LAYER_PRESSURE_DIFFERENCE_FROM_GROUND_mb,
  GRIB_LEVEL_POTENTIAL_VORTICITY_SURFACE_PV_UNITS,
  GRIB_LEVEL_ETA,
  GRIB_LEVEL_LAYER_ETA,
  GRIB_LEVEL_LAYER_ISOBARIC_HIGH_PRECISION_mb,
  GRIB_LEVEL_HEIGHT_ABOVE_GROUND_HIGH_PRECISION_cm,
  GRIB_LEVEL_ISOBARIC_Pa,
  GRIB_LEVEL_LAYER_SIGMA_HIGH_PRECISION,
  GRIB_LEVEL_LAYER_ISOBARIC_MIXED_PRECISION_mb,
  GRIB_LEVEL_DEPTH_BELOW_SEA_m = 160,
  GRIB_LEVEL_ENTIRE_ATMOSPHERE = 200,
  GRIB_LEVEL_ENTIRE_OCEAN,
  GRIB_LEVEL_SPECIAL = 204
} t_enum_GRIB_LEVELS;



Level::Style Level::parseStyle(const std::string& str)
{
	if (str == "GRIB1") return GRIB1;
	if (str == "GRIB2S") return GRIB2S;
	if (str == "GRIB2D") return GRIB2D;
	throw wibble::exception::Consistency("parsing Level style", "cannot parse Level style '"+str+"': only GRIB1, GRIB2S and GRIB2D are supported");
}

std::string Level::formatStyle(Level::Style s)
{
	switch (s)
	{
		case Level::GRIB1: return "GRIB1";
		case Level::GRIB2S: return "GRIB2S";
		case Level::GRIB2D: return "GRIB2D";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

int Level::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Level* v = dynamic_cast<const Level*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Level, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Level::compare(const Level& o) const
{
	return style() - o.style();
}

types::Code Level::serialisationCode() const { return CODE; }
size_t Level::serialisationSizeLength() const { return SERSIZELEN; }
std::string Level::tag() const { return TAG; }

std::string Level::encodeWithoutEnvelope() const
{
	using namespace utils;
	return encodeUInt(style(), 1);
}

Item<Level> Level::decode(const unsigned char* buf, size_t len)
{
	using namespace utils;
	ensureSize(len, 1, "Level");
	Style s = (Style)decodeUInt(buf, 1);
	buf += 1; len -= 1;
	switch (s)
	{
		case GRIB1: {
			ensureSize(len, 1, "Level type");
			unsigned char ltype = decodeUInt(buf, 1);
			buf += 1; len -= 1;
			switch (level::GRIB1::getValType(ltype))
			{
				case 0:
					return level::GRIB1::create(ltype);
				case 1:
					ensureSize(len, 2, "Level value");
					return level::GRIB1::create(ltype, decodeUInt(buf, 2));
				default:
					ensureSize(len, 2, "Level value");
					return level::GRIB1::create(ltype, decodeUInt(buf, 1), decodeUInt(buf+1, 1));
			}
		}
		case GRIB2S: {
			ensureSize(len, 6, "Level information");
			return level::GRIB2S::create(decodeUInt(buf, 1), decodeUInt(buf+1, 1), decodeUInt(buf+2, 4));
		}
		case GRIB2D: {
			ensureSize(len, 12, "Level type");
			return level::GRIB2D::create(
						decodeUInt(buf+0, 1), decodeUInt(buf+1, 1), decodeUInt(buf+2, 4),
						decodeUInt(buf+6, 1), decodeUInt(buf+7, 1), decodeUInt(buf+8, 4));
		}
		default:
			throw wibble::exception::Consistency("parsing Level", "style is " + formatStyle(s) + " but we can only decode GRIB1, GRIB2S and GRIB2D");
	}
}

static int getNumber(const char * & start, const char* what)
{
	char* endptr;

	if (!*start)
		throw wibble::exception::Consistency("parsing Level", string("no ") + what + " after level type");

	int res = strtol(start, &endptr, 10);
	if (endptr == start)
		throw wibble::exception::Consistency("parsing Level",
				string("expected ") + what + ", but found \"" + start + "\"");
	start = endptr;

	// Skip colons and spaces, if any
	while (*start && (::isspace(*start) || *start == ','))
		++start;

	return res;
}

Item<Level> Level::decodeString(const std::string& val)
{
	string inner;
	Level::Style style = outerParse<Level>(val, inner);
	switch (style)
	{
		case Level::GRIB1: {
			const char* start = inner.c_str();

			int type = getNumber(start, "level type");
			switch (level::GRIB1::getValType((unsigned char)type))
			{
				case 0: return level::GRIB1::create(type);
				case 1: {
					int l1 = getNumber(start, "level value");
					return level::GRIB1::create(type, l1);
				}
				default: {
					int l1 = getNumber(start, "first level value");
					int l2 = getNumber(start, "second level value");
					return level::GRIB1::create(type, l1, l2);
				}
			}
		}
		case Level::GRIB2S: {
			const char* start = inner.c_str();

			int type = getNumber(start, "level type");
			int scale = getNumber(start, "scale of level value");
			int value = getNumber(start, "level value");
			return level::GRIB2S::create(type, scale, value);
		}
		case Level::GRIB2D: {
			const char* start = inner.c_str();

			int type1 = getNumber(start, "type of first level");
			int scale1 = getNumber(start, "scale of value of first level");
			int value1 = getNumber(start, "value of first level");
			int type2 = getNumber(start, "type of second level");
			int scale2 = getNumber(start, "scale of value of second level");
			int value2 = getNumber(start, "value of second level");
			return level::GRIB2D::create(type1, scale1, value1, type2, scale2, value2);
		}
		default:
			throw wibble::exception::Consistency("parsing Level", "unknown Level style " + formatStyle(style));
	}
}

#ifdef HAVE_LUA
int Level::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Level reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const Level& v = **(const Level**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "style")
	{
		string s = Level::formatStyle(v.style());
		lua_pushlstring(L, s.data(), s.size());
		return 1;
	}
	else if (name == "grib1" && v.style() == Level::GRIB1)
	{
		const level::GRIB1* v1 = v.upcast<level::GRIB1>();
		lua_pushnumber(L, v1->type);
		lua_pushnumber(L, v1->l1);
		lua_pushnumber(L, v1->l2);
		return 3;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_level(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Level::lua_lookup, 2);
	return 1;
}
void Level::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Level** s = (const Level**)lua_newuserdata(L, sizeof(const Level*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_level);
		lua_settable(L, -3);
		/* set the __tostring metamethod */
		lua_pushstring(L, "__tostring");
		lua_pushcfunction(L, utils::lua::tostring_arkitype<Level>);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

namespace level {

Level::Style GRIB1::style() const { return Level::GRIB1; }

std::string GRIB1::encodeWithoutEnvelope() const
{
	using namespace utils;
	string res = Level::encodeWithoutEnvelope() + encodeUInt(type, 1);
	switch (valType())
	{
		case 0: break;
		case 1: res += encodeUInt(l1, 2); break;
		default: res += encodeUInt(l1, 1) + encodeUInt(l2, 1); break;
	}
	return res;
}
std::ostream& GRIB1::writeToOstream(std::ostream& o) const
{
    o << formatStyle(style()) << "(";
	o << setfill('0') << internal;
	o << setw(3) << (int)type;
	switch (valType())
	{
		case 0: break;
		case 1:
			 o << ", " << setw(5) << (int)l1;
			 break;
		default:
			 o << ", " << setw(3) << (int)l1 << ", " << setw(3) << (int)l2;
			 break;
	}
	o << setfill(' ');
	return o << ")";
}

int GRIB1::compare(const Level& o) const
{
	int res = Level::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Level, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int GRIB1::compare(const GRIB1& o) const
{
	if (int res = type - o.type) return res;
	if (int res = l1 - o.l1) return res;
	return l2 - o.l2;
}

bool GRIB1::operator==(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;
	return type == v->type && l1 == v->l1 && l2 == v->l2;
}

Item<GRIB1> GRIB1::create(unsigned char type)
{
	GRIB1* res = new GRIB1;
	res->type = type;
	res->l1 = 0;
	res->l2 = 0;
	return res;
}

Item<GRIB1> GRIB1::create(unsigned char type, unsigned short l1)
{
	GRIB1* res = new GRIB1;
	res->type = type;
	switch (getValType(type))
	{
		case 0: res->l1 = 0; break;
		default: res->l1 = l1; break;
	}
	res->l2 = 0;
	return res;
}

Item<GRIB1> GRIB1::create(unsigned char type, unsigned char l1, unsigned char l2)
{
	GRIB1* res = new GRIB1;
	res->type = type;
	switch (getValType(type))
	{
		case 0: res->l1 = 0; res->l2 = 0; break;
		case 1: res->l1 = l1; res->l2 = 0; break;
		default: res->l1 = l1; res->l2 = l2; break;
	}
	return res;
}

int GRIB1::valType() const
{
	return getValType(type);
}

int GRIB1::getValType(unsigned char type)
{
	switch ((t_enum_GRIB_LEVELS)type)
	{
		case GRIB_LEVEL_SURFACE:
		case GRIB_LEVEL_CLOUD_BASE:
		case GRIB_LEVEL_CLOUD_TOP:
		case GRIB_LEVEL_ISOTHERM_0_DEG:
		case GRIB_LEVEL_ADIABATIC_CONDENSATION_LIFTED_FROM_SURFACE:
		case GRIB_LEVEL_MAXIMUM_WIND:
		case GRIB_LEVEL_TROPOPAUSE:
		case GRIB_LEVEL_NOMINAL_ATMOSPHERE_TOP:
		case GRIB_LEVEL_ENTIRE_ATMOSPHERE:
		case GRIB_LEVEL_ENTIRE_OCEAN:
		case GRIB_LEVEL_MEAN_SEA_LEVEL:
			return 0;
		case GRIB_LEVEL_ISOBARIC_mb:
		case GRIB_LEVEL_ALTITUDE_ABOVE_MSL_m:
		case GRIB_LEVEL_HEIGHT_ABOVE_GROUND_m:
		case GRIB_LEVEL_HYBRID:
		case GRIB_LEVEL_DEPTH_BELOW_SURFACE_cm:
		case GRIB_LEVEL_ISENTROPIC_K:
		case GRIB_LEVEL_PRESSURE_DIFFERENCE_FROM_GROUND_mb:
		case GRIB_LEVEL_POTENTIAL_VORTICITY_SURFACE_PV_UNITS:
		case GRIB_LEVEL_ISOBARIC_Pa:
		case GRIB_LEVEL_DEPTH_BELOW_SEA_m:
		case GRIB_LEVEL_HEIGHT_ABOVE_GROUND_HIGH_PRECISION_cm:
		case GRIB_LEVEL_ISOTHERMAL_K:
		case GRIB_LEVEL_SIGMA:
		case GRIB_LEVEL_ETA:
			return 1;
		case GRIB_LEVEL_LAYER_HYBRID:
		case GRIB_LEVEL_LAYER_DEPTH_BELOW_SURFACE_cm:
		case GRIB_LEVEL_LAYER_PRESSURE_DIFFERENCE_FROM_GROUND_mb:
		case GRIB_LEVEL_LAYER_ISOBARIC_mb:
		case GRIB_LEVEL_LAYER_ALTITUDE_ABOVE_MSL_m:
		case GRIB_LEVEL_LAYER_HEIGHT_ABOVE_GROUND_m:
		case GRIB_LEVEL_LAYER_SIGMA:
		case GRIB_LEVEL_LAYER_ETA:
		case GRIB_LEVEL_LAYER_ISENTROPIC_K:
		case GRIB_LEVEL_LAYER_ISOBARIC_HIGH_PRECISION_mb:
		case GRIB_LEVEL_LAYER_SIGMA_HIGH_PRECISION:
		case GRIB_LEVEL_LAYER_ISOBARIC_MIXED_PRECISION_mb:
		default:
			return 2;
	}
}


Level::Style GRIB2S::style() const { return Level::GRIB2S; }

std::string GRIB2S::encodeWithoutEnvelope() const
{
	using namespace utils;
	string res = Level::encodeWithoutEnvelope()
	           + encodeUInt(type, 1) + encodeUInt(scale, 1) + encodeUInt(value, 4);
	return res;
}
std::ostream& GRIB2S::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sios(o);
    return o
	  << formatStyle(style()) << "("
	  << setfill('0') << internal
	  << setw(3) << (int)type << ", "
	  << setw(3) << (int)scale << ", "
	  << setw(10) << (int)value << ")";
}

int GRIB2S::compare(const Level& o) const
{
	int res = Level::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const GRIB2S* v = dynamic_cast<const GRIB2S*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB2S Level, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int GRIB2S::compare(const GRIB2S& o) const
{
	// FIXME: here we can handle uniforming the scales if needed
	if (int res = type - o.type) return res;
	if (int res = scale - o.scale) return res;
	return value - o.value;
}

bool GRIB2S::operator==(const Type& o) const
{
	const GRIB2S* v = dynamic_cast<const GRIB2S*>(&o);
	if (!v) return false;
	// FIXME: here we can handle uniforming the scales if needed
	return type == v->type && scale == v->scale && value == v->value;
}

Item<GRIB2S> GRIB2S::create(unsigned char type, unsigned char scale, unsigned long value)
{
	GRIB2S* res = new GRIB2S;
	res->type = type;
	res->scale = scale;
	res->value = value;
	return res;
}


Level::Style GRIB2D::style() const { return Level::GRIB2D; }

std::string GRIB2D::encodeWithoutEnvelope() const
{
	using namespace utils;
	string res = Level::encodeWithoutEnvelope()
	           + encodeUInt(type1, 1) + encodeUInt(scale1, 1) + encodeUInt(value1, 4)
	           + encodeUInt(type2, 1) + encodeUInt(scale2, 1) + encodeUInt(value2, 4);
	return res;
}
std::ostream& GRIB2D::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sios(o);
    return o
	  << formatStyle(style()) << "("
	  << setfill('0') << internal
	  << setw(3) << (int)type1 << ", "
	  << setw(3) << (int)scale1 << ", "
	  << setw(10) << (int)value1 << ", "
	  << setw(3) << (int)type2 << ", "
	  << setw(3) << (int)scale2 << ", "
	  << setw(10) << (int)value2 << ")";
}

int GRIB2D::compare(const Level& o) const
{
	int res = Level::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const GRIB2D* v = dynamic_cast<const GRIB2D*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB2D Level, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int GRIB2D::compare(const GRIB2D& o) const
{
	// FIXME: here we can handle uniforming the scales if needed
	if (int res = type1 - o.type1) return res;
	if (int res = scale1 - o.scale1) return res;
	if (int res = value1 - o.value1) return res;
	if (int res = type2 - o.type2) return res;
	if (int res = scale2 - o.scale2) return res;
	return value2 - o.value2;
}

bool GRIB2D::operator==(const Type& o) const
{
	const GRIB2D* v = dynamic_cast<const GRIB2D*>(&o);
	if (!v) return false;
	// FIXME: here we can handle uniforming the scales if needed
	return type1 == v->type1 && scale1 == v->scale1 && value1 == v->value1
	    && type2 == v->type2 && scale2 == v->scale2 && value2 == v->value2;
}

Item<GRIB2D> GRIB2D::create(
	unsigned char type1, unsigned char scale1, unsigned long value1,
	unsigned char type2, unsigned char scale2, unsigned long value2)
{
	GRIB2D* res = new GRIB2D;
	res->type1 = type1;
	res->scale1 = scale1;
	res->value1 = value1;
	res->type2 = type2;
	res->scale2 = scale2;
	res->value2 = value2;
	return res;
}

}

static MetadataType levelType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Level::decode),
	(MetadataType::string_decoder)(&Level::decodeString));

}
}
// vim:set ts=4 sw=4:
