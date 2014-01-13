/*
 * types/level - Vertical level or layer
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/utils/codec.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include "config.h"
#include <sstream>
#include <iomanip>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_LEVEL
#define TAG "level"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;
using namespace wibble;

namespace arki {
namespace types {

const char* traits<Level>::type_tag = TAG;
const types::Code traits<Level>::type_code = CODE;
const size_t traits<Level>::type_sersize_bytes = SERSIZELEN;
const char* traits<Level>::type_lua_tag = LUATAG_TYPES ".level";

// Style constants
const unsigned char Level::GRIB1;
const unsigned char Level::GRIB2S;
const unsigned char Level::GRIB2D;
const unsigned char Level::ODIMH5;

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
	if (str == "ODIMH5") return ODIMH5;
	throw wibble::exception::Consistency("parsing Level style", "cannot parse Level style '"+str+"': only GRIB1, GRIB2S, GRIB2D, ODIMH5 are supported");
}

std::string Level::formatStyle(Level::Style s)
{
	switch (s)
	{
		case Level::GRIB1: return "GRIB1";
		case Level::GRIB2S: return "GRIB2S";
		case Level::GRIB2D: return "GRIB2D";
		case Level::ODIMH5: return "ODIMH5";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

Item<Level> Level::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	Decoder dec(buf, len);
	Style s = (Style)dec.popUInt(1, "level style");
	switch (s)
	{
		case GRIB1: {
			unsigned char ltype = dec.popUInt(1, "level type");
			switch (level::GRIB1::getValType(ltype))
			{
				case 0:
					return level::GRIB1::create(ltype);
				case 1: {
					unsigned short l1 = dec.popVarint<unsigned short>("GRIB1 level l1");
					return level::GRIB1::create(ltype, l1);
				}
				default: {
					unsigned char l1 = dec.popUInt(1, "GRIB1 layer l1");
					unsigned char l2 = dec.popUInt(1, "GRIB1 layer l2");
					return level::GRIB1::create(ltype, l1, l2);
				}
			}
		}
		case GRIB2S: {
			uint8_t type = dec.popUInt(1, "GRIB2S level type");
			uint8_t scale = dec.popUInt(1, "GRIB2S level scale");
			uint32_t value = dec.popVarint<uint32_t>("GRIB2S level value");
			return level::GRIB2S::create(type, scale, value);
		}
		case GRIB2D: {
			unsigned char type1 = dec.popUInt(1, "GRIB2D type1");
			unsigned char scale1 = dec.popUInt(1, "GRIB2D scale2");
			unsigned int value1 = dec.popVarint<unsigned int>("GRIB2D value1");
			unsigned char type2 = dec.popUInt(1, "GRIB2D type2");
			unsigned char scale2 = dec.popUInt(1, "GRIB2D scale2");
			unsigned int value2 = dec.popVarint<unsigned int>("GRIB2D value2");
			return level::GRIB2D::create(type1, scale1, value1, type2, scale2, value2);
		}
		case ODIMH5: {
			double min = dec.popDouble("ODIMH5 min");
			double max = dec.popDouble("ODIMH5 max");
			return level::ODIMH5::create(min, max);
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

template<typename T>
static T getUnsigned(const char * & start, const char* what, T missing=(T)-1)
{
    char* endptr;

    if (!*start)
        throw wibble::exception::Consistency("parsing Level", string("no ") + what + " found");

    // Skip spaces, if any
    while (*start && (::isspace(*start)))
        ++start;

    T res;
    if (*start == '-')
    {
        res = missing;
        ++start;
    }
    else
    {
        res = (T)strtoul(start, &endptr, 10);
        if (endptr == start)
            throw wibble::exception::Consistency("parsing Level",
                    string("expected ") + what + ", but found \"" + start + "\"");
        start = endptr;
    }

    // Skip colons and spaces, if any
    while (*start && (::isspace(*start) || *start == ','))
        ++start;

    return res;
}

static double getDouble(const char * & start, const char* what)
{
	char* endptr;

	if (!*start)
		throw wibble::exception::Consistency("parsing Level", string("no ") + what + " after level type");

	double res = strtold(start, &endptr);
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

            uint8_t type = getUnsigned<uint8_t>(start, "level type", level::GRIB2S::MISSING_TYPE);
            uint8_t scale = getUnsigned<uint8_t>(start, "scale of level value", level::GRIB2S::MISSING_SCALE);
            uint32_t value = getUnsigned<uint32_t>(start, "level value", level::GRIB2S::MISSING_VALUE);
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
		case Level::ODIMH5: {
			const char* start = inner.c_str();

			double min = getDouble(start, "ODIMH5 min level");
			double max = getDouble(start, "ODIMH5 max level");
			return level::ODIMH5::create(min, max);
		}
		default:
			throw wibble::exception::Consistency("parsing Level", "unknown Level style " + formatStyle(style));
	}
}

Item<Level> Level::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case Level::GRIB1: return level::GRIB1::decodeMapping(val);
        case Level::GRIB2S: return level::GRIB2S::decodeMapping(val);
        case Level::GRIB2D: return level::GRIB2D::decodeMapping(val);
        case Level::ODIMH5: return level::ODIMH5::decodeMapping(val);
        default:
            throw wibble::exception::Consistency("parsing Level", "unknown Level style " + val.get_string());
    }
}

static int arkilua_new_grib1(lua_State* L)
{
	int type = luaL_checkint(L, 1);
	switch (level::GRIB1::getValType(type))
	{
		case 0: level::GRIB1::create(type)->lua_push(L); break;
		case 1: level::GRIB1::create(type, luaL_checkint(L, 2))->lua_push(L); break;
		case 2: level::GRIB1::create(type, luaL_checkint(L, 2), luaL_checkint(L, 3))->lua_push(L); break;
		default: lua_pushnil(L);
	}
	return 1;
}

static int arkilua_new_grib2s(lua_State* L)
{
    uint8_t type;
    uint8_t scale;
    uint32_t val;

    if (lua_isnil(L, 1))
        type = level::GRIB2S::MISSING_TYPE;
    else
        type = luaL_checkint(L, 1);

    if (lua_isnil(L, 2))
        scale = level::GRIB2S::MISSING_SCALE;
    else
        scale = luaL_checkint(L, 2);

    if (lua_isnil(L, 3))
        val = level::GRIB2S::MISSING_VALUE;
    else
        val = luaL_checkint(L, 3);

    Item<> res = level::GRIB2S::create(type, scale, val);
    res->lua_push(L);
    return 1;
}

static int arkilua_new_grib2d(lua_State* L)
{
	int type1 = luaL_checkint(L, 1);
	int scale1 = luaL_checkint(L, 2);
	int val1 = luaL_checkint(L, 3);
	int type2 = luaL_checkint(L, 4);
	int scale2 = luaL_checkint(L, 5);
	int val2 = luaL_checkint(L, 6);
	Item<> res = level::GRIB2D::create(type1, scale1, val1, type2, scale2, val2);
	res->lua_push(L);
	return 1;
}

static int arkilua_new_odimh5(lua_State* L)
{
	double value = luaL_checknumber(L, 1);
	if (lua_gettop(L) > 1)
	{
		double max = luaL_checknumber(L, 2);
		level::ODIMH5::create(value, max)->lua_push(L);
	} else
		level::ODIMH5::create(value)->lua_push(L);
	return 1;
}


void Level::lua_loadlib(lua_State* L)
{
	static const struct luaL_Reg lib [] = {
		{ "grib1", arkilua_new_grib1 },
		{ "grib2s", arkilua_new_grib2s },
		{ "grib2d", arkilua_new_grib2d },
		{ "odimh5", arkilua_new_odimh5 },
		{ NULL, NULL }
	};
	luaL_openlib(L, "arki_level", lib, 0);
	lua_pop(L, 1);
}

namespace level {

static TypeCache<GRIB1> cache_grib1;
static TypeCache<GRIB2S> cache_grib2s;
static TypeCache<GRIB2D> cache_grib2d;
static TypeCache<ODIMH5> cache_odimh5;

Level::Style GRIB1::style() const { return Level::GRIB1; }

void GRIB1::encodeWithoutEnvelope(Encoder& enc) const
{
	Level::encodeWithoutEnvelope(enc) ;
	enc.addUInt(m_type, 1);
	switch (valType())
	{
		case 0: break;
		case 1: enc.addVarint(m_l1); break;
		default:
			enc.addUInt(m_l1, 1);
			enc.addUInt(m_l2, 1);
			break;
	}
}
std::ostream& GRIB1::writeToOstream(std::ostream& o) const
{
	o << formatStyle(style()) << "(";
	o << setfill('0') << internal;
	o << setw(3) << (unsigned)m_type;
	switch (valType())
	{
		case 0: break;
		case 1:
			 o << ", " << setw(5) << (unsigned)m_l1;
			 break;
		default:
			 o << ", " << setw(3) << (unsigned)m_l1 << ", " << setw(3) << (unsigned)m_l2;
			 break;
	}
	o << setfill(' ');
	return o << ")";
}
void GRIB1::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Level::serialiseLocal(e, f);
    e.add("lt", (unsigned)m_type);
    switch (valType())
    {
        case 0: break;
        case 1:
            e.add("l1", (unsigned)m_l1);
            break;
        case 2:
            e.add("l1", (unsigned)m_l1);
            e.add("l2", (unsigned)m_l2);
            break;
    }
}
Item<GRIB1> GRIB1::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;
    int lt = val["lt"].want_int("parsing GRIB1 level type");
    const Node& l1 = val["l1"];
    const Node& l2 = val["l2"];
    if (!l2.is_null())
        return GRIB1::create(lt,
                l1.want_int("parsing GRIB1 level l1"),
                l2.want_int("parsing GRIB1 level l2"));
    if (!l1.is_null())
        return GRIB1::create(lt,
                l1.want_int("parsing GRIB1 level l1"));
    return GRIB1::create(lt);
}
std::string GRIB1::exactQuery() const
{
	switch (valType())
	{
		case 0: return str::fmtf("GRIB1,%d", (int)m_type);
		case 1: return str::fmtf("GRIB1,%d,%d", (int)m_type, (int)m_l1);
		default: return str::fmtf("GRIB1,%d,%d,%d", (int)m_type, (int)m_l1, (int)m_l2);
	}
}
const char* GRIB1::lua_type_name() const { return "arki.types.level.grib1"; }

int GRIB1::compare_local(const Level& o) const
{
	// We should be the same kind, so upcast
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Level, but is a ") + typeid(&o).name() + " instead");

	if (int res = m_type - v->m_type) return res;
	if (int res = m_l1 - v->m_l1) return res;
	return m_l2 - v->m_l2;
}

bool GRIB1::operator==(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;
	return m_type == v->m_type && m_l1 == v->m_l1 && m_l2 == v->m_l2;
}

Item<GRIB1> GRIB1::create(unsigned char type)
{
	GRIB1* res = new GRIB1;
	res->m_type = type;
	res->m_l1 = 0;
	res->m_l2 = 0;
	return cache_grib1.intern(res);
}

Item<GRIB1> GRIB1::create(unsigned char type, unsigned short l1)
{
	GRIB1* res = new GRIB1;
	res->m_type = type;
	switch (getValType(type))
	{
		case 0: res->m_l1 = 0; break;
		default: res->m_l1 = l1; break;
	}
	res->m_l2 = 0;
	return cache_grib1.intern(res);
}

Item<GRIB1> GRIB1::create(unsigned char type, unsigned char l1, unsigned char l2)
{
	GRIB1* res = new GRIB1;
	res->m_type = type;
	switch (getValType(type))
	{
		case 0: res->m_l1 = 0; res->m_l2 = 0; break;
		case 1: res->m_l1 = l1; res->m_l2 = 0; break;
		default: res->m_l1 = l1; res->m_l2 = l2; break;
	}
	return cache_grib1.intern(res);
}

int GRIB1::valType() const
{
	return getValType(m_type);
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

bool GRIB1::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "type")
		lua_pushnumber(L, type());
	else if (name == "l1")
		lua_pushnumber(L, l1());
	else if (name == "l2")
		lua_pushnumber(L, l2());
	else
		return Level::lua_lookup(L, name);
	return true;
}

const uint8_t GRIB2S::MISSING_TYPE = 0xff;
const uint8_t GRIB2S::MISSING_SCALE = 0xff;
const uint32_t GRIB2S::MISSING_VALUE = 0xFFFFFFFF;

Level::Style GRIB2S::style() const { return Level::GRIB2S; }

void GRIB2S::encodeWithoutEnvelope(Encoder& enc) const
{
	Level::encodeWithoutEnvelope(enc);
	enc.addUInt(m_type, 1);
	enc.addUInt(m_scale, 1);
	enc.addVarint(m_value);
}
std::ostream& GRIB2S::writeToOstream(std::ostream& o) const
{
    utils::SaveIOState sios(o);
    o << formatStyle(style()) << "(";

    if (m_type == MISSING_TYPE)
        o << setfill(' ') << internal << setw(3) << "-" << ", ";
    else
        o << setfill('0') << internal << setw(3) << (unsigned)m_type << ", ";

    if (m_scale == MISSING_SCALE)
        o << setfill(' ') << internal << setw(3) << "-" << ", ";
    else
        o << setfill('0') << internal << setw(3) << (unsigned)m_scale << ", ";

    if (m_value == MISSING_VALUE)
        o << setfill(' ') << internal << setw(10) << "-" << ")";
    else
        o << setfill('0') << internal << setw(10) << m_value << ")";

    return o;
}
void GRIB2S::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Level::serialiseLocal(e, f);

    if (m_type == MISSING_TYPE)
    {
        e.add("lt"); e.add_null();
    }
    else
        e.add("lt", (unsigned)m_type);

    if (m_scale == MISSING_SCALE)
    {
        e.add("sc"); e.add_null();
    } else
        e.add("sc", (unsigned)m_scale);

    if (m_value == MISSING_VALUE)
    {
        e.add("va"); e.add_null();
    } else
        e.add("va", m_value);
}
Item<GRIB2S> GRIB2S::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;
    const Node& lt = val["lt"];
    const Node& sc = val["sc"];
    const Node& va = val["va"];
    return GRIB2S::create(
            lt.is_null() ? MISSING_TYPE : lt.want_int("parsing GRIB2S level type"),
            sc.is_null() ? MISSING_SCALE : sc.want_int("parsing GRIB2S level scale"),
            va.is_null() ? MISSING_VALUE : va.want_int("parsing GRIB2S level value"));
}
std::string GRIB2S::exactQuery() const
{
    stringstream res;
    res << "GRIB2S,";
    if (m_type == MISSING_TYPE) res << "-,"; else res << (unsigned)m_type << ",";
    if (m_scale == MISSING_SCALE) res << "-,"; else res << (unsigned)m_scale << ",";
    if (m_value == MISSING_VALUE) res << "-"; else res << m_value;
    return res.str();
}
const char* GRIB2S::lua_type_name() const { return "arki.types.level.grib2s"; }

int GRIB2S::compare_local(const Level& o) const
{
	// We should be the same kind, so upcast
	const GRIB2S* v = dynamic_cast<const GRIB2S*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB2S Level, but is a ") + typeid(&o).name() + " instead");

	// FIXME: here we can handle uniforming the scales if needed
	if (int res = m_type - v->m_type) return res;
	if (int res = m_scale - v->m_scale) return res;
	return m_value - v->m_value;
}

bool GRIB2S::operator==(const Type& o) const
{
	const GRIB2S* v = dynamic_cast<const GRIB2S*>(&o);
	if (!v) return false;
	// FIXME: here we can handle uniforming the scales if needed
	return m_type == v->m_type && m_scale == v->m_scale && m_value == v->m_value;
}

bool GRIB2S::lua_lookup(lua_State* L, const std::string& name) const
{
    if (name == "type")
        if (m_type == MISSING_TYPE)
            lua_pushnil(L);
        else
            lua_pushnumber(L, m_type);
    else if (name == "scale")
        if (m_scale == MISSING_SCALE)
            lua_pushnil(L);
        else
            lua_pushnumber(L, m_scale);
    else if (name == "value")
        if (m_value == MISSING_VALUE)
            lua_pushnil(L);
        else
            lua_pushnumber(L, m_value);
    else
        return Level::lua_lookup(L, name);
    return true;
}

Item<GRIB2S> GRIB2S::create(unsigned char type, unsigned char scale, unsigned int value)
{
	GRIB2S* res = new GRIB2S;
	res->m_type = type;
	res->m_scale = scale;
	res->m_value = value;
	return cache_grib2s.intern(res);
}


Level::Style GRIB2D::style() const { return Level::GRIB2D; }

void GRIB2D::encodeWithoutEnvelope(Encoder& enc) const
{
	Level::encodeWithoutEnvelope(enc);
	enc.addUInt(m_type1, 1).addUInt(m_scale1, 1).addVarint(m_value1);
	enc.addUInt(m_type2, 1).addUInt(m_scale2, 1).addVarint(m_value2);
}
std::ostream& GRIB2D::writeToOstream(std::ostream& o) const
{
    utils::SaveIOState sios(o);
    return o
        << formatStyle(style()) << "("
        << setfill('0') << internal
        << setw(3) << (unsigned)m_type1 << ", "
        << setw(3) << (unsigned)m_scale1 << ", "
        << setw(10) << m_value1 << ", "
        << setw(3) << (unsigned)m_type2 << ", "
        << setw(3) << (unsigned)m_scale2 << ", "
        << setw(10) << m_value2 << ")";
}
void GRIB2D::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Level::serialiseLocal(e, f);
    e.add("l1", (unsigned)m_type1);
    e.add("s1", (unsigned)m_scale1);
    e.add("v1", m_value1);
    e.add("l2", (unsigned)m_type2);
    e.add("s2", (unsigned)m_scale2);
    e.add("v2", m_value2);
}
Item<GRIB2D> GRIB2D::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;
    return GRIB2D::create(
            val["l1"].want_int("parsing GRIB2D level type1"),
            val["s1"].want_int("parsing GRIB2D level scale1"),
            val["v1"].want_int("parsing GRIB2D level value1"),
            val["l2"].want_int("parsing GRIB2D level type2"),
            val["s2"].want_int("parsing GRIB2D level scale2"),
            val["v2"].want_int("parsing GRIB2D level value2"));
}
std::string GRIB2D::exactQuery() const
{
	return str::fmtf("GRIB2D,%d,%d,%d,%d,%d,%d",
			(int)m_type1, (int)m_scale1, (int)m_value1,
			(int)m_type2, (int)m_scale2, (int)m_value2);
}
const char* GRIB2D::lua_type_name() const { return "arki.types.level.grib2d"; }

int GRIB2D::compare_local(const Level& o) const
{
	// We should be the same kind, so upcast
	const GRIB2D* v = dynamic_cast<const GRIB2D*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB2D Level, but is a ") + typeid(&o).name() + " instead");

	// FIXME: here we can handle uniforming the scales if needed
	if (int res = m_type1 - v->m_type1) return res;
	if (int res = m_scale1 - v->m_scale1) return res;
	if (int res = m_value1 - v->m_value1) return res;
	if (int res = m_type2 - v->m_type2) return res;
	if (int res = m_scale2 - v->m_scale2) return res;
	return m_value2 - v->m_value2;
}

bool GRIB2D::operator==(const Type& o) const
{
	const GRIB2D* v = dynamic_cast<const GRIB2D*>(&o);
	if (!v) return false;
	// FIXME: here we can handle uniforming the scales if needed
	return m_type1 == v->m_type1 && m_scale1 == v->m_scale1 && m_value1 == v->m_value1
	    && m_type2 == v->m_type2 && m_scale2 == v->m_scale2 && m_value2 == v->m_value2;
}

#ifdef HAVE_LUA
bool GRIB2D::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "type1")
		lua_pushnumber(L, type1());
	else if (name == "scale1")
		lua_pushnumber(L, scale1());
	else if (name == "value1")
		lua_pushnumber(L, value1());
	else if (name == "type2")
		lua_pushnumber(L, type2());
	else if (name == "scale2")
		lua_pushnumber(L, scale2());
	else if (name == "value2")
		lua_pushnumber(L, value2());
	else
		return Level::lua_lookup(L, name);
	return true;
}
#endif

Item<GRIB2D> GRIB2D::create(
	unsigned char type1, unsigned char scale1, unsigned int value1,
	unsigned char type2, unsigned char scale2, unsigned int value2)
{
	GRIB2D* res = new GRIB2D;
	res->m_type1 = type1;
	res->m_scale1 = scale1;
	res->m_value1 = value1;
	res->m_type2 = type2;
	res->m_scale2 = scale2;
	res->m_value2 = value2;
	return cache_grib2d.intern(res);
}

Level::Style ODIMH5::style() const { return Level::ODIMH5; }

void ODIMH5::encodeWithoutEnvelope(Encoder& enc) const
{
	Level::encodeWithoutEnvelope(enc);
	enc.addDouble(m_min);
	enc.addDouble(m_max);
}
std::ostream& ODIMH5::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sios(o);
    return o
		<< formatStyle(style()) << "("
		<< std::setprecision(5) << m_min
		<< ", "
		<< std::setprecision(5) << m_max
		<< ")"
		;
}
void ODIMH5::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Level::serialiseLocal(e, f);
    e.add("mi", m_min);
    e.add("ma", m_max);
}
Item<ODIMH5> ODIMH5::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;
    return ODIMH5::create(
            val["mi"].want_double("parsing ODIMH5 level min"),
            val["ma"].want_double("parsing ODIMH5 level max"));
}
std::string ODIMH5::exactQuery() const
{
	std::ostringstream ss;
	ss  << "ODIMH5,range " 
		<< std::setprecision(5) << m_min
		<< " "
		<< std::setprecision(5) << m_max
		;
	return ss.str();
	//return str::fmtf("ODIMH5,%lf,%lf", m_min, m_max);
}
const char* ODIMH5::lua_type_name() const { return "arki.types.level.odimh5"; }

int ODIMH5::compare_local(const Level& o) const
{
	// We should be the same kind, so upcast
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a ODIMH5 Level, but is a ") + typeid(&o).name() + " instead");

    if (int res = m_min - v->m_min) return res;
    return m_max - v->m_max;
    // return (m_max - m_min) - (v->m_max - v->m_min);
}

bool ODIMH5::operator==(const Type& o) const
{
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v) return false;
	// FIXME: here we can handle uniforming the scales if needed
	return m_max == v->m_max && m_min == v->m_min;
}

#ifdef HAVE_LUA
bool ODIMH5::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "max")
		lua_pushnumber(L, max());
	else if (name == "min")
		lua_pushnumber(L, min());
	else
		return Level::lua_lookup(L, name);
	return true;
}
#endif

Item<ODIMH5> ODIMH5::create(double val)
{
	return ODIMH5::create(val, val);
}

Item<ODIMH5> ODIMH5::create(double min, double max)
{
	ODIMH5* res = new ODIMH5;
	res->m_max = max;
	res->m_min = min;
	return cache_odimh5.intern(res);
}


static void debug_interns()
{
	fprintf(stderr, "level GRIB1: sz %zd reused %zd\n", cache_grib1.size(), cache_grib1.reused());
	fprintf(stderr, "level GRIB2S: sz %zd reused %zd\n", cache_grib2s.size(), cache_grib2s.reused());
	fprintf(stderr, "level GRIB2D: sz %zd reused %zd\n", cache_grib2d.size(), cache_grib2d.reused());
	fprintf(stderr, "level ODIMH5: sz %zd reused %zd\n", cache_odimh5.size(), cache_odimh5.reused());
}

}

void Level::init()
{
    MetadataType::register_type<Level>(level::debug_interns);
}

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
