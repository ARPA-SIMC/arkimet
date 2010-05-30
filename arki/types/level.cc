/*
 * types/level - Vertical level or layer
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
#include <arki/types/level.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
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
			unsigned char type = dec.popUInt(1, "GRIB2S level type");
			unsigned char scale = dec.popUInt(1, "GRIB2S level scale");
			unsigned long value = dec.popVarint<unsigned long>("GRIB2S level value");
			return level::GRIB2S::create(type, scale, value);
		}
		case GRIB2D: {
			unsigned char type1 = dec.popUInt(1, "GRIB2D type1");
			unsigned char scale1 = dec.popUInt(1, "GRIB2D scale2");
			unsigned long value1 = dec.popVarint<unsigned long>("GRIB2D value1");
			unsigned char type2 = dec.popUInt(1, "GRIB2D type2");
			unsigned char scale2 = dec.popUInt(1, "GRIB2D scale2");
			unsigned long value2 = dec.popVarint<unsigned long>("GRIB2D value2");
			return level::GRIB2D::create(type1, scale1, value1, type2, scale2, value2);
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

namespace level {

static TypeCache<GRIB1> cache_grib1;
static TypeCache<GRIB2S> cache_grib2s;
static TypeCache<GRIB2D> cache_grib2d;

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
	o << setw(3) << (int)m_type;
	switch (valType())
	{
		case 0: break;
		case 1:
			 o << ", " << setw(5) << (int)m_l1;
			 break;
		default:
			 o << ", " << setw(3) << (int)m_l1 << ", " << setw(3) << (int)m_l2;
			 break;
	}
	o << setfill(' ');
	return o << ")";
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
    return o
	  << formatStyle(style()) << "("
	  << setfill('0') << internal
	  << setw(3) << (int)m_type << ", "
	  << setw(3) << (int)m_scale << ", "
	  << setw(10) << (int)m_value << ")";
}
std::string GRIB2S::exactQuery() const
{
	return str::fmtf("GRIB2S,%d,%d,%d", (int)m_type, (int)m_scale, (int)m_value);
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
		lua_pushnumber(L, type());
	else if (name == "scale")
		lua_pushnumber(L, scale());
	else if (name == "value")
		lua_pushnumber(L, value());
	else
		return Level::lua_lookup(L, name);
	return true;
}

Item<GRIB2S> GRIB2S::create(unsigned char type, unsigned char scale, unsigned long value)
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
	  << setw(3) << (int)m_type1 << ", "
	  << setw(3) << (int)m_scale1 << ", "
	  << setw(10) << (int)m_value1 << ", "
	  << setw(3) << (int)m_type2 << ", "
	  << setw(3) << (int)m_scale2 << ", "
	  << setw(10) << (int)m_value2 << ")";
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
	unsigned char type1, unsigned char scale1, unsigned long value1,
	unsigned char type2, unsigned char scale2, unsigned long value2)
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

static void debug_interns()
{
	fprintf(stderr, "level GRIB1: sz %zd reused %zd\n", cache_grib1.size(), cache_grib1.reused());
	fprintf(stderr, "level GRIB2S: sz %zd reused %zd\n", cache_grib2s.size(), cache_grib2s.reused());
	fprintf(stderr, "level GRIB2D: sz %zd reused %zd\n", cache_grib2d.size(), cache_grib2d.reused());
}

}

static MetadataType levelType = MetadataType::create<Level>(level::debug_interns);

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
