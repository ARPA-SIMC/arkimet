#include "arki/exceptions.h"
#include "arki/binary.h"
#include "arki/utils/iostream.h"
#include "arki/types/level.h"
#include "arki/types/utils.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <sstream>
#include <iomanip>
#include <cmath>

#ifdef HAVE_GRIBAPI
#include <grib_api.h>
#endif

#ifdef HAVE_LUA
#include "arki/utils/lua.h"
#endif

#define CODE TYPE_LEVEL
#define TAG "level"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Level>::type_tag = TAG;
const types::Code traits<Level>::type_code = CODE;
const size_t traits<Level>::type_sersize_bytes = SERSIZELEN;
const char* traits<Level>::type_lua_tag = LUATAG_TYPES ".level";

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
    if (str == "GRIB1") return Style::GRIB1;
    if (str == "GRIB2S") return Style::GRIB2S;
    if (str == "GRIB2D") return Style::GRIB2D;
    if (str == "ODIMH5") return Style::ODIMH5;
    throw_consistency_error("parsing Level style", "cannot parse Level style '"+str+"': only GRIB1, GRIB2S, GRIB2D, ODIMH5 are supported");
}

std::string Level::formatStyle(Level::Style s)
{
    switch (s)
    {
        case Style::GRIB1: return "GRIB1";
        case Style::GRIB2S: return "GRIB2S";
        case Style::GRIB2D: return "GRIB2D";
        case Style::ODIMH5: return "ODIMH5";
        default:
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

unique_ptr<Level> Level::decode(BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "level style");
    switch (s)
    {
        case Style::GRIB1: {
            unsigned char ltype = dec.pop_uint(1, "level type");
            switch (level::GRIB1::getValType(ltype))
            {
                case 0:
                    return createGRIB1(ltype);
                case 1: {
                    unsigned short l1 = dec.pop_varint<unsigned short>("GRIB1 level l1");
                    return createGRIB1(ltype, l1);
                }
                default: {
                    unsigned char l1 = dec.pop_uint(1, "GRIB1 layer l1");
                    unsigned char l2 = dec.pop_uint(1, "GRIB1 layer l2");
                    return createGRIB1(ltype, l1, l2);
                }
            }
        }
        case Style::GRIB2S: {
            uint8_t type = dec.pop_uint(1, "GRIB2S level type");
            uint8_t scale = dec.pop_uint(1, "GRIB2S level scale");
            uint32_t value = dec.pop_varint<uint32_t>("GRIB2S level value");
            return createGRIB2S(type, scale, value);
        }
        case Style::GRIB2D: {
            uint8_t type1 = dec.pop_uint(1, "GRIB2D level type1");
            uint8_t scale1 = dec.pop_uint(1, "GRIB2D level scale1");
            uint32_t value1 = dec.pop_varint<uint32_t>("GRIB2D level value1");
            uint8_t type2 = dec.pop_uint(1, "GRIB2D level type2");
            uint8_t scale2 = dec.pop_uint(1, "GRIB2D level scale2");
            uint32_t value2 = dec.pop_varint<uint32_t>("GRIB2D level value2");
            return createGRIB2D(type1, scale1, value1, type2, scale2, value2);
        }
        case Style::ODIMH5: {
            double min = dec.pop_double("ODIMH5 min");
            double max = dec.pop_double("ODIMH5 max");
            return createODIMH5(min, max);
        }
		default:
			throw_consistency_error("parsing Level", "style is " + formatStyle(s) + " but we can only decode GRIB1, GRIB2S and GRIB2D");
	}
}

static int getNumber(const char * & start, const char* what)
{
	char* endptr;

	if (!*start)
		throw_consistency_error("parsing Level", string("no ") + what + " after level type");

	int res = strtol(start, &endptr, 10);
	if (endptr == start)
		throw_consistency_error("parsing Level",
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
        throw_consistency_error("parsing Level", string("no ") + what + " found");

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
            throw_consistency_error("parsing Level",
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
		throw_consistency_error("parsing Level", string("no ") + what + " after level type");

	double res = strtold(start, &endptr);
	if (endptr == start)
		throw_consistency_error("parsing Level",
				string("expected ") + what + ", but found \"" + start + "\"");
	start = endptr;

	// Skip colons and spaces, if any
	while (*start && (::isspace(*start) || *start == ','))
		++start;

	return res;
}

unique_ptr<Level> Level::decodeString(const std::string& val)
{
	string inner;
	Level::Style style = outerParse<Level>(val, inner);
	switch (style)
	{
        case Style::GRIB1: {
            const char* start = inner.c_str();

            int type = getNumber(start, "level type");
            switch (level::GRIB1::getValType((unsigned char)type))
            {
                case 0: return createGRIB1(type);
                case 1: {
                    int l1 = getNumber(start, "level value");
                    return createGRIB1(type, l1);
                }
                default: {
                    int l1 = getNumber(start, "first level value");
                    int l2 = getNumber(start, "second level value");
                    return createGRIB1(type, l1, l2);
                }
            }
        }
        case Style::GRIB2S: {
            const char* start = inner.c_str();

            uint8_t type = getUnsigned<uint8_t>(start, "level type", level::GRIB2S::MISSING_TYPE);
            uint8_t scale = getUnsigned<uint8_t>(start, "scale of level value", level::GRIB2S::MISSING_SCALE);
            uint32_t value = getUnsigned<uint32_t>(start, "level value", level::GRIB2S::MISSING_VALUE);
            return createGRIB2S(type, scale, value);
        }
        case Style::GRIB2D: {
            const char* start = inner.c_str();

            uint8_t type1 = getUnsigned<uint8_t>(start, "type of first level", level::GRIB2S::MISSING_TYPE);
            uint32_t scale1 = getUnsigned<uint32_t>(start, "scale of value of first level", level::GRIB2S::MISSING_SCALE);
            uint32_t value1 = getUnsigned<uint32_t>(start, "value of first level", level::GRIB2S::MISSING_VALUE);
            uint8_t type2 = getUnsigned<uint8_t>(start, "type of second level", level::GRIB2S::MISSING_TYPE);
            uint32_t scale2 = getUnsigned<uint32_t>(start, "scale of value of second level", level::GRIB2S::MISSING_SCALE);
            uint32_t value2 = getUnsigned<uint32_t>(start, "value of second level", level::GRIB2S::MISSING_VALUE);
            return createGRIB2D(type1, scale1, value1, type2, scale2, value2);
        }
        case Style::ODIMH5: {
            const char* start = inner.c_str();

            double min = getDouble(start, "ODIMH5 min level");
            double max = getDouble(start, "ODIMH5 max level");
            return createODIMH5(min, max);
        }
		default:
			throw_consistency_error("parsing Level", "unknown Level style " + formatStyle(style));
	}
}

std::unique_ptr<Level> Level::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    switch (style_from_structure(keys, val))
    {
        case Style::GRIB1: return upcast<Level>(level::GRIB1::decode_structure(keys, val));
        case Style::GRIB2S: return upcast<Level>(level::GRIB2S::decode_structure(keys, val));
        case Style::GRIB2D: return upcast<Level>(level::GRIB2D::decode_structure(keys, val));
        case Style::ODIMH5: return upcast<Level>(level::ODIMH5::decode_structure(keys, val));
        default: throw std::runtime_error("Unknown Level style");
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

    level::GRIB2S::create(type, scale, val)->lua_push(L);
    return 1;
}

static int arkilua_new_grib2d(lua_State* L)
{
    uint8_t type1;
    uint8_t scale1;
    uint32_t val1;
    uint8_t type2;
    uint8_t scale2;
    uint32_t val2;

    if (lua_isnil(L, 1))
        type1 = level::GRIB2S::MISSING_TYPE;
    else
        type1 = luaL_checkint(L, 1);

    if (lua_isnil(L, 2))
        scale1 = level::GRIB2S::MISSING_SCALE;
    else
        scale1 = luaL_checkint(L, 2);

    if (lua_isnil(L, 3))
        val1 = level::GRIB2S::MISSING_VALUE;
    else
        val1 = luaL_checkint(L, 3);

    if (lua_isnil(L, 1))
        type2 = level::GRIB2S::MISSING_TYPE;
    else
        type2 = luaL_checkint(L, 4);

    if (lua_isnil(L, 2))
        scale2 = level::GRIB2S::MISSING_SCALE;
    else
        scale2 = luaL_checkint(L, 5);

    if (lua_isnil(L, 3))
        val2 = level::GRIB2S::MISSING_VALUE;
    else
        val2 = luaL_checkint(L, 6);

    level::GRIB2D::create(type1, scale1, val1, type2, scale2, val2)->lua_push(L);
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
    utils::lua::add_global_library(L, "arki_level", lib);
}

unique_ptr<Level> Level::createGRIB1(unsigned char type)
{
    return upcast<Level>(level::GRIB1::create(type));
}
unique_ptr<Level> Level::createGRIB1(unsigned char type, unsigned short l1)
{
    return upcast<Level>(level::GRIB1::create(type, l1));
}
unique_ptr<Level> Level::createGRIB1(unsigned char type, unsigned char l1, unsigned char l2)
{
    return upcast<Level>(level::GRIB1::create(type, l1, l2));
}
unique_ptr<Level> Level::createGRIB2S(uint8_t type, uint8_t scale, uint32_t val)
{
    return upcast<Level>(level::GRIB2S::create(type, scale, val));
}
unique_ptr<Level> Level::createGRIB2D(uint8_t type1, uint8_t scale1, uint32_t val1,
                                    uint8_t type2, uint8_t scale2, uint32_t val2)
{
    return upcast<Level>(level::GRIB2D::create(type1, scale1, val1, type2, scale2, val2));
}
unique_ptr<Level> Level::createODIMH5(double value)
{
    return upcast<Level>(level::ODIMH5::create(value));
}
unique_ptr<Level> Level::createODIMH5(double min, double max)
{
    return upcast<Level>(level::ODIMH5::create(min, max));
}

namespace level {

Level::Style GRIB1::style() const { return Style::GRIB1; }

void GRIB1::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Level::encodeWithoutEnvelope(enc) ;
    enc.add_unsigned(m_type, 1);
    switch (valType())
    {
        case 0: break;
        case 1: enc.add_varint(m_l1); break;
        default:
            enc.add_unsigned(m_l1, 1);
            enc.add_unsigned(m_l2, 1);
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
void GRIB1::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Level::serialise_local(e, keys, f);
    e.add(keys.level_type, (unsigned)m_type);
    switch (valType())
    {
        case 0: break;
        case 1:
            e.add(keys.level_l1, (unsigned)m_l1);
            break;
        case 2:
            e.add(keys.level_l1, (unsigned)m_l1);
            e.add(keys.level_l2, (unsigned)m_l2);
            break;
    }
}

std::unique_ptr<GRIB1> GRIB1::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    int lt = val.as_int(keys.level_type, "level type");
    if (val.has_key(keys.level_l2, structured::NodeType::INT))
        return GRIB1::create(lt,
                val.as_int(keys.level_l1, "level l1"),
                val.as_int(keys.level_l2, "level l2"));
    if (val.has_key(keys.level_l1, structured::NodeType::INT))
        return GRIB1::create(lt,
                val.as_int(keys.level_l1, "level l1"));
    return GRIB1::create(lt);
}

std::string GRIB1::exactQuery() const
{
    char buf[128];
    switch (valType())
    {
        case 0: snprintf(buf, 128, "GRIB1,%d", (int)m_type); break;
        case 1: snprintf(buf, 128, "GRIB1,%d,%d", (int)m_type, (int)m_l1); break;
        default: snprintf(buf, 128, "GRIB1,%d,%d,%d", (int)m_type, (int)m_l1, (int)m_l2); break;
    }
    return buf;
}
const char* GRIB1::lua_type_name() const { return "arki.types.level.grib1"; }

int GRIB1::compare_local(const Level& o) const
{
    if (int res = Level::compare_local(o)) return res;
	// We should be the same kind, so upcast
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Level, but is a ") + typeid(&o).name() + " instead");

	if (int res = m_type - v->m_type) return res;
	if (int res = m_l1 - v->m_l1) return res;
	return m_l2 - v->m_l2;
}

bool GRIB1::equals(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;
	return m_type == v->m_type && m_l1 == v->m_l1 && m_l2 == v->m_l2;
}

GRIB1* GRIB1::clone() const
{
    GRIB1* res = new GRIB1;
    res->m_type = m_type;
    res->m_l1 = m_l1;
    res->m_l2 = m_l2;
    return res;
}

unique_ptr<GRIB1> GRIB1::create(unsigned char type)
{
    GRIB1* res = new GRIB1;
    res->m_type = type;
    res->m_l1 = 0;
    res->m_l2 = 0;
    return unique_ptr<GRIB1>(res);
}

unique_ptr<GRIB1> GRIB1::create(unsigned char type, unsigned short l1)
{
	GRIB1* res = new GRIB1;
	res->m_type = type;
	switch (getValType(type))
	{
		case 0: res->m_l1 = 0; break;
		default: res->m_l1 = l1; break;
	}
	res->m_l2 = 0;
    return unique_ptr<GRIB1>(res);
}

unique_ptr<GRIB1> GRIB1::create(unsigned char type, unsigned char l1, unsigned char l2)
{
	GRIB1* res = new GRIB1;
	res->m_type = type;
	switch (getValType(type))
	{
		case 0: res->m_l1 = 0; res->m_l2 = 0; break;
		case 1: res->m_l1 = l1; res->m_l2 = 0; break;
		default: res->m_l1 = l1; res->m_l2 = l2; break;
	}
    return unique_ptr<GRIB1>(res);
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
const uint32_t GRIB2S::MISSING_VALUE = 0xffffffff;

Level::Style GRIB2S::style() const { return Style::GRIB2S; }

void GRIB2S::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Level::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_type, 1);
    enc.add_unsigned(m_scale, 1);
    enc.add_varint(m_value);
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
void GRIB2S::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Level::serialise_local(e, keys, f);

    if (m_type == MISSING_TYPE)
    {
        e.add(keys.level_type); e.add_null();
    }
    else
        e.add(keys.level_type, (unsigned)m_type);

    if (m_scale == MISSING_SCALE)
    {
        e.add(keys.level_scale); e.add_null();
    } else
        e.add(keys.level_scale, (unsigned)m_scale);

    if (m_value == MISSING_VALUE)
    {
        e.add(keys.level_value); e.add_null();
    } else
        e.add(keys.level_value, m_value);
}

std::unique_ptr<GRIB2S> GRIB2S::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    int lt = MISSING_TYPE, sc = MISSING_SCALE, va = MISSING_VALUE;
    if (val.has_key(keys.level_type, structured::NodeType::INT))
        lt = val.as_int(keys.level_type, "level type");
    if (val.has_key(keys.level_scale, structured::NodeType::INT))
        sc = val.as_int(keys.level_scale, "level scale");
    if (val.has_key(keys.level_value, structured::NodeType::INT))
        va = val.as_int(keys.level_value, "level value");
    return GRIB2S::create(lt, sc, va);
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

template<typename T>
static inline int compare_with_missing(const T& a, const T& b, const T& missing)
{
    if (a == missing)
        return b == missing ? 0 : 1;
    else if (b == missing)
        return -1;
    else if (a == b)
        return 0;
    else if (a < b)
        return -1;
    else
        return 1;
}

int GRIB2S::compare_local(const Level& o) const
{
    if (int res = Level::compare_local(o)) return res;
	// We should be the same kind, so upcast
	const GRIB2S* v = dynamic_cast<const GRIB2S*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a GRIB2S Level, but is a ") + typeid(&o).name() + " instead");

	// FIXME: here we can handle uniforming the scales if needed
    if (int res = compare_with_missing(m_type, v->m_type, MISSING_TYPE)) return res;
    if (int res = compare_with_missing(m_scale, v->m_scale, MISSING_SCALE)) return res;
    return compare_with_missing(m_value, v->m_value, MISSING_VALUE);
}

bool GRIB2S::equals(const Type& o) const
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

GRIB2S* GRIB2S::clone() const
{
    GRIB2S* res = new GRIB2S;
    res->m_type = m_type;
    res->m_scale = m_scale;
    res->m_value = m_value;
    return res;
}

unique_ptr<GRIB2S> GRIB2S::create(unsigned char type, unsigned char scale, unsigned int value)
{
    GRIB2S* res = new GRIB2S;
    res->m_type = type;
    res->m_scale = scale;
#ifdef HAVE_GRIBAPI
    res->m_value = value == GRIB_MISSING_LONG ? MISSING_VALUE : value;
#else
    res->m_value = value;
#endif
    return unique_ptr<GRIB2S>(res);
}


Level::Style GRIB2D::style() const { return Style::GRIB2D; }

void GRIB2D::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Level::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_type1, 1); enc.add_unsigned(m_scale1, 1); enc.add_varint(m_value1);
    enc.add_unsigned(m_type2, 1); enc.add_unsigned(m_scale2, 1); enc.add_varint(m_value2);
}
std::ostream& GRIB2D::writeToOstream(std::ostream& o) const
{
    utils::SaveIOState sios(o);

    o << formatStyle(style()) << "(";

    if (m_type1 == GRIB2S::MISSING_TYPE)
        o << setfill(' ') << internal << setw(3) << "-" << ", ";
    else
        o << setfill('0') << internal << setw(3) << (unsigned)m_type1 << ", ";

    if (m_scale1 == GRIB2S::MISSING_SCALE)
        o << setfill(' ') << internal << setw(3) << "-" << ", ";
    else
        o << setfill('0') << internal << setw(3) << (unsigned)m_scale1 << ", ";

    if (m_value1 == GRIB2S::MISSING_VALUE)
        o << setfill(' ') << internal << setw(10) << "-" << ",";
    else
        o << setfill('0') << internal << setw(10) << m_value1 << ", ";

    if (m_type2 == GRIB2S::MISSING_TYPE)
        o << setfill(' ') << internal << setw(3) << "-" << ", ";
    else
        o << setfill('0') << internal << setw(3) << (unsigned)m_type2 << ", ";

    if (m_scale2 == GRIB2S::MISSING_SCALE)
        o << setfill(' ') << internal << setw(3) << "-" << ", ";
    else
        o << setfill('0') << internal << setw(3) << (unsigned)m_scale2 << ", ";

    if (m_value2 == GRIB2S::MISSING_VALUE)
        o << setfill(' ') << internal << setw(10) << "-" << ")";
    else
        o << setfill('0') << internal << setw(10) << m_value2 << ")";

    return o;
}
void GRIB2D::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Level::serialise_local(e, keys, f);

    if (m_type1 == GRIB2S::MISSING_TYPE)
    {
        e.add(keys.level_l1); e.add_null();
    }
    else
        e.add(keys.level_l1, (unsigned)m_type1);

    if (m_scale1 == GRIB2S::MISSING_SCALE)
    {
        e.add(keys.level_scale1); e.add_null();
    } else
        e.add(keys.level_scale1, (unsigned)m_scale1);

    if (m_value1 == GRIB2S::MISSING_VALUE)
    {
        e.add(keys.level_value1); e.add_null();
    } else
        e.add(keys.level_value1, m_value1);

    if (m_type2 == GRIB2S::MISSING_TYPE)
    {
        e.add(keys.level_l2); e.add_null();
    }
    else
        e.add(keys.level_l2, (unsigned)m_type2);

    if (m_scale2 == GRIB2S::MISSING_SCALE)
    {
        e.add(keys.level_scale2); e.add_null();
    } else
        e.add(keys.level_scale2, (unsigned)m_scale2);

    if (m_value2 == GRIB2S::MISSING_VALUE)
    {
        e.add(keys.level_value2); e.add_null();
    } else
        e.add(keys.level_value2, m_value2);

}

std::unique_ptr<GRIB2D> GRIB2D::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    int l1 = GRIB2S::MISSING_TYPE, s1 = GRIB2S::MISSING_SCALE, v1 = GRIB2S::MISSING_VALUE;
    if (val.has_key(keys.level_l1, structured::NodeType::INT))
        l1 = val.as_int(keys.level_l1, "level type1");
    if (val.has_key(keys.level_scale1, structured::NodeType::INT))
        s1 = val.as_int(keys.level_scale1, "level scale1");
    if (val.has_key(keys.level_value1, structured::NodeType::INT))
        v1 = val.as_int(keys.level_value1, "level value1");

    int l2 = GRIB2S::MISSING_TYPE, s2 = GRIB2S::MISSING_SCALE, v2 = GRIB2S::MISSING_VALUE;
    if (val.has_key(keys.level_l2, structured::NodeType::INT))
        l2 = val.as_int(keys.level_l2, "level type2");
    if (val.has_key(keys.level_scale2, structured::NodeType::INT))
        s2 = val.as_int(keys.level_scale2, "level scale2");
    if (val.has_key(keys.level_value2, structured::NodeType::INT))
        v2 = val.as_int(keys.level_value2, "level value2");

    return GRIB2D::create(l1, s1, v1, l2, s2, v2);
}

std::string GRIB2D::exactQuery() const
{
    stringstream res;
    res << "GRIB2D,";
    if (m_type1 == GRIB2S::MISSING_TYPE) res << "-,"; else res << (unsigned)m_type1 << ",";
    if (m_scale1 == GRIB2S::MISSING_SCALE) res << "-,"; else res << (unsigned)m_scale1 << ",";
    if (m_value1 == GRIB2S::MISSING_VALUE) res << "-,"; else res << m_value1 << ",";
    if (m_type2 == GRIB2S::MISSING_TYPE) res << "-,"; else res << (unsigned)m_type2 << ",";
    if (m_scale2 == GRIB2S::MISSING_SCALE) res << "-,"; else res << (unsigned)m_scale2 << ",";
    if (m_value2 == GRIB2S::MISSING_VALUE) res << "-"; else res << m_value2;
    return res.str();
}
const char* GRIB2D::lua_type_name() const { return "arki.types.level.grib2d"; }

int GRIB2D::compare_local(const Level& o) const
{
    if (int res = Level::compare_local(o)) return res;
	// We should be the same kind, so upcast
	const GRIB2D* v = dynamic_cast<const GRIB2D*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a GRIB2D Level, but is a ") + typeid(&o).name() + " instead");

	// FIXME: here we can handle uniforming the scales if needed
    if (int res = compare_with_missing(m_type1, v->m_type1, GRIB2S::MISSING_TYPE)) return res;
    if (int res = compare_with_missing(m_scale1, v->m_scale1, GRIB2S::MISSING_SCALE)) return res;
    if (int res = compare_with_missing(m_value1, v->m_value1, GRIB2S::MISSING_VALUE)) return res;
    if (int res = compare_with_missing(m_type2, v->m_type2, GRIB2S::MISSING_TYPE)) return res;
    if (int res = compare_with_missing(m_scale2, v->m_scale2, GRIB2S::MISSING_SCALE)) return res;
    return compare_with_missing(m_value2, v->m_value2, GRIB2S::MISSING_VALUE);
}

bool GRIB2D::equals(const Type& o) const
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
        if (m_type1 == GRIB2S::MISSING_TYPE)
            lua_pushnil(L);
        else
            lua_pushnumber(L, m_type1);
    else if (name == "scale1")
        if (m_scale1 == GRIB2S::MISSING_SCALE)
            lua_pushnil(L);
        else
            lua_pushnumber(L, m_scale1);
    else if (name == "value1")
        if (m_value1 == GRIB2S::MISSING_VALUE)
            lua_pushnil(L);
        else
            lua_pushnumber(L, m_value1);
    else if (name == "type2")
        if (m_type2 == GRIB2S::MISSING_TYPE)
            lua_pushnil(L);
        else
            lua_pushnumber(L, m_type2);
    else if (name == "scale2")
        if (m_scale2 == GRIB2S::MISSING_SCALE)
            lua_pushnil(L);
        else
            lua_pushnumber(L, m_scale2);
    else if (name == "value2")
        if (m_value2 == GRIB2S::MISSING_VALUE)
            lua_pushnil(L);
        else
            lua_pushnumber(L, m_value2);
    else
        return Level::lua_lookup(L, name);

    return true;
}
#endif

GRIB2D* GRIB2D::clone() const
{
    GRIB2D* res = new GRIB2D;
    res->m_type1 = m_type1;
    res->m_scale1 = m_scale1;
    res->m_value1 = m_value1;
    res->m_type2 = m_type2;
    res->m_scale2 = m_scale2;
    res->m_value2 = m_value2;
    return res;
}

unique_ptr<GRIB2D> GRIB2D::create(
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
    return unique_ptr<GRIB2D>(res);
}

Level::Style ODIMH5::style() const { return Style::ODIMH5; }

void ODIMH5::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Level::encodeWithoutEnvelope(enc);
    enc.add_double(m_min);
    enc.add_double(m_max);
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
void ODIMH5::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Level::serialise_local(e, keys, f);
    e.add(keys.level_min, m_min);
    e.add(keys.level_max, m_max);
}

std::unique_ptr<ODIMH5> ODIMH5::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return ODIMH5::create(
            val.as_double(keys.level_min, "level min"),
            val.as_double(keys.level_max, "level max"));
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

static inline int compare_double(double a, double b)
{
    if (a < b) return -1;
    if (b < a) return 1;
    return 0;
}

int ODIMH5::compare_local(const Level& o) const
{
    if (int res = Level::compare_local(o)) return res;
	// We should be the same kind, so upcast
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a ODIMH5 Level, but is a ") + typeid(&o).name() + " instead");

    if (int res = compare_double(m_min, v->m_min)) return res;
    return compare_double(m_max, v->m_max);
}

bool ODIMH5::equals(const Type& o) const
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

ODIMH5* ODIMH5::clone() const
{
    ODIMH5* res = new ODIMH5;
    res->m_max = m_max;
    res->m_min = m_min;
    return res;
}

unique_ptr<ODIMH5> ODIMH5::create(double val)
{
    return ODIMH5::create(val, val);
}

unique_ptr<ODIMH5> ODIMH5::create(double min, double max)
{
    ODIMH5* res = new ODIMH5;
    res->m_max = max;
    res->m_min = min;
    return unique_ptr<ODIMH5>(res);
}

}

void Level::init()
{
    MetadataType::register_type<Level>();
}

}
}
#include <arki/types/styled.tcc>
