#include "arki/exceptions.h"
#include "arki/core/binary.h"
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

const uint8_t Level::GRIB2_MISSING_TYPE = 0xff;
const uint8_t Level::GRIB2_MISSING_SCALE = 0xff;
const uint32_t Level::GRIB2_MISSING_VALUE = 0xffffffff;


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

template<typename A, typename B, typename M>
static inline int compare_with_missing(const A& a, const B& b, const M& missing)
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

static inline int compare_double(double a, double b)
{
    if (a < b) return -1;
    if (b < a) return 1;
    return 0;
}

unsigned Level::GRIB1_type_vals(unsigned char type)
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

Level* Level::clone() const
{
    return new Level(data, size);
}

int Level::compare(const Type& o) const
{
    int res = Encoded::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Level* v = dynamic_cast<const Level*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `Level`, but it is `" << typeid(&o).name() << "' instead";
        throw std::runtime_error(ss.str());
    }

    auto sty = style();

    // Compare style
    if (int res = (int)sty - (int)v->style()) return res;

    // Styles are the same, compare the rest
    switch (sty)
    {
        case level::Style::GRIB1:
        {
            // TODO: can probably do lexicographical compare of raw data here?
            unsigned ty, l1, l2, vty, vl1, vl2;
            get_GRIB1(ty, l1, l2);
            v->get_GRIB1(vty, vl1, vl2);
            if (int res = ty - vty) return res;
            if (int res = l1 - vl1) return res;
            return l2 - vl2;
        }
        case level::Style::GRIB2S:
        {
            // TODO: can probably do lexicographical compare of raw data here?
            unsigned ty, sc, va, vty, vsc, vva;
            get_GRIB2S(ty, sc, va);
            v->get_GRIB2S(vty, vsc, vva);

            // FIXME: here we can handle uniforming the scales if needed
            if (int res = compare_with_missing(ty, vty, GRIB2_MISSING_TYPE)) return res;
            if (int res = compare_with_missing(sc, vsc, GRIB2_MISSING_SCALE)) return res;
            return compare_with_missing(va, vva, GRIB2_MISSING_VALUE);
        }
        case level::Style::GRIB2D:
        {
            // TODO: can probably do lexicographical compare of raw data here?
            unsigned ty1, sc1, va1, ty2, sc2, va2;
            unsigned vty1, vsc1, vva1, vty2, vsc2, vva2;
            get_GRIB2D(ty1, sc1, va1, ty2, sc2, va2);
            v->get_GRIB2D(vty1, vsc1, vva1, vty2, vsc2, vva2);

            // FIXME: here we can handle uniforming the scales if needed
            if (int res = compare_with_missing(ty1, vty1, GRIB2_MISSING_TYPE)) return res;
            if (int res = compare_with_missing(sc1, vsc1, GRIB2_MISSING_SCALE)) return res;
            if (int res = compare_with_missing(va1, vva1, GRIB2_MISSING_VALUE)) return res;
            if (int res = compare_with_missing(ty2, vty2, GRIB2_MISSING_TYPE)) return res;
            if (int res = compare_with_missing(sc2, vsc2, GRIB2_MISSING_SCALE)) return res;
            return compare_with_missing(va2, vva2, GRIB2_MISSING_VALUE);
        }
        case level::Style::ODIMH5:
        {
            double vmin, vmax, vvmin, vvmax;
            get_ODIMH5(vmin, vmax);
            v->get_ODIMH5(vvmin, vvmax);

            if (int res = compare_double(vmin, vvmin)) return res;
            return compare_double(vmax, vvmax);
        }

        default:
            throw_consistency_error("parsing Level", "unknown Level style " + formatStyle(sty));
    }
}


level::Style Level::style(const uint8_t* data, unsigned size)
{
    return (level::Style)data[0];
}

void Level::get_GRIB1(const uint8_t* data, unsigned size, unsigned& type, unsigned& l1, unsigned& l2)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    type = dec.pop_uint(1, "level type");
    switch (GRIB1_type_vals(type))
    {
        case 0:
            l1 = l2 = 0;
            break;
        case 1: {
            l1 = dec.pop_varint<unsigned short>("GRIB1 level l1");
            l2 = 0;
            break;
        }
        default: {
            l1 = dec.pop_uint(1, "GRIB1 layer l1");
            l2 = dec.pop_uint(1, "GRIB1 layer l2");
            break;
        }
    }
}
void Level::get_GRIB2S(const uint8_t* data, unsigned size, unsigned& type, unsigned& scale, unsigned& value)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    type = dec.pop_uint(1, "GRIB2S level type");
    scale = dec.pop_uint(1, "GRIB2S level scale");
    value = dec.pop_varint<uint32_t>("GRIB2S level value");
}

void Level::get_GRIB2D(const uint8_t* data, unsigned size, unsigned& type1, unsigned& scale1, unsigned& value1, unsigned& type2, unsigned& scale2, unsigned& value2)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    type1 = dec.pop_uint(1, "GRIB2D level type1");
    scale1 = dec.pop_uint(1, "GRIB2D level scale1");
    value1 = dec.pop_varint<uint32_t>("GRIB2D level value1");
    type2 = dec.pop_uint(1, "GRIB2D level type2");
    scale2 = dec.pop_uint(1, "GRIB2D level scale2");
    value2 = dec.pop_varint<uint32_t>("GRIB2D level value2");
}

void Level::get_ODIMH5(const uint8_t* data, unsigned size, double& min, double& max)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    min = dec.pop_double("ODIMH5 min");
    max = dec.pop_double("ODIMH5 max");
}

std::ostream& Level::writeToOstream(std::ostream& o) const
{
    auto sty = style();
    switch (sty)
    {
        case level::Style::GRIB1:
        {
            unsigned ty, l1, l2;
            get_GRIB1(ty, l1, l2);
            o << formatStyle(sty) << "(";
            o << setfill('0') << internal;
            o << setw(3) << ty;
            switch (GRIB1_type_vals(ty))
            {
                case 0: break;
                case 1:
                     o << ", " << setw(5) << l1;
                     break;
                default:
                     o << ", " << setw(3) << l1 << ", " << setw(3) << l2;
                     break;
            }
            o << setfill(' ');
            return o << ")";
        }
        case level::Style::GRIB2S:
        {
            unsigned ty, sc, va;
            get_GRIB2S(ty, sc, va);

            utils::SaveIOState sios(o);
            o << formatStyle(sty) << "(";

            if (ty == GRIB2_MISSING_TYPE)
                o << setfill(' ') << internal << setw(3) << "-" << ", ";
            else
                o << setfill('0') << internal << setw(3) << ty << ", ";

            if (sc == GRIB2_MISSING_SCALE)
                o << setfill(' ') << internal << setw(3) << "-" << ", ";
            else
                o << setfill('0') << internal << setw(3) << sc << ", ";

            if (va == GRIB2_MISSING_VALUE)
                o << setfill(' ') << internal << setw(10) << "-" << ")";
            else
                o << setfill('0') << internal << setw(10) << va << ")";

            return o;
        }
        case level::Style::GRIB2D:
        {
            unsigned ty1, sc1, va1, ty2, sc2, va2;
            get_GRIB2D(ty1, sc1, va1, ty2, sc2, va2);

            utils::SaveIOState sios(o);

            o << formatStyle(style()) << "(";

            if (ty1 == GRIB2_MISSING_TYPE)
                o << setfill(' ') << internal << setw(3) << "-" << ", ";
            else
                o << setfill('0') << internal << setw(3) << ty1 << ", ";

            if (sc1 == GRIB2_MISSING_SCALE)
                o << setfill(' ') << internal << setw(3) << "-" << ", ";
            else
                o << setfill('0') << internal << setw(3) << sc1 << ", ";

            if (va1 == GRIB2_MISSING_VALUE)
                o << setfill(' ') << internal << setw(10) << "-" << ",";
            else
                o << setfill('0') << internal << setw(10) << va1 << ", ";

            if (ty2 == GRIB2_MISSING_TYPE)
                o << setfill(' ') << internal << setw(3) << "-" << ", ";
            else
                o << setfill('0') << internal << setw(3) << ty2 << ", ";

            if (sc2 == GRIB2_MISSING_SCALE)
                o << setfill(' ') << internal << setw(3) << "-" << ", ";
            else
                o << setfill('0') << internal << setw(3) << sc2 << ", ";

            if (va2 == GRIB2_MISSING_VALUE)
                o << setfill(' ') << internal << setw(10) << "-" << ")";
            else
                o << setfill('0') << internal << setw(10) << va2 << ")";

            return o;
        }
        case level::Style::ODIMH5:
        {
            double vmin, vmax;
            get_ODIMH5(vmin, vmax);

            utils::SaveIOState sios(o);
            return o
                << formatStyle(style()) << "("
                << std::setprecision(5) << vmin
                << ", "
                << std::setprecision(5) << vmax
                << ")"
                ;
        }
        default:
            throw_consistency_error("parsing Level", "unknown Level style " + formatStyle(sty));
    }
}

void Level::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    auto sty = style();
    e.add(keys.type_style, formatStyle(sty));
    switch (sty)
    {
        case level::Style::GRIB1:
        {
            unsigned ty, l1, l2;
            get_GRIB1(ty, l1, l2);
            e.add(keys.level_type, ty);
            switch (GRIB1_type_vals(ty))
            {
                case 0: break;
                case 1:
                    e.add(keys.level_l1, l1);
                    break;
                case 2:
                    e.add(keys.level_l1, l1);
                    e.add(keys.level_l2, l2);
                    break;
            }
            break;
        }
        case level::Style::GRIB2S:
        {
            unsigned ty, sc, va;
            get_GRIB2S(ty, sc, va);
            if (ty == GRIB2_MISSING_TYPE)
            {
                e.add(keys.level_type); e.add_null();
            }
            else
                e.add(keys.level_type, ty);

            if (sc == GRIB2_MISSING_SCALE)
            {
                e.add(keys.level_scale); e.add_null();
            } else
                e.add(keys.level_scale, sc);

            if (va == GRIB2_MISSING_VALUE)
            {
                e.add(keys.level_value); e.add_null();
            } else
                e.add(keys.level_value, va);
            break;
        }
        case level::Style::GRIB2D:
        {
            unsigned ty1, sc1, va1, ty2, sc2, va2;
            get_GRIB2D(ty1, sc1, va1, ty2, sc2, va2);
            if (ty1 == GRIB2_MISSING_TYPE)
            {
                e.add(keys.level_l1); e.add_null();
            }
            else
                e.add(keys.level_l1, ty1);

            if (sc1 == GRIB2_MISSING_SCALE)
            {
                e.add(keys.level_scale1); e.add_null();
            } else
                e.add(keys.level_scale1, sc1);

            if (va1 == GRIB2_MISSING_VALUE)
            {
                e.add(keys.level_value1); e.add_null();
            } else
                e.add(keys.level_value1, va1);

            if (ty2 == GRIB2_MISSING_TYPE)
            {
                e.add(keys.level_l2); e.add_null();
            }
            else
                e.add(keys.level_l2, ty2);

            if (sc2 == GRIB2_MISSING_SCALE)
            {
                e.add(keys.level_scale2); e.add_null();
            } else
                e.add(keys.level_scale2, sc2);

            if (va2 == GRIB2_MISSING_VALUE)
            {
                e.add(keys.level_value2); e.add_null();
            } else
                e.add(keys.level_value2, va2);
            break;
        }
        case level::Style::ODIMH5:
        {
            double vmin, vmax;
            get_ODIMH5(vmin, vmax);
            e.add(keys.level_min, vmin);
            e.add(keys.level_max, vmax);
            break;
        }
        default:
            throw_consistency_error("parsing Level", "unknown Level style " + formatStyle(sty));
    }
}

std::string Level::exactQuery() const
{
    auto sty = style();
    switch (sty)
    {
        case level::Style::GRIB1:
        {
            unsigned ty, l1, l2;
            get_GRIB1(ty, l1, l2);
            char buf[128];
            switch (GRIB1_type_vals(ty))
            {
                case 0: snprintf(buf, 128, "GRIB1,%u", ty); break;
                case 1: snprintf(buf, 128, "GRIB1,%u,%u", ty, l1); break;
                default: snprintf(buf, 128, "GRIB1,%u,%u,%u", ty, l1, l2); break;
            }
            return buf;
        }
        case level::Style::GRIB2S:
        {
            unsigned ty, sc, va;
            get_GRIB2S(ty, sc, va);
            std::stringstream res;
            res << "GRIB2S,";
            if (ty == GRIB2_MISSING_TYPE) res << "-,"; else res << ty << ",";
            if (sc == GRIB2_MISSING_SCALE) res << "-,"; else res << sc << ",";
            if (va == GRIB2_MISSING_VALUE) res << "-"; else res << va;
            return res.str();
        }
        case level::Style::GRIB2D:
        {
            unsigned ty1, sc1, va1, ty2, sc2, va2;
            get_GRIB2D(ty1, sc1, va1, ty2, sc2, va2);
            std::stringstream res;
            res << "GRIB2D,";
            if (ty1 == GRIB2_MISSING_TYPE) res << "-,"; else res << ty1 << ",";
            if (sc1 == GRIB2_MISSING_SCALE) res << "-,"; else res << sc1 << ",";
            if (va1 == GRIB2_MISSING_VALUE) res << "-,"; else res << va1 << ",";
            if (ty2 == GRIB2_MISSING_TYPE) res << "-,"; else res << ty2 << ",";
            if (sc2 == GRIB2_MISSING_SCALE) res << "-,"; else res << sc2 << ",";
            if (va2 == GRIB2_MISSING_VALUE) res << "-"; else res << va2;
            return res.str();
        }
        case level::Style::ODIMH5:
        {
            double vmin, vmax;
            get_ODIMH5(vmin, vmax);
            std::ostringstream ss;
            ss  << "ODIMH5,range " 
                << std::setprecision(5) << vmin
                << " "
                << std::setprecision(5) << vmax
                ;
            return ss.str();
        }
        default:
            throw_consistency_error("parsing Level", "unknown Level style " + formatStyle(sty));
    }
}


unique_ptr<Level> Level::decode(core::BinaryDecoder& dec)
{
    dec.ensure_size(1, "Level style");
    std::unique_ptr<Level> res(new Level(dec.buf, dec.size));
    dec.skip(dec.size);
    return res;
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
            switch (GRIB1_type_vals((unsigned char)type))
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

            uint8_t type = getUnsigned<uint8_t>(start, "level type", GRIB2_MISSING_TYPE);
            uint8_t scale = getUnsigned<uint8_t>(start, "scale of level value", GRIB2_MISSING_SCALE);
            uint32_t value = getUnsigned<uint32_t>(start, "level value", GRIB2_MISSING_VALUE);
            return createGRIB2S(type, scale, value);
        }
        case Style::GRIB2D: {
            const char* start = inner.c_str();

            uint8_t type1 = getUnsigned<uint8_t>(start, "type of first level", GRIB2_MISSING_TYPE);
            uint32_t scale1 = getUnsigned<uint32_t>(start, "scale of value of first level", GRIB2_MISSING_SCALE);
            uint32_t value1 = getUnsigned<uint32_t>(start, "value of first level", GRIB2_MISSING_VALUE);
            uint8_t type2 = getUnsigned<uint8_t>(start, "type of second level", GRIB2_MISSING_TYPE);
            uint32_t scale2 = getUnsigned<uint32_t>(start, "scale of value of second level", GRIB2_MISSING_SCALE);
            uint32_t value2 = getUnsigned<uint32_t>(start, "value of second level", GRIB2_MISSING_VALUE);
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
    level::Style sty = parseStyle(val.as_string(keys.type_style, "type style"));
    switch (sty)
    {
        case level::Style::GRIB1:
        {
            int lt = val.as_int(keys.level_type, "level type");
            switch (GRIB1_type_vals(lt))
            {
                case 0: return createGRIB1(lt);
                case 1: return createGRIB1(lt, val.as_int(keys.level_l1, "level l1"));
                case 2:
                    return createGRIB1(
                            lt,
                            val.as_int(keys.level_l1, "level l1"),
                            val.as_int(keys.level_l2, "level l2"));
                default:
                    throw std::invalid_argument("unsupported level type value " + std::to_string(lt));
            }
        }
        case level::Style::GRIB2S:
        {
            int lt = GRIB2_MISSING_TYPE, sc = GRIB2_MISSING_SCALE, va = GRIB2_MISSING_VALUE;
            if (val.has_key(keys.level_type, structured::NodeType::INT))
                lt = val.as_int(keys.level_type, "level type");
            if (val.has_key(keys.level_scale, structured::NodeType::INT))
                sc = val.as_int(keys.level_scale, "level scale");
            if (val.has_key(keys.level_value, structured::NodeType::INT))
                va = val.as_int(keys.level_value, "level value");
            return createGRIB2S(lt, sc, va);
        }
        case level::Style::GRIB2D:
        {
            int l1 = GRIB2_MISSING_TYPE, s1 = GRIB2_MISSING_SCALE, v1 = GRIB2_MISSING_VALUE;
            if (val.has_key(keys.level_l1, structured::NodeType::INT))
                l1 = val.as_int(keys.level_l1, "level type1");
            if (val.has_key(keys.level_scale1, structured::NodeType::INT))
                s1 = val.as_int(keys.level_scale1, "level scale1");
            if (val.has_key(keys.level_value1, structured::NodeType::INT))
                v1 = val.as_int(keys.level_value1, "level value1");

            int l2 = GRIB2_MISSING_TYPE, s2 = GRIB2_MISSING_SCALE, v2 = GRIB2_MISSING_VALUE;
            if (val.has_key(keys.level_l2, structured::NodeType::INT))
                l2 = val.as_int(keys.level_l2, "level type2");
            if (val.has_key(keys.level_scale2, structured::NodeType::INT))
                s2 = val.as_int(keys.level_scale2, "level scale2");
            if (val.has_key(keys.level_value2, structured::NodeType::INT))
                v2 = val.as_int(keys.level_value2, "level value2");

            return createGRIB2D(l1, s1, v1, l2, s2, v2);
        }
        case level::Style::ODIMH5:
            return createODIMH5(
                    val.as_double(keys.level_min, "level min"),
                    val.as_double(keys.level_max, "level max"));
        default:
            throw std::runtime_error("Unknown Level style");
    }
}

std::unique_ptr<Level> Level::createGRIB1(unsigned char type)
{
    return createGRIB1(type, 0, 0);
}

std::unique_ptr<Level> Level::createGRIB1(unsigned char type, unsigned short l1)
{
    return createGRIB1(type, l1, 0);
}

std::unique_ptr<Level> Level::createGRIB1(unsigned char type, unsigned short l1, unsigned char l2)
{
    // TODO: optimize encoding by precomputing buffer size and not using a vector?
    // to do that, we first need a function that estimates the size of the varints
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned((unsigned)level::Style::GRIB1, 1);
    enc.add_unsigned(type, 1);
    switch (GRIB1_type_vals(type))
    {
        case 0: break;
        case 1: enc.add_varint(l1); break;
        default:
            enc.add_unsigned(l1, 1);
            enc.add_unsigned(l2, 1);
            break;
    }
    return std::unique_ptr<Level>(new Level(buf));
}

std::unique_ptr<Level> Level::createGRIB2S(uint8_t type, uint8_t scale, uint32_t val)
{
#ifdef HAVE_GRIBAPI
    if (val == GRIB_MISSING_LONG)
        val = GRIB2_MISSING_VALUE;
#endif
    // TODO: optimize encoding by precomputing buffer size and not using a vector?
    // to do that, we first need a function that estimates the size of the varints
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned((unsigned)level::Style::GRIB2S, 1);
    enc.add_unsigned(type, 1);
    enc.add_unsigned(scale, 1);
    enc.add_varint(val);
    return std::unique_ptr<Level>(new Level(buf));
}

std::unique_ptr<Level> Level::createGRIB2D(uint8_t type1, uint8_t scale1, uint32_t val1,
                                    uint8_t type2, uint8_t scale2, uint32_t val2)
{
    // TODO: optimize encoding by precomputing buffer size and not using a vector?
    // to do that, we first need a function that estimates the size of the varints
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned((unsigned)level::Style::GRIB2D, 1);
    enc.add_unsigned(type1, 1); enc.add_unsigned(scale1, 1); enc.add_varint(val1);
    enc.add_unsigned(type2, 1); enc.add_unsigned(scale2, 1); enc.add_varint(val2);
    return std::unique_ptr<Level>(new Level(buf));
}

std::unique_ptr<Level> Level::createODIMH5(double value)
{
    return createODIMH5(value, value);
}

std::unique_ptr<Level> Level::createODIMH5(double min, double max)
{
    constexpr int bufsize = 1 + sizeof(double) * 2;
    uint8_t* buf = new uint8_t[bufsize];
    buf[0] = (uint8_t)level::Style::ODIMH5;
    core::BinaryEncoder::set_double(buf + 1, min);
    core::BinaryEncoder::set_double(buf + 1 + sizeof(double), max);
    return std::unique_ptr<Level>(new Level(buf, bufsize));
}

void Level::init()
{
    MetadataType::register_type<Level>();
}

}
}
