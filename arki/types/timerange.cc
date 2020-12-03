#include "arki/exceptions.h"
#include "arki/types/timerange.h"
#include "arki/types/utils.h"
#include "arki/utils/iostream.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <sstream>
#include <iomanip>
#include <cmath>
#include <cstring>

#define CODE TYPE_TIMERANGE
#define TAG "timerange"
#define SERSIZELEN 1

using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace types {

const char* traits<Timerange>::type_tag = TAG;
const types::Code traits<Timerange>::type_code = CODE;
const size_t traits<Timerange>::type_sersize_bytes = SERSIZELEN;

const char* traits<timerange::Timedef>::type_tag = TAG;
const types::Code traits<timerange::Timedef>::type_code = CODE;
const size_t traits<timerange::Timedef>::type_sersize_bytes = SERSIZELEN;

// Constants from meteosatlib's libgrib
/// Time ranges
typedef enum t_enum_GRIB_TIMERANGE {
  GRIB_TIMERANGE_UNKNOWN                                                  = -1,
  GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1                              = 0,
  GRIB_TIMERANGE_ANALYSIS_AT_REFTIME                                      = 1,
  GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2                 = 2,
  GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2               = 3,
  GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2     = 4,
  GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1               = 5,
  GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2             = 6,
  GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2              = 7,
  GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2                               = 10,
  GRIB_TIMERANGE_COSMO_NUDGING                                            = 13,
  GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2           = 51,
  GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2     = 113,
  GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2    = 114,
  GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2       = 115,
  GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2     = 116,
  GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED          = 117,
  GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2    = 118,
  GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED            = 119,
  GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2                 = 123,
  GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2            = 124,
  GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY       = 125,
  GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS                  = 128,
  GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS             = 129,
  GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES                       = 130,
  GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES                  = 131
} t_enum_GRIB_TIMERANGE;

/// Time units
typedef enum t_enum_GRIB_TIMEUNIT {
  GRIB_TIMEUNIT_UNKNOWN = -1,  ///< software internal use
  GRIB_TIMEUNIT_MINUTE  = 0,   ///< minute
  GRIB_TIMEUNIT_HOUR    = 1,   ///< hour
  GRIB_TIMEUNIT_DAY     = 2,   ///< day
  GRIB_TIMEUNIT_MONTH   = 3,   ///< month
  GRIB_TIMEUNIT_YEAR    = 4,   ///< year
  GRIB_TIMEUNIT_DECADE  = 5,   ///< 10 years
  GRIB_TIMEUNIT_NORMAL  = 6,   ///< 30 years
  GRIB_TIMEUNIT_CENTURY = 7,   ///< century
  GRIB_TIMEUNIT_HOURS3  = 10,  ///< 3 hours
  GRIB_TIMEUNIT_HOURS6  = 11,  ///< 6 hours
  GRIB_TIMEUNIT_HOURS12 = 12,  ///< 12 hours
  GRIB_TIMEUNIT_SECOND  = 254  ///< seconds
} t_enum_GRIB_TIMEUNIT;

static bool GRIB1_get_timeunit_conversion(t_enum_GRIB_TIMEUNIT unit, int& timemul)
{
    bool is_seconds = true;
    timemul = 1;
    switch (unit)
    {
        case GRIB_TIMEUNIT_UNKNOWN:
            throw_consistency_error("normalising TimeRange", "time unit is UNKNOWN (-1)");
        case GRIB_TIMEUNIT_MINUTE: timemul = 60; break;
        case GRIB_TIMEUNIT_HOUR: timemul = 3600; break;
        case GRIB_TIMEUNIT_DAY: timemul = 3600*24; break;
        case GRIB_TIMEUNIT_MONTH: timemul = 1; is_seconds = false; break;
        case GRIB_TIMEUNIT_YEAR: timemul = 12; is_seconds = false; break;
        case GRIB_TIMEUNIT_DECADE: timemul = 120; is_seconds = false; break;
        case GRIB_TIMEUNIT_NORMAL: timemul = 12*30; is_seconds = false; break;
        case GRIB_TIMEUNIT_CENTURY: timemul = 12*100; is_seconds = false; break;
        case GRIB_TIMEUNIT_HOURS3: timemul = 3600*3; break;
        case GRIB_TIMEUNIT_HOURS6: timemul = 3600*6; break;
        case GRIB_TIMEUNIT_HOURS12: timemul = 3600*12; break;
        case GRIB_TIMEUNIT_SECOND: timemul = 1; break;
        default:
        {
            std::stringstream ss;
            ss << "cannot normalise TimeRange: time unit is unknown (" << (unsigned)unit << ")";
            throw std::runtime_error(ss.str());
        }
    }
    return is_seconds;
}

static std::string formatTimeUnit(t_enum_GRIB_TIMEUNIT unit)
{
	switch (unit)
	{
		case GRIB_TIMEUNIT_UNKNOWN:
			throw_consistency_error("formatting TimeRange unit", "time unit is UNKNOWN (-1)");
		case GRIB_TIMEUNIT_MINUTE: return "m";
		case GRIB_TIMEUNIT_HOUR: return "h";
		case GRIB_TIMEUNIT_DAY: return "d";
		case GRIB_TIMEUNIT_MONTH: return "mo";
		case GRIB_TIMEUNIT_YEAR: return "y";
		case GRIB_TIMEUNIT_DECADE: return "de";
		case GRIB_TIMEUNIT_NORMAL: return "no";
		case GRIB_TIMEUNIT_CENTURY: return "ce";
		case GRIB_TIMEUNIT_HOURS3: return "h3";
		case GRIB_TIMEUNIT_HOURS6: return "h6";
		case GRIB_TIMEUNIT_HOURS12: return "h12";
		case GRIB_TIMEUNIT_SECOND: return "s";
		default:
        {
            std::stringstream ss;
            ss << "cannot normalise TimeRange: time unit is unknown (" << (int)unit << ")";
            throw std::runtime_error(ss.str());
        }
    }
}

static t_enum_GRIB_TIMEUNIT parseTimeUnit(const std::string& tu)
{
	if (tu == "m") return GRIB_TIMEUNIT_MINUTE;
	if (tu == "h") return GRIB_TIMEUNIT_HOUR;
	if (tu == "d") return GRIB_TIMEUNIT_DAY;
	if (tu == "mo") return GRIB_TIMEUNIT_MONTH;
	if (tu == "y") return GRIB_TIMEUNIT_YEAR;
	if (tu == "de") return GRIB_TIMEUNIT_DECADE;
	if (tu == "no") return GRIB_TIMEUNIT_NORMAL;
	if (tu == "ce") return GRIB_TIMEUNIT_CENTURY;
	if (tu == "h3") return GRIB_TIMEUNIT_HOURS3;
	if (tu == "h6") return GRIB_TIMEUNIT_HOURS6;
	if (tu == "h12") return GRIB_TIMEUNIT_HOURS12;
	if (tu == "s") return GRIB_TIMEUNIT_SECOND;
	throw_consistency_error("parsing TimeRange unit", "unknown time unit \""+tu+"\"");
}

Timerange::Style Timerange::parseStyle(const std::string& str)
{
    if (str == "GRIB1") return Style::GRIB1;
    if (str == "GRIB2") return Style::GRIB2;
    if (str == "Timedef") return Style::TIMEDEF;
    if (str == "BUFR") return Style::BUFR;
    throw_consistency_error("parsing Timerange style", "cannot parse Timerange style '"+str+"': only GRIB1, GRIB2, Timedef and BUFR are supported");
}

std::string Timerange::formatStyle(Timerange::Style s)
{
    switch (s)
    {
        case Style::GRIB1: return "GRIB1";
        case Style::GRIB2: return "GRIB2";
        case Style::TIMEDEF: return "Timedef";
        case Style::BUFR: return "BUFR";
        default:
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

Timerange::~Timerange() {}

int Timerange::compare(const Type& o) const
{
    int res = Encoded::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Timerange* v = dynamic_cast<const Timerange*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `Timerange`, but it is `" << typeid(&o).name() << "' instead";
        throw std::runtime_error(ss.str());
    }

    auto sty = style();

    // Compare style
    if (int res = (int)sty - (int)v->style()) return res;

    // Styles are the same, compare the rest.
    //
    // We can safely reinterpret_cast, avoiding an expensive dynamic_cast,
    // since we checked the style.
    switch (sty)
    {
        case timerange::Style::GRIB1:
            return reinterpret_cast<const timerange::GRIB1*>(this)->compare_local(
                    *reinterpret_cast<const timerange::GRIB1*>(v));
        case timerange::Style::GRIB2:
            return reinterpret_cast<const timerange::GRIB2*>(this)->compare_local(
                    *reinterpret_cast<const timerange::GRIB2*>(v));
        case timerange::Style::TIMEDEF:
            return reinterpret_cast<const timerange::Timedef*>(this)->compare_local(
                    *reinterpret_cast<const timerange::Timedef*>(v));
        case timerange::Style::BUFR:
            return reinterpret_cast<const timerange::BUFR*>(this)->compare_local(
                    *reinterpret_cast<const timerange::BUFR*>(v));
        default:
            throw_consistency_error("parsing Timerange", "unknown Timerange style " + formatStyle(sty));
    }
}

timerange::Style Timerange::style(const uint8_t* data, unsigned size)
{
    return (timerange::Style)data[0];
}

void Timerange::get_GRIB1(const uint8_t* data, unsigned size, unsigned& type, unsigned& unit, unsigned& p1, unsigned& p2)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    type = dec.pop_uint(1, "GRIB1 type");
    unit = dec.pop_uint(1, "GRIB1 unit");
    p1   = dec.pop_sint(1, "GRIB1 p1");
    p2   = dec.pop_sint(1, "GRIB1 p2");
}

void Timerange::get_GRIB1_normalised(const uint8_t* data, unsigned size, int& otype, timerange::GRIB1Unit& ounit, int& op1, int& op2, bool& use_op1, bool& use_op2)
{
    unsigned type, unit, p1, p2;
    get_GRIB1(data, size, type, unit, p1, p2);

    otype = type;

    // Make sense of the time range unit
    int timemul;
    if (GRIB1_get_timeunit_conversion(static_cast<t_enum_GRIB_TIMEUNIT>(unit), timemul))
        ounit = timerange::SECOND;
    else
        ounit = timerange::MONTH;

    // Convert p1 and p2
    op1 = 0, op2 = 0;
    use_op1 = use_op2 = false;

    switch ((t_enum_GRIB_TIMERANGE)type)
    {
        case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
            op1 = p1;
            use_op1 = true;
            break;
		case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
			break;
		case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2:
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2:
            op1 = p1; op2 = p2;
            use_op1 = use_op2 = true;
            break;
        case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2:
            op1 = p1 << 8 | p2;
            use_op1 = true;
            use_op2 = false;
            break;
		case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2:
		case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2:
		case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED:
		case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED:
		case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY:
		case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS:
		case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS:
		case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES:
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES:
            op1 = p1;
            op2 = p2;
            use_op1 = use_op2 = true;
            break;
		case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2:
        case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2:
            op2 = p2;
            use_op2 = true;
            break;
        default:
            // Fallback for unknown time range types
            op1 = p1;
            op2 = p2;
            use_op1 = use_op2 = true;
            break;
    }

	op1 *= timemul;
	op2 *= timemul;
}


void Timerange::get_GRIB2(const uint8_t* data, unsigned size, unsigned& type, unsigned& unit, signed long& p1, signed long& p2)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    type = dec.pop_uint(1, "GRIB2 type");
    unit = dec.pop_uint(1, "GRIB2 unit");
    p1   = dec.pop_sint(4, "GRIB2 p1");
    p2   = dec.pop_sint(4, "GRIB2 p2");
}

void Timerange::get_Timedef(const uint8_t* data, unsigned size, timerange::TimedefUnit& step_unit, unsigned& step_len, unsigned& stat_type, timerange::TimedefUnit& stat_unit, unsigned& stat_len)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    step_unit = static_cast<timerange::TimedefUnit>(dec.pop_byte("timedef forecast step unit"));
    step_len = 0;
    if (step_unit != 255)
        step_len = dec.pop_varint<uint32_t>("timedef forecast step length");
    stat_type = dec.pop_uint(1, "timedef statistical processing type");
    stat_unit = timerange::UNIT_MISSING;
    stat_len = 0;
    if (stat_type != 255)
    {
        stat_unit = static_cast<timerange::TimedefUnit>(dec.pop_byte("timedef statistical processing unit"));
        if (stat_unit != 255)
            stat_len = dec.pop_varint<uint32_t>("timedef statistical processing length");
    }
}

void Timerange::get_BUFR(const uint8_t* data, unsigned size, unsigned& unit, unsigned& value)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    unit  = dec.pop_byte("BUFR unit");
    value = dec.pop_varint<unsigned>("BUFR value");
}

std::unique_ptr<Timerange> Timerange::decode(core::BinaryDecoder& dec)
{
    dec.ensure_size(1, "Timerange style");
    Style sty = static_cast<timerange::Style>(dec.buf[0]);
    std::unique_ptr<Timerange> res;
    switch (sty)
    {
        case timerange::Style::GRIB1:
            dec.ensure_size(5, "GRIB1 data");
            res.reset(new timerange::GRIB1(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        case timerange::Style::GRIB2:
            dec.ensure_size(11, "GRIB2 data");
            res.reset(new timerange::GRIB2(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        case timerange::Style::TIMEDEF:
            dec.ensure_size(3, "Timedef data");
            res.reset(new timerange::Timedef(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        case timerange::Style::BUFR:
            dec.ensure_size(3, "BUFR data");
            res.reset(new timerange::BUFR(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        default:
            throw_consistency_error("parsing Timerange", "unknown Timerange style " + formatStyle(sty));
    }
    return res;
}

static void skipCommasAndSpaces(const char*& str)
{
    // Skip commas and spaces, if any
    while (*str && (::isspace(*str) || *str == ','))
        ++str;
}

static int getNumber(const char * & start, const char* what)
{
    char* endptr;

    skipCommasAndSpaces(start);

    if (!*start)
        throw_consistency_error("parsing TimeRange", std::string("no ") + what);

    int res = strtol(start, &endptr, 10);
    if (endptr == start)
        throw_consistency_error("parsing TimeRange",
                std::string("expected ") + what + ", but found \"" + start + "\"");
    start = endptr;

    skipCommasAndSpaces(start);

    return res;
}

static void skipSuffix(const char*& start)
{
    // Skip anything until the next space or comma
    while (*start && !::isspace(*start) && *start != ',')
        ++start;

    // Skip further commas and spaces
    while (*start && (::isspace(*start) || *start == ','))
        ++start;
}

std::unique_ptr<Timerange> Timerange::decodeString(const std::string& val)
{
    std::string inner;
    timerange::Style sty = outerParse<Timerange>(val, inner);
    switch (sty)
    {
        case timerange::Style::GRIB1:
        {
            const char* start = inner.c_str();
            char* endptr;
            int p1 = -1, p2 = -1;
            std::string p1tu, p2tu;

            if (!*start)
                throw_consistency_error("parsing TimeRange", "value is empty");

            // Parse the type
            int type = strtol(start, &endptr, 10);
            if (endptr == start)
                throw_consistency_error("parsing TimeRange",
                        "expected type, but found \"" + inner.substr(start - inner.c_str()) + "\"");
            start = endptr;

            // Skip colons and spaces, if any
            while (*start && (::isspace(*start) || *start == ','))
                ++start;

            if (*start)
            {
                const char* sfxend;

                // Parse p1
                p1 = strtol(start, &endptr, 10);
                if (endptr == start)
                    throw_consistency_error("parsing TimeRange",
                            "expected p1, but found \"" + inner.substr(start - inner.c_str()) + "\"");
                start = endptr;

                // Parse p1 suffix
                sfxend = start;
                while (*sfxend && isalnum(*sfxend))
                    ++sfxend;
                if (sfxend != start)
                {
                    p1tu = inner.substr(start-inner.c_str(), sfxend-start);
                    start = sfxend;
                }

                // Skip colons and spaces, if any
                while (*start && (::isspace(*start) || *start == ','))
                    ++start;

                if (*start)
                {
                    // Parse p2
                    p2 = strtol(start, &endptr, 10);
                    if (endptr == start)
                        throw_consistency_error("parsing TimeRange",
                                "expected p2, but found \"" + inner.substr(start - inner.c_str()) + "\"");
                    start = endptr;

                    // Parse p2 suffix
                    sfxend = start;
                    while (*sfxend && isalnum(*sfxend))
                        ++sfxend;
                    if (sfxend != start)
                    {
                        p2tu = inner.substr(start-inner.c_str(), sfxend-start);
                        start = sfxend;
                    }
                }
            }

            if (*start)
                throw_consistency_error("parsing TimeRange",
                    "found trailing characters at the end: \"" + inner.substr(start - inner.c_str()) + "\"");

            switch ((t_enum_GRIB_TIMERANGE)type)
            {
                case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
                    if (p1 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p1 not found, but it is required");
                    if (p2 != -1)
                        throw_consistency_error("parsing TimeRange",
                                "found an extra p2 when it was not required");
                    return createGRIB1(type, parseTimeUnit(p1tu), p1, 0);
                case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
                    if (p1 != -1)
                        throw_consistency_error("parsing TimeRange",
                                "found an extra p1 when it was not required");
                    if (p2 != -1)
                        throw_consistency_error("parsing TimeRange",
                                "found an extra p2 when it was not required");
                    return createGRIB1(type, 254, 0, 0);
                case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
                case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
                case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
                case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
                case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2:
                case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2:
                case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2:
                case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2:
                case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2:
                case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2:
                case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2:
                case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED:
                case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED:
                case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY:
                case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS:
                case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS:
                case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES:
                case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES:
                    if (p1 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p1 not found, but it is required");
                    if (p2 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p2 not found, but it is required");
                    if (p1tu != p2tu)
                        throw_consistency_error("parsing TimeRange",
                                "time unit for p1 (\""+p1tu+"\") is different than time unit of p2 (\""+p2tu+"\")");
                    return createGRIB1(type, parseTimeUnit(p1tu), p1, p2);
                case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2: {
                    if (p1 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p1 not found, but it is required");
                    if (p2 != -1)
                        throw_consistency_error("parsing TimeRange",
                                "found an extra p2 when it was not required");
                    p2 = p1 & 0xff;
                    p1 = p1 >> 8;
                    return createGRIB1(type, parseTimeUnit(p1tu), p1, p2);
                }
                case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2:
                case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2:
                case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2:
                    if (p1 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p1 not found, but it is required");
                    if (p2 != -1)
                        throw_consistency_error("parsing TimeRange",
                                "found an extra p2 when it was not required");
                    return createGRIB1(type, parseTimeUnit(p1tu), 0, p1);
                default:
                    // Fallback for unknown types
                    if (p1 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p1 not found, but it is required");
                    if (p2 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p2 not found, but it is required");
                    if (p1tu != p2tu)
                        throw_consistency_error("parsing TimeRange",
                                "time unit for p1 (\""+p1tu+"\") is different than time unit of p2 (\""+p2tu+"\")");
                    return createGRIB1(type, parseTimeUnit(p1tu), p1, p2);
            }
        }
        case timerange::Style::GRIB2:
        {
            const char* start = inner.c_str();
            int type = getNumber(start, "time range type");
            int unit = getNumber(start, "unit of time range values");
            int p1 = getNumber(start, "first time range value");
            skipSuffix(start);
            int p2 = getNumber(start, "second time range value");
            skipSuffix(start);
            return createGRIB2(type, unit, p1, p2);
        }
        case timerange::Style::BUFR:
        {
            const char* start = inner.c_str();
            unsigned value = getNumber(start, "forecast seconds");
            unsigned unit = parseTimeUnit(start);
            return createBUFR(value, unit);
        }
        case timerange::Style::TIMEDEF:
        {
            const char* str = inner.c_str();

            timerange::TimedefUnit step_unit;
            uint32_t step_len;
            if (!timerange::Timedef::timeunit_parse(str, step_unit, step_len))
                throw_consistency_error("parsing Timerange", "cannot parse time range step");

            int stat_type;
            timerange::TimedefUnit stat_unit = timerange::UNIT_MISSING;
            uint32_t stat_len = 0;

            if (!*str)
                stat_type = 255;
            else
            {
                stat_type = getNumber(str, "type of statistical processing");
                if (*str && !timerange::Timedef::timeunit_parse(str, stat_unit, stat_len))
                    throw_consistency_error("parsing Timerange", "cannot parse length of statistical processing");
            }

            return createTimedef(step_len, step_unit, stat_type, stat_len, stat_unit);
        }
        default:
            throw_consistency_error("parsing Timerange", "unknown Timerange style " + formatStyle(sty));
    }
}

std::unique_ptr<Timerange> Timerange::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    timerange::Style sty = parseStyle(val.as_string(keys.type_style, "type style"));
    switch (sty)
    {
        case timerange::Style::GRIB1:
            return createGRIB1(
                    val.as_int(keys.timerange_type, "timerange type"),
                    val.as_int(keys.timerange_unit, "timerange unit"),
                    val.as_int(keys.timerange_p1, "timerange p1"),
                    val.as_int(keys.timerange_p2, "timerange p2"));
        case timerange::Style::GRIB2:
            return createGRIB2(
                    val.as_int(keys.timerange_type, "timerange type"),
                    val.as_int(keys.timerange_unit, "timerange unit"),
                    val.as_int(keys.timerange_p1, "timerange p1"),
                    val.as_int(keys.timerange_p2, "timerange p2"));
        case timerange::Style::TIMEDEF:
        {
            uint32_t step_len = val.as_int(keys.timerange_step_len, "Timedef forecast step length");
            auto step_unit = static_cast<timerange::TimedefUnit>(val.as_int(keys.timerange_step_unit, "Timedef forecast step units"));

            int stat_type = 255;
            uint32_t stat_len = 0;
            auto stat_unit = timerange::UNIT_MISSING;

            if (val.has_key(keys.timerange_stat_type, structured::NodeType::INT))
            {
                stat_type = val.as_int(keys.timerange_stat_type, "Timedef statistical type");

                if (val.has_key(keys.timerange_stat_unit, structured::NodeType::INT))
                {
                    stat_unit = static_cast<timerange::TimedefUnit>(val.as_int(keys.timerange_stat_unit, "Timedef statistical unit"));
                    stat_len = val.as_int(keys.timerange_stat_len, "Timedef length of interval of statistical processing");
                }
            }

            return createTimedef(step_len, step_unit, stat_type, stat_len, stat_unit);
        }
        case timerange::Style::BUFR:
            return createBUFR(
                    val.as_int(keys.timerange_value, "timerange value"),
                    val.as_int(keys.timerange_unit, "timerange unit"));
        default:
            throw std::runtime_error("Unknown Timerange style");
    }
}

std::unique_ptr<Timerange> Timerange::createGRIB1(unsigned char type, unsigned char unit, unsigned char p1, unsigned char p2)
{
    // Normalise values to make comparison easier
    switch (static_cast<t_enum_GRIB_TIMERANGE>(type))
    {
        case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
            // Units do not matter for this timerange type, force everything to
            // hours
            if (p1 == 0 && p2 == 0)
                unit = 1;
            break;
        default:
            break;
    }

    uint8_t* buf = new uint8_t[5];
    buf[0] = (uint8_t)timerange::Style::GRIB1;
    buf[1] = type;
    buf[2] = unit;
    buf[3] = p1;
    buf[4] = p2;
    return std::unique_ptr<Timerange>(new timerange::GRIB1(buf, 5, true));
}

std::unique_ptr<Timerange> Timerange::createGRIB2(unsigned char type, unsigned char unit, signed long p1, signed long p2)
{
    uint8_t* buf = new uint8_t[11];
    buf[0] = (uint8_t)timerange::Style::GRIB2;
    buf[1] = type;
    buf[2] = unit;
    core::BinaryEncoder::set_signed(buf + 3, p1, 4);
    core::BinaryEncoder::set_signed(buf + 7, p2, 4);
    return std::unique_ptr<Timerange>(new timerange::GRIB2(buf, 11, true));
}

std::unique_ptr<Timerange> Timerange::createTimedef(uint32_t step_len, timerange::TimedefUnit step_unit)
{
    // Normalize input
    step_unit = step_unit == timerange::UNIT_MISSING ? timerange::UNIT_MISSING : step_len == 0 ? timerange::UNIT_SECOND : step_unit;

    // TODO: optimize encoding by precomputing buffer size and not using a vector?
    // to do that, we first need a function that estimates the size of the varints
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned(static_cast<unsigned>(timerange::Style::TIMEDEF), 1);
    enc.add_unsigned(static_cast<unsigned>(step_unit), 1);
    if (step_unit != 255)
        enc.add_varint(step_len);
    enc.add_unsigned(255u, 1);
    return std::unique_ptr<Timerange>(new timerange::Timedef(buf));
}

std::unique_ptr<Timerange> Timerange::createTimedef(uint32_t step_len, timerange::TimedefUnit step_unit,
                                             uint8_t stat_type, uint32_t stat_len, timerange::TimedefUnit stat_unit)
{
    // Normalize input
    step_unit = step_unit == timerange::UNIT_MISSING ? timerange::UNIT_MISSING : step_len == 0 ? timerange::UNIT_SECOND : step_unit;
    stat_unit = stat_unit == timerange::UNIT_MISSING ? timerange::UNIT_MISSING : stat_len == 0 ? timerange::UNIT_SECOND : stat_unit;

    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned(static_cast<unsigned>(timerange::Style::TIMEDEF), 1);
    enc.add_unsigned(static_cast<unsigned>(step_unit), 1);
    if (step_unit != 255)
        enc.add_varint(step_len);
    enc.add_unsigned(stat_type, 1);
    if (stat_type != 255)
    {
        enc.add_unsigned(static_cast<unsigned>(stat_unit), 1);
        if (stat_unit != 255)
            enc.add_varint(stat_len);
    }
    return std::unique_ptr<Timerange>(new timerange::Timedef(buf));

}

std::unique_ptr<Timerange> Timerange::createBUFR(unsigned value, unsigned char unit)
{
    // TODO: optimize encoding by precomputing buffer size and not using a vector?
    // to do that, we first need a function that estimates the size of the varints
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned(static_cast<unsigned>(timerange::Style::BUFR), 1);
    enc.add_unsigned(unit, 1);
    enc.add_varint(value);
    return std::unique_ptr<Timerange>(new timerange::BUFR(buf));
}

namespace timerange {

/*
 * GRIB1
 */

bool GRIB1::equals(const Type& o) const
{
    const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
    if (!v) return false;

    int atype, ap1, ap2, btype, bp1, bp2;
    GRIB1Unit aunit, bunit;
    bool dummy_use_p1, dummy_use_p2;
    get_GRIB1_normalised(atype, aunit, ap1, ap2, dummy_use_p1, dummy_use_p2);
    v->get_GRIB1_normalised(btype, bunit, bp1, bp2, dummy_use_p1, dummy_use_p2);

    return atype == btype && aunit == bunit && ap1 == bp1 && ap2 == bp2;
}

int GRIB1::compare_local(const GRIB1& o) const
{
    int atype, ap1, ap2, btype, bp1, bp2;
    GRIB1Unit aunit, bunit;
    bool dummy_use_p1, dummy_use_p2;
    get_GRIB1_normalised(atype, aunit, ap1, ap2, dummy_use_p1, dummy_use_p2);
    o.get_GRIB1_normalised(btype, bunit, bp1, bp2, dummy_use_p1, dummy_use_p2);
    if (int res = atype - btype) return res;
    if (int res = aunit - bunit) return res;
    if (int res = ap1 - bp1) return res;
    return ap2 - bp2;
}

std::ostream& GRIB1::writeToOstream(std::ostream& o) const
{
    o << formatStyle(Style::GRIB1) << "(";
    writeNumbers(o);
    return o << ")";
}

void GRIB1::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    unsigned ty, un, p1, p2;
    get_GRIB1(ty, un, p1, p2);
    e.add(keys.type_style, formatStyle(Style::GRIB1));
    e.add(keys.timerange_type, ty);
    e.add(keys.timerange_unit, un);
    e.add(keys.timerange_p1, p1);
    e.add(keys.timerange_p2, p2);
}

std::string GRIB1::exactQuery() const
{
    // unsigned ty, un, p1, p2;
    // get_GRIB1(ty, un, p1, p2);

    std::stringstream o;
    o << formatStyle(Style::GRIB1) << ", ";
    writeNumbers(o);
    return o.str();
}




std::ostream& GRIB1::writeNumbers(std::ostream& o) const
{
    using namespace std;
    unsigned type, unit, p1, p2;
    get_GRIB1(type, unit, p1, p2);
    o << setfill('0') << internal;
    switch ((t_enum_GRIB_TIMERANGE)type)
    {
        case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1: {
            std::string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)unit);
            o << setw(3) << type << ", " << setw(3) << p1 << suffix;
            break;
        }
        case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
            o << setw(3) << type;
            break;
		case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2: {
		case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2:
		case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2:
		case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED:
		case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED:
		case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY:
		case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS:
		case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS:
		case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES:
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES:
            string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)unit);
            o << setw(3) << type << ", " << setw(3) << p1 << suffix << ", " << setw(3) << p2 << suffix;
            break;
        }
        case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2: {
            string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)unit);
            o << setw(3) << type << ", " << setw(5) << (p1 << 8 | p2) << suffix;
            break;
        }
        case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2:
        case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2:
        case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2: {
            string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)unit);
            o << setw(3) << type << ", " << setw(3) << p2 << suffix;
            break;
        }
        default:
            // Fallback for unknown types
            string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)unit);
            o << setw(3) << type << ", " << setw(3) << p1 << suffix << ", " << setw(3) << p2 << suffix;
            break;
    }

	return o << setfill(' ');
}

bool GRIB1::get_forecast_step(int& step, bool& is_seconds) const
{
    unsigned type, unit, p1, p2;
    get_GRIB1(type, unit, p1, p2);

    int timemul;
    is_seconds = GRIB1_get_timeunit_conversion(static_cast<t_enum_GRIB_TIMEUNIT>(unit), timemul);

    switch (type)
    {
        case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1: // = 0
        case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME: // = 1
        case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2: // = 10
  // statproc = 254
  // CALL gribtr_to_second(unit, p1_g1, p1)
  // p2 = 0
            step = p1 * timemul;
            return true;
        case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2: // = 2
  // ELSE IF (tri == 2) THEN ! somewhere between p1 and p2, not good for grib2 standard
  //   statproc = 205
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            step = p2 * timemul;
            return true;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2: // = 3
  // ELSE IF (tri == 3) THEN ! average
  //   statproc = 0
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            step = p2 * timemul;
            return true;
        case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2: // = 4
  // ELSE IF (tri == 4) THEN ! accumulation
  //   statproc = 1
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            step = p2 * timemul;
            return true;
        case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1: // = 5
  // ELSE IF (tri == 5) THEN ! difference
  //   statproc = 4
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            step = p2 * timemul;
            return true;
        case GRIB_TIMERANGE_COSMO_NUDGING: // = 13
  // ELSE IF (tri == 13) THEN ! COSMO-nudging, use a temporary value then normalize
  //   statproc = 206 ! check if 206 is legal!
  //   p1 = 0 ! analysis regardless of p2_g1
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
// ! Timerange 206 (COSMO nudging) is converted to point in time if
// ! interval length is 0, or to a proper timerange if parameter is
// ! recognized as a COSMO statistically processed parameter (and in case
// ! of maximum or minimum the parameter is corrected as well); if
// ! parameter is not recognized, it is converted to instantaneous with a
// ! warning.
//            return 206;
// We cannot do that as we do not know the parameter here
            step = 0;
            return true;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2://             = 6,
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2://              = 7,
        case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2://           = 51,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2://     = 113,
        case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2://    = 114,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2://       = 115,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2://     = 116,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED://          = 117,
        case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2://    = 118,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED://            = 119,
        case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2://                 = 123,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2://            = 124,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY://       = 125,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS://                  = 128,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS://             = 129,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES://                       = 130,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES://                  = 131
  // call l4f_log(L4F_ERROR,'timerange_g1_to_g2: GRIB1 timerange '//TRIM(to_char(tri)) &
  //  //' cannot be converted to GRIB2.')
  // CALL raise_error()
            return false;

// if (statproc == 254 .and. p2 /= 0 ) then
//   call l4f_log(L4F_WARN,"inconsistence in timerange:254,"//trim(to_char(p1))//","//trim(to_char(p2)))
// end if
    }
    return false;
}

int GRIB1::get_proc_type() const
{
    auto type = static_cast<t_enum_GRIB_TIMERANGE>(data[1]);

    switch (type)
    {
        case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
        case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
        case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2:
  // statproc = 254
  // CALL gribtr_to_second(unit, p1_g1, p1)
  // p2 = 0
            return 254;
        case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 2) THEN ! somewhere between p1 and p2, not good for grib2 standard
  //   statproc = 205
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            return 205;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 3) THEN ! average
  //   statproc = 0
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            return 0;
        case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 4) THEN ! accumulation
  //   statproc = 1
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            return 1;
        case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
  // ELSE IF (tri == 5) THEN ! difference
  //   statproc = 4
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            return 4;
        case GRIB_TIMERANGE_COSMO_NUDGING:
  // ELSE IF (tri == 13) THEN ! COSMO-nudging, use a temporary value then normalize
  //   statproc = 206 ! check if 206 is legal!
  //   p1 = 0 ! analysis regardless of p2_g1
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
// ! Timerange 206 (COSMO nudging) is converted to point in time if
// ! interval length is 0, or to a proper timerange if parameter is
// ! recognized as a COSMO statistically processed parameter (and in case
// ! of maximum or minimum the parameter is corrected as well); if
// ! parameter is not recognized, it is converted to instantaneous with a
// ! warning.
//            return 206;
// We cannot do that as we do not know the parameter here
            return 254;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2://             = 6,
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2://              = 7,
        case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2://           = 51,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2://     = 113,
        case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2://    = 114,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2://       = 115,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2://     = 116,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED://          = 117,
        case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2://    = 118,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED://            = 119,
        case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2://                 = 123,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2://            = 124,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY://       = 125,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS://                  = 128,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS://             = 129,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES://                       = 130,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES://                  = 131
        case GRIB_TIMERANGE_UNKNOWN://                  = -1
  // call l4f_log(L4F_ERROR,'timerange_g1_to_g2: GRIB1 timerange '//TRIM(to_char(tri)) &
  //  //' cannot be converted to GRIB2.')
  // CALL raise_error()
            return -1;

// if (statproc == 254 .and. p2 /= 0 ) then
//   call l4f_log(L4F_WARN,"inconsistence in timerange:254,"//trim(to_char(p1))//","//trim(to_char(p2)))
// end if
    }
    return -1;
}

bool GRIB1::get_proc_duration(int& duration, bool& is_seconds) const
{
    unsigned type, unit, p1, p2;
    get_GRIB1(type, unit, p1, p2);

    int timemul;
    is_seconds = GRIB1_get_timeunit_conversion(static_cast<t_enum_GRIB_TIMEUNIT>(unit), timemul);

    switch (type)
    {
        case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
        case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
        case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2:
  // statproc = 254
  // CALL gribtr_to_second(unit, p1_g1, p1)
  // p2 = 0
            duration = 0;
            return true;
        case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 2) THEN ! somewhere between p1 and p2, not good for grib2 standard
  //   statproc = 205
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            duration = (p2 - p1) * timemul;
            return true;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 3) THEN ! average
  //   statproc = 0
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            duration = (p2 - p1) * timemul;
            return true;
        case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 4) THEN ! accumulation
  //   statproc = 1
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            duration = (p2 - p1) * timemul;
            return true;
        case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
  // ELSE IF (tri == 5) THEN ! difference
  //   statproc = 4
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            duration = (p2 - p1) * timemul;
            return true;
        case GRIB_TIMERANGE_COSMO_NUDGING:
  // ELSE IF (tri == 13) THEN ! COSMO-nudging, use a temporary value then normalize
  //   statproc = 206 ! check if 206 is legal!
  //   p1 = 0 ! analysis regardless of p2_g1
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
// ! Timerange 206 (COSMO nudging) is converted to point in time if
// ! interval length is 0, or to a proper timerange if parameter is
// ! recognized as a COSMO statistically processed parameter (and in case
// ! of maximum or minimum the parameter is corrected as well); if
// ! parameter is not recognized, it is converted to instantaneous with a
// ! warning.
//            return 206;
// We cannot do that as we do not know the parameter here
            duration = 0;
            return true;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2://             = 6,
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2://              = 7,
        case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2://           = 51,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2://     = 113,
        case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2://    = 114,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2://       = 115,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2://     = 116,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED://          = 117,
        case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2://    = 118,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED://            = 119,
        case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2://                 = 123,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2://            = 124,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY://       = 125,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS://                  = 128,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS://             = 129,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES://                       = 130,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES://                  = 131
  // call l4f_log(L4F_ERROR,'timerange_g1_to_g2: GRIB1 timerange '//TRIM(to_char(tri)) &
  //  //' cannot be converted to GRIB2.')
  // CALL raise_error()
            return false;

// if (statproc == 254 .and. p2 /= 0 ) then
//   call l4f_log(L4F_WARN,"inconsistence in timerange:254,"//trim(to_char(p1))//","//trim(to_char(p2)))
// end if
    }
    return false;
}

void GRIB1::arg_significance(unsigned type, bool& use_p1, bool& use_p2)
{
    use_p1 = use_p2 = false;

    switch ((t_enum_GRIB_TIMERANGE)type)
    {
        case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
            use_p1 = true;
            break;
        case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
            break;
        case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
        case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
        case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2:
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2:
            use_p1 = use_p2 = true;
            break;
        case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2:
            use_p1 = true;
            use_p2 = true;
            break;
        case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2:
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2:
        case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2:
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2:
        case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2:
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED:
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED:
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY:
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS:
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS:
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES:
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES:
            use_p1 = use_p2 = true;
            break;
        case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2:
        case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2:
        case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2:
            use_p2 = true;
            break;
        default:
            // Fallback for unknown time range types
            use_p1 = use_p2 = true;
            break;
    }
}


/*
 * GRIB2
 */

int GRIB2::compare_local(const GRIB2& o) const
{
    // TODO: can probably do lexicographical compare of raw data here?
    unsigned ty, un, vty, vun;
    signed long p1, p2, vp1, vp2;
    get_GRIB2(ty, un, p1, p2);
    o.get_GRIB2(vty, vun, vp1, vp2);
    // TODO: normalise the time units if needed
    if (int res = ty - vty) return res;
    if (int res = un - vun) return res;
    if (int res = p1 - vp1) return res;
    return p2 - vp2;
}

std::ostream& GRIB2::writeToOstream(std::ostream& o) const
{
    using namespace std;
    unsigned ty, un;
    signed long p1, p2;
    get_GRIB2(ty, un, p1, p2);

    utils::SaveIOState sios(o);
    std::string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)un);

    return o
      << formatStyle(Style::GRIB2) << "("
      << setfill('0') << internal
      << setw(3) << ty << ", "
      << setw(3) << un << ", "
      << setw(10) << p1 << suffix << ", "
      << setw(10) << p2 << suffix
      << ")";
}

void GRIB2::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    unsigned ty, un;
    signed long p1, p2;
    get_GRIB2(ty, un, p1, p2);
    e.add(keys.type_style, formatStyle(Style::GRIB2));
    e.add(keys.timerange_type, ty);
    e.add(keys.timerange_unit, un);
    e.add(keys.timerange_p1, p1);
    e.add(keys.timerange_p2, p2);
}

std::string GRIB2::exactQuery() const
{
    unsigned ty, un;
    signed long p1, p2;
    get_GRIB2(ty, un, p1, p2);

    std::stringstream o;
    o << formatStyle(Style::GRIB2) << ","
      << ty << ","
      << un << ","
      << p1 << ","
      << p2;
    return o.str();
}

bool GRIB2::get_forecast_step(int& step, bool& is_seconds) const
{
    return false;
}

int GRIB2::get_proc_type() const
{
    return -1;
}

bool GRIB2::get_proc_duration(int& duration, bool& is_seconds) const
{
    return false;
}


/*
 * Timedef
 */

bool Timedef::equals(const Type& o) const
{
    const Timedef* v = dynamic_cast<const Timedef*>(&o);
    if (!v) return false;

    timerange::TimedefUnit step_unit, stat_unit, vstep_unit, vstat_unit;
    unsigned step_len, stat_type, stat_len, vstep_len, vstat_type, vstat_len;
    get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);
    v->get_Timedef(vstep_unit, vstep_len, vstat_type, vstat_unit, vstat_len);


    // Normalise step time units and compare steps
    if (step_unit == UNIT_MISSING && vstep_unit != UNIT_MISSING) return false;
    if (step_unit != UNIT_MISSING && vstep_unit == UNIT_MISSING) return false;
    if (step_unit != UNIT_MISSING)
    {
        int timemul1;
        bool is1 = timeunit_conversion(step_unit, timemul1);
        int timemul2;
        bool is2 = timeunit_conversion(vstep_unit, timemul2);
        if (is1 != is2) return false;
        if (step_len * timemul1 != vstep_len * timemul2) return false;
    }

    // Compare type of statistical processing
    if (stat_type != vstat_type) return false;

    // Normalise stat units and compare stat processing length
    if (stat_unit == UNIT_MISSING && vstat_unit != UNIT_MISSING) return false;
    if (stat_unit != UNIT_MISSING && vstat_unit == UNIT_MISSING) return false;
    if (stat_unit != UNIT_MISSING)
    {
        int timemul1;
        bool is1 = timeunit_conversion(stat_unit, timemul1);
        int timemul2;
        bool is2 = timeunit_conversion(vstat_unit, timemul2);
        if (is1 != is2) return false;
        if (stat_len * timemul1 != vstat_len * timemul2) return false;
    }

    return true;
}

int Timedef::compare_local(const Timedef& o) const
{
    timerange::TimedefUnit step_unit, stat_unit, vstep_unit, vstat_unit;
    unsigned step_len, stat_type, stat_len, vstep_len, vstat_type, vstat_len;
    get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);
    o.get_Timedef(vstep_unit, vstep_len, vstat_type, vstat_unit, vstat_len);

    // Normalise step time units and compare steps
    if (step_unit == 255 && vstep_unit != 255) return -1;
    if (step_unit != 255 && vstep_unit == 255) return 1;
    if (step_unit != 255)
    {
        int timemul1;
        bool is1 = timeunit_conversion(step_unit, timemul1);
        int timemul2;
        bool is2 = timeunit_conversion(vstep_unit, timemul2);
        if (is1 && !is2) return -1;
        if (is2 && !is1) return 1;
        if (int res = step_len * timemul1 - vstep_len * timemul2) return res;
    }

    // Compare type of statistical processing
    if (int res = stat_type - vstat_type) return res;

    // Normalise stat units and compare stat processing length
    if (stat_unit == 255 && vstat_unit != 255) return -1;
    if (stat_unit != 255 && vstat_unit == 255) return 1;
    if (stat_unit != 255)
    {
        int timemul1;
        bool is1 = timeunit_conversion(stat_unit, timemul1);
        int timemul2;
        bool is2 = timeunit_conversion(vstat_unit, timemul2);
        if (is1 && !is2) return -1;
        if (is2 && !is1) return 1;
        if (int res = stat_len * timemul1 - vstat_len * timemul2) return res;
    }

    return 0;
}

std::ostream& Timedef::writeToOstream(std::ostream& o) const
{
    using namespace std;
    timerange::TimedefUnit step_unit, stat_unit;
    unsigned step_len, stat_type, stat_len;
    get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);

    utils::SaveIOState sios(o);

    o << formatStyle(Style::TIMEDEF) << "("
      << setfill('0') << internal;

    timeunit_output(step_unit, step_len, o);

    if (stat_type != 255)
    {
        o << ", " << stat_type;
        if (stat_unit != UNIT_MISSING)
        {
            o << ", ";
            timeunit_output(stat_unit, stat_len, o);
        }
    }
    o << ")";

    return o;
}

void Timedef::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    timerange::TimedefUnit step_unit, stat_unit;
    unsigned step_len, stat_type, stat_len;
    get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);
    e.add(keys.type_style, formatStyle(Style::TIMEDEF));
    e.add(keys.timerange_step_len, step_len);
    e.add(keys.timerange_step_unit, step_unit);
    if (stat_type != 255)
    {
        e.add(keys.timerange_stat_type, stat_type);
        if (stat_unit != UNIT_MISSING)
        {
            e.add(keys.timerange_stat_len, stat_len);
            e.add(keys.timerange_stat_unit, stat_unit);
        }
    }
}

std::string Timedef::exactQuery() const
{
    timerange::TimedefUnit step_unit, stat_unit;
    unsigned step_len, stat_type, stat_len;
    get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);

    std::stringstream o;
    o << formatStyle(Style::TIMEDEF) << ",";
    if (step_unit == 255)
        o << "-,";
    else
        o << step_len << timeunit_suffix(step_unit) << ",";
    if (stat_type == 255)
        o << "-";
    else
    {
        o << stat_type << ",";
        if (stat_unit == 255)
            o << "-";
        else
            o << stat_len << timeunit_suffix(stat_unit);
    }
    return o.str();
}

bool Timedef::get_forecast_step(int& step, bool& is_seconds) const
{
    timerange::TimedefUnit step_unit, stat_unit;
    unsigned step_len, stat_type, stat_len;
    get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);

    if (step_unit == UNIT_MISSING) return false;

    int timemul;
    is_seconds = timeunit_conversion(step_unit, timemul);
    step = step_len * timemul;
    return true;
}

int Timedef::get_proc_type() const
{
    timerange::TimedefUnit step_unit, stat_unit;
    unsigned step_len, stat_type, stat_len;
    get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);

    if (stat_type == 255)
        return -1;
    return stat_type;
}

bool Timedef::get_proc_duration(int& duration, bool& is_seconds) const
{
    timerange::TimedefUnit step_unit, stat_unit;
    unsigned step_len, stat_type, stat_len;
    get_Timedef(step_unit, step_len, stat_type, stat_unit, stat_len);

    if (stat_type == 255 || stat_unit == UNIT_MISSING) return false;

    int timemul;
    is_seconds = timeunit_conversion(stat_unit, timemul);
    duration = stat_len * timemul;
    return true;
}

std::unique_ptr<Reftime> Timedef::validity_time_to_emission_time(const reftime::Position& src) const
{
    // Compute our forecast step in seconds
    int secs = 0;
    bool is_secs = false;
    bool can_compute = get_forecast_step(secs, is_secs);
    if (!can_compute)
        throw_consistency_error("converting validity time to emission time", "cannot compute the forecast step");
    if (!is_secs)
        throw_consistency_error("converting validity time to emission time", "timedef has a step that cannot be converted to seconds");

    // Compute the new time
    Time new_time(src.get_Position());
    new_time.se -= secs;
    new_time.normalise();

    // Return it
    return Reftime::createPosition(new_time);
}


bool Timedef::timeunit_conversion(TimedefUnit unit, int& timemul)
{
    bool is_seconds = true;
    timemul = 1;
    switch (unit)
    {
        case UNIT_MINUTE: timemul = 60; break;
        case UNIT_HOUR: timemul = 3600; break;
        case UNIT_DAY: timemul = 3600*24; break;
        case UNIT_MONTH: timemul = 1; is_seconds = false; break;
        case UNIT_YEAR: timemul = 12; is_seconds = false; break;
        case UNIT_DECADE: timemul = 120; is_seconds = false; break;
        case UNIT_NORMAL: timemul = 12*30; is_seconds = false; break;
        case UNIT_CENTURY: timemul = 12*100; is_seconds = false; break;
        case UNIT_3HOURS: timemul = 3600*3; break;
        case UNIT_6HOURS: timemul = 3600*6; break;
        case UNIT_12HOURS: timemul = 3600*12; break;
        case UNIT_SECOND: timemul = 1; break;
        case UNIT_MISSING:
            throw_consistency_error("normalising time", "time unit is missing (255)");
        default:
        {
            std::stringstream ss;
            ss << "cannot normalise time: time unit is unknown (" << (int)unit << ")";
            throw std::runtime_error(ss.str());
        }
    }
    return is_seconds;
}

void Timedef::timeunit_output(TimedefUnit unit, uint32_t val, std::ostream& out)
{
    out << val << timeunit_suffix(unit);
}

bool Timedef::timeunit_parse_suffix(const char*& str, TimedefUnit& unit)
{
    if (!*str) return false;

    switch (*str)
    {
        case 'm':
            ++str;
            switch (*str)
            {
                case 'o': ++str; unit = UNIT_MONTH; break;
                default: unit = UNIT_MINUTE; break;
            }
            return true;
        case 'h':
            ++str;
            switch (*str)
            {
                case '3': ++str; unit = UNIT_3HOURS; break;
                case '6': ++str; unit = UNIT_6HOURS; break;
                case '1':
                    if (*(str+1) && *(str+1) == '2')
                    {
                        str += 2;
                        unit = UNIT_12HOURS;
                        break;
                    }
                    // Falls through
                default: unit = UNIT_HOUR; break;
            }
            return true;
        case 'd':
            ++str;
            switch (*str)
            {
                case 'e': ++str; unit = UNIT_DECADE; break;
                default: unit = UNIT_DAY; break;
            }
            return true;
        case 'y': ++str; unit = UNIT_YEAR; return true;
        case 'n':
            if (*(str+1) && *(str+1) == 'o')
            {
                str += 2;
                unit = UNIT_NORMAL;
                return true;
            }
            return false;
        case 'c':
            if (*(str+1) && *(str+1) == 'e')
            {
                str += 2;
                unit = UNIT_CENTURY;
                return true;
            }
            return false;
        case 's': ++str; unit = UNIT_SECOND; return true;
    }
    return false;
}

bool Timedef::timeunit_parse(const char*& str, TimedefUnit& unit, uint32_t& val)
{
    if (!*str)
        return false;

    skipCommasAndSpaces(str);

    if (str[0] == '-')
    {
        unit = UNIT_MISSING;
        val = 0;

        skipCommasAndSpaces(str);
        return true;
    }

    char* endptr;
    val = strtoul(str, &endptr, 10);
    if (endptr == str)
        return false;

    str = endptr;

    if (!timeunit_parse_suffix(str, unit))
    {
        if (val == 0)
        {
            unit = UNIT_SECOND;
            return true;
        }
        return false;
    }

    skipCommasAndSpaces(str);
    return true;
}

const char* Timedef::timeunit_suffix(TimedefUnit unit)
{
    switch (unit)
    {
        case UNIT_MINUTE: return "m";
        case UNIT_HOUR: return "h";
        case UNIT_DAY: return "d";
        case UNIT_MONTH: return "mo";
        case UNIT_YEAR: return "y";
        case UNIT_DECADE: return "de";
        case UNIT_NORMAL: return "no";
        case UNIT_CENTURY: return "ce";
        case UNIT_3HOURS: return "h3";
        case UNIT_6HOURS: return "h6";
        case UNIT_12HOURS: return "h12";
        case UNIT_SECOND: return "s";
        case UNIT_MISSING:
            throw_consistency_error("finding time unit suffix", "time unit is missing (255)");
        default:
        {
            std::stringstream ss;
            ss << "cannot find find time unit suffix: time unit is unknown (" << (int)unit << ")";
            throw std::runtime_error(ss.str());
        }
    }
}


/*
 * BUFR
 */

bool BUFR::equals(const Type& o) const
{
    const BUFR* v = dynamic_cast<const BUFR*>(&o);
    if (!v) return false;

    unsigned un, va, vun, vva;
    get_BUFR(un, va);
    v->get_BUFR(vun, vva);

    if (is_seconds(un) != v->is_seconds(vun)) return false;
    if (is_seconds(un))
        return seconds(un, va) == v->seconds(vun, vva);
    else
        return months(un, va) == v->months(vun, vva);
}

int BUFR::compare_local(const BUFR& o) const
{
    unsigned un, va, vun, vva;
    get_BUFR(un, va);
    o.get_BUFR(vun, vva);

    if (int res = (is_seconds(un) ? 0 : 1) - (o.is_seconds(vun) ? 0 : 1)) return res;
    if (is_seconds(un))
        return seconds(un, va) - o.seconds(vun, vva);
    else
        return months(un, va) - o.months(vun, vva);
}

std::ostream& BUFR::writeToOstream(std::ostream& o) const
{
    unsigned un, va;
    get_BUFR(un, va);

    utils::SaveIOState sios(o);
    std::string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)un);
    o << formatStyle(Style::BUFR) << "(";
    if (va != 0) o << va << suffix;
    return o << ")";
}

void BUFR::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    unsigned un, va;
    get_BUFR(un, va);
    e.add(keys.type_style, formatStyle(Style::BUFR));
    e.add(keys.timerange_unit, un);
    e.add(keys.timerange_value, va);
}

std::string BUFR::exactQuery() const
{
    unsigned un, va;
    get_BUFR(un, va);

    std::stringstream o;
    std::string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)un);
    o << formatStyle(Style::BUFR) << "," << va << suffix;
    return o.str();
}

bool BUFR::get_forecast_step(int& step, bool& is_seconds) const
{
    unsigned un, va;
    get_BUFR(un, va);

    is_seconds = this->is_seconds(un);
    if (is_seconds)
        step = seconds(un, va);
    else
        step = months(un, va);
    return true;
}

int BUFR::get_proc_type() const
{
    return -1;
}

bool BUFR::get_proc_duration(int& duration, bool& is_seconds) const
{
    return false;
}

bool BUFR::is_seconds(unsigned unit)
{
    switch ((t_enum_GRIB_TIMEUNIT)unit)
    {
        case GRIB_TIMEUNIT_UNKNOWN:
			throw_consistency_error("normalising TimeRange", "time unit is UNKNOWN (-1)");
		case GRIB_TIMEUNIT_MINUTE:
		case GRIB_TIMEUNIT_HOUR:
		case GRIB_TIMEUNIT_DAY:
		case GRIB_TIMEUNIT_HOURS3:
		case GRIB_TIMEUNIT_HOURS6:
		case GRIB_TIMEUNIT_HOURS12:
		case GRIB_TIMEUNIT_SECOND:
			return true;
		case GRIB_TIMEUNIT_MONTH:
		case GRIB_TIMEUNIT_YEAR:
		case GRIB_TIMEUNIT_DECADE:
		case GRIB_TIMEUNIT_NORMAL:
		case GRIB_TIMEUNIT_CENTURY:
			return false;
        default:
        {
            std::stringstream ss;
            ss << "cannot normalise TimeRange: time unit is unknown (" << unit << ")";
            throw std::runtime_error(ss.str());
        }
    }
}

unsigned BUFR::seconds(unsigned unit, unsigned value)
{
    if (value == 0) return 0;

    switch ((t_enum_GRIB_TIMEUNIT)unit)
    {
        case GRIB_TIMEUNIT_UNKNOWN:
            throw_consistency_error("normalising TimeRange", "time unit is UNKNOWN (-1)");
        case GRIB_TIMEUNIT_MINUTE: return value * 60;
        case GRIB_TIMEUNIT_HOUR: return value * 3600;
        case GRIB_TIMEUNIT_DAY: return value * 3600*24;
        case GRIB_TIMEUNIT_HOURS3: return value * 3600*3;
        case GRIB_TIMEUNIT_HOURS6: return value * 3600*6;
        case GRIB_TIMEUNIT_HOURS12: return value * 3600*12;
        case GRIB_TIMEUNIT_SECOND: return value * 1;
        default:
        {
            std::stringstream ss;
            ss << "cannot normalise TimeRange: time unit (" << unit << ") does not convert to seconds";
            throw std::runtime_error(ss.str());
        }
    }
}

unsigned BUFR::months(unsigned unit, unsigned value)
{
    if (value == 0) return 0;

    switch ((t_enum_GRIB_TIMEUNIT)unit)
    {
        case GRIB_TIMEUNIT_UNKNOWN:
            throw_consistency_error("normalising TimeRange", "time unit is UNKNOWN (-1)");
        case GRIB_TIMEUNIT_MONTH: return value * 1;
        case GRIB_TIMEUNIT_YEAR: return value * 12;
        case GRIB_TIMEUNIT_DECADE: return value * 120;
        case GRIB_TIMEUNIT_NORMAL: return value * 12*30;
        case GRIB_TIMEUNIT_CENTURY: return value * 12*100;
        default:
        {
            std::stringstream ss;
            ss << "cannot normalise TimeRange: time unit (" << unit << ") does not convert to months";
            throw std::runtime_error(ss.str());
        }
    }
}

}

void Timerange::init()
{
    MetadataType::register_type<Timerange>();
}

}
}
