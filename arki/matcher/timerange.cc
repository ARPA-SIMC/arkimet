#include "timerange.h"
#include "arki/exceptions.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace matcher {

template<typename INT>
static Optional<INT> parseValueWithUnit(const std::string& str, types::timerange::GRIB1Unit& u)
{
    if (str.empty()) return Optional<INT>();

    const char* s = str.c_str();
    char* e = NULL;
    long int value = strtol(s, &e, 10);
    if (value == 0)
        return 0;
    std::string unit = str.substr(e - s);
    if (unit == "s")
    {
        u = types::timerange::SECOND;
        return value;
    }
    else if (unit == "m")
    {
        u = types::timerange::SECOND;
        return value * 60;
    }
    else if (unit == "h")
    {
        u = types::timerange::SECOND;
        return value * 3600;
    }
    else if (unit == "d")
    {
        u = types::timerange::SECOND;
        return value * 3600 * 24;
    }
    else if (unit == "mo")
    {
        u = types::timerange::MONTH;
        return value;
    }
    else if (unit == "y")
    {
        u = types::timerange::MONTH;
        return value * 12;
    } else {
        std::stringstream ss;
        ss << "cannot parse timerange match expression '" << str << "': unknown time suffix '" << unit << "': valid ones are 's', 'm', 'h', 'd', 'mo', 'y'";
        throw std::invalid_argument(ss.str());
    }
}

static int parseTimedefValueWithUnit(const std::string& str, bool& is_second)
{
    using namespace types::timerange;

    const char* s = str.c_str();
    timerange::TimedefUnit unit;
    uint32_t val;
    if (!Timedef::timeunit_parse(s, unit, val) || *s)
        throw_consistency_error(
                "parsing 'timerange' match expression",
                "cannot parse time '" + str + "'");

    int timemul;
    is_second = Timedef::timeunit_conversion(unit, timemul);
    return val * timemul;
}

std::string MatchTimerange::name() const { return "timerange"; }

MatchTimerangeGRIB1::MatchTimerangeGRIB1(types::timerange::GRIB1Unit unit, const Optional<int>& ptype, const Optional<int>& p1, const Optional<int>& p2)
    : unit(unit), ptype(ptype), p1(p1), p2(p2)
{
}

MatchTimerangeGRIB1::MatchTimerangeGRIB1(const std::string& pattern)
    : unit(types::timerange::SECOND)
{
    OptionalCommaList args(pattern);
    const types::timerange::GRIB1Unit missingUnit = (types::timerange::GRIB1Unit)-1;

    if (!args.empty())
    {
        types::timerange::GRIB1Unit first = missingUnit;
        types::timerange::GRIB1Unit second = missingUnit;
        if (!args[0].empty())
        {
            ptype.set(strtoul(args[0].c_str(), NULL, 10));
        } else
            ptype.unset();
        if (args.size() == 1)
            return;
        p1 = parseValueWithUnit<int>(args[1], first);
        if (args.size() == 2)
        {
            if (first == missingUnit)
                unit = types::timerange::SECOND;
            else
                unit = first;
            return;
        }
        p2 = parseValueWithUnit<int>(args[2], second);

		// If first or second units haven't been set, use the other as default
		if (first == missingUnit)
			unit = second;
		else if (second == missingUnit)
			unit = first;
		// Ensure that they are the same
		else if (first == second)
			unit = first;
		else
			throw_consistency_error(
					"parsing 'timerange' match expression",
					"the two time values '" + args[1] + "' and '" + args[2] + "' have different units");

        // If both are unset (value is zero), then default to seconds
        if (unit == missingUnit)
            unit = types::timerange::SECOND;
    }
}

MatchTimerangeGRIB1* MatchTimerangeGRIB1::clone() const
{
    return new MatchTimerangeGRIB1(unit, ptype, p1, p2);
}

bool MatchTimerangeGRIB1::match_data(int mtype, int munit, int mp1, int mp2, bool use_p1, bool use_p2) const
{
    // FIXME: here FAILS for GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2:
    // normalisation does something else than reduction to months or seconds in
    // that case. GRIB1 match should in fact be deprecated in favour of
    // timedef. Even better, GRIB1 timeranges should just be scanned as
    // timedefs

    if (!ptype.matches(mtype))
        return false;
    if (p1.present && use_p1 && p1.value != mp1)
        return false;
    if (p2.present && use_p2 && p2.value != mp2)
        return false;
    if (unit != munit)
    {
        // If the units are different, we fail unless we match zeros
        if (p1.present && use_p1 && p1.value != 0)
            return false;
        if (p2.present && use_p2 && p2.value != 0)
            return false;
    }
    return true;
}

bool MatchTimerangeGRIB1::matchItem(const Type& o) const
{
    const types::timerange::GRIB1* v = dynamic_cast<const types::timerange::GRIB1*>(&o);
    if (!v) return false;
    int mtype, mp1, mp2;
    types::timerange::GRIB1Unit munit;
    bool use_p1, use_p2;
    v->get_GRIB1_normalised(mtype, munit, mp1, mp2, use_p1, use_p2);
    return match_data(mtype, munit, mp1, mp2, use_p1, use_p2);
}

bool MatchTimerangeGRIB1::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_TIMERANGE) return false;
    if (size < 1) return false;
    if (Timerange::style(data, size) != timerange::Style::GRIB1) return false;
    int mtype, mp1, mp2;
    types::timerange::GRIB1Unit munit;
    bool use_p1, use_p2;
    Timerange::get_GRIB1_normalised(data, size, mtype, munit, mp1, mp2, use_p1, use_p2);
    return match_data(mtype, munit, mp1, mp2, use_p1, use_p2);
}

std::string MatchTimerangeGRIB1::toString() const
{
    CommaJoiner res;
    res.add("GRIB1");
    bool use_p1 = true, use_p2 = true;
    if (ptype.present)
    {
        res.add(ptype.value);
        types::timerange::GRIB1::arg_significance(ptype.value, use_p1, use_p2);
    }
    else
        res.addUndef();

    const char* u = "s";
    switch (unit)
    {
        case types::timerange::SECOND: u = "s"; break;
        case types::timerange::MONTH: u = "mo"; break;
    }

    if (p1.present && use_p1)
    {
        std::stringstream ss;
        ss << p1.value << u;
        res.add(ss.str());
    }
    else
        res.addUndef();

    if (p2.present && use_p2)
    {
        std::stringstream ss;
        ss << p2.value << u;
        res.add(ss.str());
    }
    else
        res.addUndef();

    return res.join();
}

MatchTimerangeGRIB2::MatchTimerangeGRIB2(int type, int unit, int p1, int p2)
    : type(type), unit(unit), p1(p1), p2(p2)
{
}

MatchTimerangeGRIB2::MatchTimerangeGRIB2(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	type = args.getInt(0, -1);
	unit = args.getInt(1, -1);
	p1 = args.getInt(2, -1);
	p2 = args.getInt(3, -1);
}

MatchTimerangeGRIB2* MatchTimerangeGRIB2::clone() const
{
    return new MatchTimerangeGRIB2(type, unit, p1, p2);
}

bool MatchTimerangeGRIB2::matchItem(const Type& o) const
{
    const types::timerange::GRIB2* v = dynamic_cast<const types::timerange::GRIB2*>(&o);
    if (!v) return false;
    unsigned vtype, vunit;
    signed long vp1, vp2;
    v->get_GRIB2(vtype, vunit, vp1, vp2);
    if (type != -1 && (unsigned)type != vtype) return false;
    if (unit != -1 && (unsigned)unit != vunit) return false;
    if (p1 >= 0 && p1 != vp1) return false;
    if (p2 >= 0 && p2 != vp2) return false;
    return true;
}

bool MatchTimerangeGRIB2::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_TIMERANGE) return false;
    if (size < 1) return false;
    if (Timerange::style(data, size) != timerange::Style::GRIB2) return false;
    unsigned vtype, vunit;
    signed long vp1, vp2;
    Timerange::get_GRIB2(data, size, vtype, vunit, vp1, vp2);
    if (type != -1 && (unsigned)type != vtype) return false;
    if (unit != -1 && (unsigned)unit != vunit) return false;
    if (p1 >= 0 && p1 != vp1) return false;
    if (p2 >= 0 && p2 != vp2) return false;
    return true;
}

std::string MatchTimerangeGRIB2::toString() const
{
	CommaJoiner res;
	res.add("GRIB2");
	if (type != -1) res.add(type); else res.addUndef();
	if (unit != -1) res.add(unit); else res.addUndef();
	if (p1 != -1) res.add(p1); else res.addUndef();
	if (p2 != -1) res.add(p2); else res.addUndef();
	return res.join();
}


MatchTimerangeBUFR::MatchTimerangeBUFR(const Optional<unsigned>& forecast, bool is_seconds)
    : forecast(forecast), is_seconds(is_seconds)
{
}

MatchTimerangeBUFR::MatchTimerangeBUFR(const std::string& pattern)
{
    OptionalCommaList args(pattern);
    if (args.empty())
        is_seconds = true;
    else
    {
        types::timerange::GRIB1Unit unit = types::timerange::SECOND;
        forecast = parseValueWithUnit<unsigned>(args[0], unit);
        is_seconds = unit == types::timerange::SECOND;
    }
}

MatchTimerangeBUFR* MatchTimerangeBUFR::clone() const
{
    return new MatchTimerangeBUFR(forecast, is_seconds);
}

bool MatchTimerangeBUFR::matchItem(const Type& o) const
{
    const types::timerange::BUFR* v = dynamic_cast<const types::timerange::BUFR*>(&o);
    if (!v) return false;
    unsigned vunit, vvalue;
    v->get_BUFR(vunit, vvalue);
    if (!forecast.present) return true;
    if (forecast.value == 0) return vvalue == 0;
    if (is_seconds != types::timerange::BUFR::is_seconds(vunit)) return false;
    if (is_seconds)
        return forecast.value == types::timerange::BUFR::seconds(vunit, vvalue);
    else
        return forecast.value == types::timerange::BUFR::months(vunit, vvalue);
}

bool MatchTimerangeBUFR::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_TIMERANGE) return false;
    if (size < 1) return false;
    if (Timerange::style(data, size) != timerange::Style::BUFR) return false;
    unsigned vunit, vvalue;
    Timerange::get_BUFR(data, size, vunit, vvalue);
    if (!forecast.present) return true;
    if (forecast.value == 0) return vvalue == 0;
    if (is_seconds != types::timerange::BUFR::is_seconds(vunit)) return false;
    if (is_seconds)
        return forecast.value == types::timerange::BUFR::seconds(vunit, vvalue);
    else
        return forecast.value == types::timerange::BUFR::months(vunit, vvalue);
}

std::string MatchTimerangeBUFR::toString() const
{
    if (!forecast.present) return "BUFR";
    char buf[32];
    snprintf(buf, 32, "BUFR,%u%s", forecast.value, is_seconds ? "s" : "mo");
    return buf;
}

MatchTimerangeTimedef::MatchTimerangeTimedef(const Optional<int>& step, bool step_is_seconds, const Optional<int>& proc_type, const Optional<int>& proc_duration, bool proc_duration_is_seconds)
    : step(step), step_is_seconds(step_is_seconds), proc_type(proc_type), proc_duration(proc_duration), proc_duration_is_seconds(proc_duration_is_seconds)
{
}

MatchTimerangeTimedef::MatchTimerangeTimedef(const std::string& pattern)
{
    OptionalCommaList args(pattern);

    // Step match
    if (args.has(0))
    {
        if (args[0] == "-")
        {
            step.set(-1);
            step_is_seconds = true;
        } else
            step.set(parseTimedefValueWithUnit(args[0], step_is_seconds));
    }

    // Statistical processing type
    if (args.has(1))
    {
        if (args[1] == "-")
        {
            proc_type.set(-1);
            return;
        }
        proc_type.set(args.getInt(1, -1));
    }

    // Statistical processing duration
    if (args.has(2))
    {
        if (args[2] == "-")
        {
            proc_duration.set(-1);
            proc_duration_is_seconds = true;
        } else
            proc_duration.set(parseTimedefValueWithUnit(args[2], proc_duration_is_seconds));
    }
}

MatchTimerangeTimedef* MatchTimerangeTimedef::clone() const
{
    return new MatchTimerangeTimedef(step, step_is_seconds, proc_type, proc_duration, proc_duration_is_seconds);
}

bool MatchTimerangeTimedef::matchItem(const Type& o) const
{
    const types::Timerange* v = dynamic_cast<const types::Timerange*>(&o);
    if (!v) return false;

    if (step.present)
    {
        int v_step;
        bool v_is_seconds;
        if (!v->get_forecast_step(v_step, v_is_seconds))
            return step.value == -1;
        if (step.value != v_step || step_is_seconds != v_is_seconds)
            return false;
    }

    if (!proc_type.matches(v->get_proc_type()))
        return false;

    if (proc_duration.present)
    {
        int v_duration;
        bool v_is_seconds;
        if (!v->get_proc_duration(v_duration, v_is_seconds))
            return proc_duration.value == -1;
        if (proc_duration.value != v_duration || proc_duration_is_seconds != v_is_seconds)
            return false;
    }
    return true;
}

std::string MatchTimerangeTimedef::toString() const
{
    CommaJoiner res;
    res.add("Timedef");
    if (step.present)
    {
        if (step.value == -1)
        {
            res.add_missing();
        } else {
            std::stringstream ss;
            ss << step.value;
            if (step_is_seconds)
                ss << "s";
            else
                ss << "mo";
            res.add(ss.str());
        }
    } else
        res.addUndef();

    if (proc_type.present)
    {
        if (proc_type.value == -1)
            res.add_missing();
        else
            res.add(proc_type.value);
    } else
        res.addUndef();

    if (proc_duration.present)
    {
        std::stringstream ss;
        if (proc_duration.value == -1)
            ss << "-";
        else if (proc_duration_is_seconds)
            ss << proc_duration.value << "s";
        else
            ss << proc_duration.value << "mo";
        res.add(ss.str());
    }

    return res.join();
}

Implementation* MatchTimerange::parse(const std::string& pattern)
{
    size_t beg = 0;
    size_t pos = pattern.find(',', beg);
    string name;
    string rest;
    if (pos == string::npos)
        name = str::strip(pattern.substr(beg));
    else {
        name = str::strip(pattern.substr(beg, pos-beg));
        rest = pattern.substr(pos+1);
    }

    switch (types::Timerange::parseStyle(name))
    {
        case types::Timerange::Style::GRIB1: return new MatchTimerangeGRIB1(rest);
        case types::Timerange::Style::GRIB2: return new MatchTimerangeGRIB2(rest);
        case types::Timerange::Style::TIMEDEF: return new MatchTimerangeTimedef(rest);
        case types::Timerange::Style::BUFR: return new MatchTimerangeBUFR(rest);
        default: throw std::invalid_argument("cannot parse type of timerange to match: unsupported timerange style: " + name);
    }
}

void MatchTimerange::init()
{
    MatcherType::register_matcher("timerange", TYPE_TIMERANGE, (MatcherType::subexpr_parser)MatchTimerange::parse);
}

}
}
