/*
 * matcher/timerange - Timerange matcher
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/matcher/timerange.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>

using namespace std;
using namespace wibble;
using namespace arki::types;

namespace arki {
namespace matcher {

template<typename INT>
static bool parseValueWithUnit(const std::string& str, INT& val, types::timerange::GRIB1Unit& u)
{
    if (str.empty()) return false;

    const char* s = str.c_str();
    char* e = NULL;
    long int value = strtol(s, &e, 10);
    if (value == 0)
    {
        val = 0;
        return true;
    }
    string unit = str.substr(e-s);
    if (unit == "s")
    {
        u = types::timerange::SECOND;
        val = value;
        return true;
    }
    else if (unit == "m")
    {
        u = types::timerange::SECOND;
        val = value * 60;
        return true;
    }
    else if (unit == "h")
    {
        u = types::timerange::SECOND;
        val = value * 3600;
        return true;
    }
    else if (unit == "d")
    {
        u = types::timerange::SECOND;
        val = value * 3600 * 24;
        return true;
    }
    else if (unit == "mo")
    {
        u = types::timerange::MONTH;
        val = value;
        return true;
    }
    else if (unit == "y")
    {
        u = types::timerange::MONTH;
        val = value * 12;
        return true;
    } else {
        throw wibble::exception::Consistency(
                "parsing 'timerange' match expression '" + str + "'",
                "unknown time suffix '" + unit + "': valid ones are 's', 'm', 'h', 'd', 'mo', 'y'");
    }
}

static int parseTimedefValueWithUnit(const std::string& str, bool& is_second)
{
    using namespace types::timerange;

    const char* s = str.c_str();
    timerange::TimedefUnit unit;
    uint32_t val;
    if (!Timedef::timeunit_parse(s, unit, val) || *s)
        throw wibble::exception::Consistency(
                "parsing 'timerange' match expression",
                "cannot parse time '" + str + "'");

    int timemul;
    is_second = Timedef::timeunit_conversion(unit, timemul);
    return val * timemul;
}

std::string MatchTimerange::name() const { return "timerange"; }

MatchTimerangeGRIB1::MatchTimerangeGRIB1(const std::string& pattern)
    : unit(types::timerange::SECOND),
      has_ptype(false), has_p1(false), has_p2(false),
      ptype(0), p1(0), p2(0)
{
    OptionalCommaList args(pattern);
    const types::timerange::GRIB1Unit missingUnit = (types::timerange::GRIB1Unit)-1;

    if (!args.empty())
    {
        types::timerange::GRIB1Unit first = missingUnit;
        types::timerange::GRIB1Unit second = missingUnit;
        if (!args[0].empty())
        {
            has_ptype = true;
            ptype = strtoul(args[0].c_str(), NULL, 10);
        } else
            has_ptype = false;
        if (args.size() == 1)
            return;
        has_p1 = parseValueWithUnit(args[1], p1, first);
		if (args.size() == 2)
		{
			if (first == missingUnit)
				unit = types::timerange::SECOND;
			else
				unit = first;
			return;
		}
        has_p2 = parseValueWithUnit(args[2], p2, second);

		// If first or second units haven't been set, use the other as default
		if (first == missingUnit)
			unit = second;
		else if (second == missingUnit)
			unit = first;
		// Ensure that they are the same
		else if (first == second)
			unit = first;
		else
			throw wibble::exception::Consistency(
					"parsing 'timerange' match expression",
					"the two time values '" + args[1] + "' and '" + args[2] + "' have different units");

        // If both are unset (value is zero), then default to seconds
        if (unit == missingUnit)
            unit = types::timerange::SECOND;
    }
}

bool MatchTimerangeGRIB1::matchItem(const Type& o) const
{
    const types::timerange::GRIB1* v = dynamic_cast<const types::timerange::GRIB1*>(&o);
    if (!v) return false;
    int mtype, mp1, mp2;
    types::timerange::GRIB1Unit munit;
    bool use_p1, use_p2;
    v->getNormalised(mtype, munit, mp1, mp2, use_p1, use_p2);
    // FIXME: here FAILS for GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2:
    // normalisation does something else than reduction to months or seconds in
    // that case. GRIB1 match should in fact be deprecated in favour of
    // timedef. Even better, GRIB1 timeranges should just be scanned as
    // timedefs

    if (has_ptype && ptype != mtype)
        return false;
    if (has_p1 && use_p1 && p1 != mp1)
        return false;
    if (has_p2 && use_p2 && p2 != mp2)
        return false;
    if (unit != munit)
    {
        // If the units are different, we fail unless we match zeros
        if (has_p1 && use_p1 && p1 != 0)
            return false;
        if (has_p2 && use_p2 && p2 != 0)
            return false;
    }
    return true;
}

std::string MatchTimerangeGRIB1::toString() const
{
    CommaJoiner res;
    res.add("GRIB1");
    bool use_p1 = true, use_p2 = true;
    if (has_ptype)
    {
        res.add(ptype);
        types::timerange::GRIB1::arg_significance(ptype, use_p1, use_p2);
    }
    else
        res.addUndef();

    const char* u = "s";
    switch (unit)
    {
        case types::timerange::SECOND: u = "s"; break;
        case types::timerange::MONTH: u = "mo"; break;
    }

    if (has_p1 && use_p1)
        res.add(str::fmtf("%d%s", p1, u));
    else
        res.addUndef();

    if (has_p2 && use_p2)
        res.add(str::fmtf("%d%s", p2, u));
    else
        res.addUndef();

    return res.join();
}

MatchTimerangeGRIB2::MatchTimerangeGRIB2(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	type = args.getInt(0, -1);
	unit = args.getInt(1, -1);
	p1 = args.getInt(2, -1);
	p2 = args.getInt(3, -1);
}

bool MatchTimerangeGRIB2::matchItem(const Type& o) const
{
    const types::timerange::GRIB2* v = dynamic_cast<const types::timerange::GRIB2*>(&o);
    if (!v) return false;
	if (type != -1 && (unsigned)type != v->type()) return false;
	if (unit != -1 && (unsigned)unit != v->unit()) return false;
	if (p1 >= 0 && p1 != v->p1()) return false;
	if (p2 >= 0 && p2 != v->p2()) return false;
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


MatchTimerangeBUFR::MatchTimerangeBUFR(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	has_forecast = !args.empty();
	if (has_forecast)
	{
        types::timerange::GRIB1Unit unit;
        has_forecast = parseValueWithUnit(args[0], value, unit);
        is_seconds = unit == types::timerange::SECOND;
	} else {
		value = 0;
		is_seconds = true;
	}
}

bool MatchTimerangeBUFR::matchItem(const Type& o) const
{
    const types::timerange::BUFR* v = dynamic_cast<const types::timerange::BUFR*>(&o);
    if (!v) return false;
	if (!has_forecast) return true;
	if (value == 0) return v->value() == 0;
	if (is_seconds != v->is_seconds()) return false;
	if (is_seconds)
		return value == v->seconds();
	else
		return value == v->months();
}

std::string MatchTimerangeBUFR::toString() const
{
	if (!has_forecast) return "BUFR";
	return str::fmtf("BUFR,%u%s", value, is_seconds ? "s" : "mo");
}

MatchTimerangeTimedef::MatchTimerangeTimedef(const std::string& pattern)
    : has_step(false), has_proc_type(false), has_proc_duration(false)
{
    OptionalCommaList args(pattern);

    // Step match
    if (args.has(0))
    {
        has_step = true;
        if (args[0] == "-")
        {
            step = -1;
            step_is_seconds = true;
        } else
            step = parseTimedefValueWithUnit(args[0], step_is_seconds);
    }

    // Statistical processing type
    if (args.has(1))
    {
        has_proc_type = true;
        if (args[1] == "-")
        {
            proc_type = -1;
            proc_duration = 0;
            return;
        }
        proc_type = args.getInt(1, -1);
    }

    // Statistical processing duration
    if (args.has(2))
    {
        has_proc_duration = true;
        if (args[2] == "-")
        {
            proc_duration = -1;
            proc_duration_is_seconds = true;
        } else
            proc_duration = parseTimedefValueWithUnit(args[2], proc_duration_is_seconds);
    }
}

bool MatchTimerangeTimedef::matchItem(const Type& o) const
{
    const types::Timerange* v = dynamic_cast<const types::Timerange*>(&o);
    if (!v) return false;

    if (has_step)
    {
        int v_step;
        bool v_is_seconds;
        if (!v->get_forecast_step(v_step, v_is_seconds))
            return step == -1;
        if (step != v_step || step_is_seconds != v_is_seconds)
            return false;
    }

    if (has_proc_type)
    {
        if (proc_type != v->get_proc_type())
            return false;
    }

    if (has_proc_duration)
    {
        int v_duration;
        bool v_is_seconds;
        if (!v->get_proc_duration(v_duration, v_is_seconds))
            return proc_duration == -1;
        if (proc_duration != v_duration || proc_duration_is_seconds != v_is_seconds)
            return false;
    }
    return true;
}

std::string MatchTimerangeTimedef::toString() const
{
    CommaJoiner res;
    res.add("Timedef");
    if (has_step)
    {
        if (step == -1)
        {
            res.add("-");
        } else {
            if (step_is_seconds)
                res.add(str::fmtf("%ds", step));
            else
                res.add(str::fmtf("%dmo", step));
        }
    } else
        res.addUndef();

    if (has_proc_type)
    {
        if (proc_type == -1)
            res.add("-");
        else
            res.add(proc_type);
    } else
        res.addUndef();

    if (has_proc_duration)
    {
        if (proc_duration == -1)
            res.add("-");
        else if (proc_duration_is_seconds)
            res.add(str::fmtf("%ds", proc_duration));
        else
            res.add(str::fmtf("%dmo", proc_duration));
    }

    return res.join();
}

MatchTimerange* MatchTimerange::parse(const std::string& pattern)
{
	size_t beg = 0;
	size_t pos = pattern.find(',', beg);
	string name;
	string rest;
	if (pos == string::npos)
		name = str::trim(pattern.substr(beg));
	else {
		name = str::trim(pattern.substr(beg, pos-beg));
		rest = pattern.substr(pos+1);
	}

    switch (types::Timerange::parseStyle(name))
    {
        case types::Timerange::GRIB1: return new MatchTimerangeGRIB1(rest);
        case types::Timerange::GRIB2: return new MatchTimerangeGRIB2(rest);
        case types::Timerange::TIMEDEF: return new MatchTimerangeTimedef(rest);
        case types::Timerange::BUFR: return new MatchTimerangeBUFR(rest);
        default:
                                     throw wibble::exception::Consistency("parsing type of timerange to match", "unsupported timerange style: " + name);
    }
}

void MatchTimerange::init()
{
    Matcher::register_matcher("timerange", types::TYPE_TIMERANGE, (MatcherType::subexpr_parser)MatchTimerange::parse);
}

}
}

// vim:set ts=4 sw=4:
