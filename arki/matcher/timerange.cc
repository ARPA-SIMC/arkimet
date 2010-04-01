/*
 * matcher/timerange - Timerange matcher
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

#include <arki/matcher/timerange.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace matcher {

std::string MatchTimerange::name() const { return "timerange"; }

MatchTimerangeGRIB1::MatchTimerangeGRIB1(const std::string& pattern)
	: matchType(false), matchBody(false), unit(types::timerange::GRIB1::SECOND), ptype(0), p1(0), p2(0)
{
	OptionalCommaList args(pattern);
	const types::timerange::GRIB1::Unit missingUnit = (types::timerange::GRIB1::Unit)-1;

	if (!args.empty())
	{
		types::timerange::GRIB1::Unit first = missingUnit;
		types::timerange::GRIB1::Unit second = missingUnit;
		matchType = true;
		ptype = strtoul(args[0].c_str(), NULL, 10);
		if (args.size() == 1)
			return;
		matchBody = true;
		p1 = parseInterval(args[1], first);
		if (args.size() == 2)
		{
			if (first == missingUnit)
				unit = types::timerange::GRIB1::SECOND;
			else
				unit = first;
			return;
		}
		p2 = parseInterval(args[2], second);

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
			unit = types::timerange::GRIB1::SECOND;
	}
}

bool MatchTimerangeGRIB1::matchItem(const Item<>& o) const
{
	const types::timerange::GRIB1* v = dynamic_cast<const types::timerange::GRIB1*>(o.ptr());
	if (!v) return false;

	if (matchType)
	{
		int mtype, mp1, mp2;
		types::timerange::GRIB1::Unit munit;
		v->getNormalised(mtype, munit, mp1, mp2);
		if (ptype != mtype)
			return false;
		if (matchBody)
		{
			if (p1 != mp1 || p2 != mp2)
				return false;
			if (unit != munit && (p1 != 0 || p2 != 0))
				return false;
		}
	}
	return true;
}

std::string MatchTimerangeGRIB1::toString() const
{
	string res = "GRIB1";
	if (matchType)
		res += ","+str::fmt(ptype);
	if (matchBody)
	{
		const char* u = "s";
		switch (unit)
		{
			case types::timerange::GRIB1::SECOND: u = "s"; break;
			case types::timerange::GRIB1::MONTH: u = "mo"; break;
		}
		res += "," + str::fmt(p1) + u + "," + str::fmt(p2) + u;
	}
	return res;
}

int MatchTimerangeGRIB1::parseInterval(const std::string& str, types::timerange::GRIB1::Unit& u)
{
	const char* s = str.c_str();
	char* e = NULL;
	long int value = strtol(s, &e, 10);
	if (value == 0)
		return 0;
	string unit = str.substr(e-s);
	if (unit == "s")
	{
		u = types::timerange::GRIB1::SECOND;
		return value;
	}
	else if (unit == "m")
	{
		u = types::timerange::GRIB1::SECOND;
		return value * 60;
	}
	else if (unit == "h")
	{
		u = types::timerange::GRIB1::SECOND;
		return value * 3600;
	}
	else if (unit == "d")
	{
		u = types::timerange::GRIB1::SECOND;
		return value * 3600 * 24;
	}
	else if (unit == "mo")
	{
		u = types::timerange::GRIB1::MONTH;
		return value;
	}
	else if (unit == "y")
	{
		u = types::timerange::GRIB1::MONTH;
		return value * 12;
	} else {
		throw wibble::exception::Consistency(
				"parsing 'timerange' match expression '" + str + "'",
				"unknown time suffix '" + unit + "': valid ones are 's', 'm', 'h', 'd', 'mo', 'y'");
	}
}


MatchTimerangeGRIB2::MatchTimerangeGRIB2(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	type = args.getInt(0, -1);
	unit = args.getInt(1, -1);
	p1 = args.getInt(2, -1);
	p2 = args.getInt(3, -1);
}

bool MatchTimerangeGRIB2::matchItem(const Item<>& o) const
{
	const types::timerange::GRIB2* v = dynamic_cast<const types::timerange::GRIB2*>(o.ptr());
	if (!v) return false;
	if (type != -1 && (unsigned)type != v->type()) return false;
	if (unit != -1 && (unsigned)unit != v->unit()) return false;
	if (p1 >= 0 && (unsigned)p1 != v->p1()) return false;
	if (p2 >= 0 && (unsigned)p2 != v->p2()) return false;
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
		default:
			throw wibble::exception::Consistency("parsing type of timerange to match", "unsupported timerange style: " + name);
	}
}

MatcherType timerange("timerange", types::TYPE_TIMERANGE, (MatcherType::subexpr_parser)MatchTimerange::parse);

}
}

// vim:set ts=4 sw=4:
