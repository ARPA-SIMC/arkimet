/*
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "tests.h"
#include "reftime.h"
#include <arki/tests/lua.h>
#include <arki/matcher.h>
#include <wibble/string.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_reftime_shar {
};
TESTGRP(arki_types_reftime);

// Check Position
template<> template<>
void to::test<1>()
{
    arki::tests::TestGenericType t("reftime", "2015-01-02T03:04:05Z");
    t.lower.push_back("2014-01-01T00:00:00");
    t.higher.push_back("2015-01-03T00:00:00");
    // Period sort later than Position
    t.higher.push_back("2014-01-01T00:00:00 to 2014-01-31T00:00:00");
    t.exact_query = "=2015-01-02T03:04:05Z";
    wassert(t);

    auto_ptr<Reftime> o = Reftime::createPosition(Time(2007, 6, 5, 4, 3, 2));
    wassert(actual(o).is_reftime_position({2007, 6, 5, 4, 3, 2}));

    wassert(actual(o) == Reftime::createPosition(Time(2007, 6, 5, 4, 3, 2)));
    wassert(actual(o) != Reftime::createPosition(Time(2007, 6, 5, 4, 3, 1)));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check Period
template<> template<>
void to::test<2>()
{
    arki::tests::TestGenericType t("reftime", "2015-01-02T00:00:00Z to 2015-01-03T00:00:00Z");
    t.lower.push_back("2015-01-01T00:00:00");
    t.lower.push_back("2015-01-10T00:00:00");
    t.lower.push_back("2015-01-01T00:00:00 to 2015-01-03T12:00:00");
    t.higher.push_back("2015-01-02T12:00:00 to 2015-01-31T00:00:00");
//#warning This does not look like a query to match a period
//    t.exact_query = "=2007-06-05T04:03:02Z";
    wassert(t);

    auto_ptr<Reftime> o = Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3));
    wassert(actual(o).is_reftime_period({2007, 6, 5, 4, 3, 2}, {2008, 7, 6, 5, 4, 3}));

    wassert(actual(o) == Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3)));
    wassert(actual(o) != Reftime::createPeriod(Time(2007, 6, 5, 4, 3, 3), Time(2008, 7, 6, 5, 4, 2)));

    // Test encoding/decoding
    wassert(actual(o).serializes());
}

// Check collector
template<> template<>
void to::test<3>()
{
    reftime::Collector c;
    Time t1(2007, 6, 5, 4, 3, 2);
    Time t2(2008, 7, 6, 5, 4, 3);
    Time t3(2007, 7, 6, 5, 4, 3);
    Time t4(2009, 8, 7, 6, 5, 4);
    Time t5(2009, 7, 6, 5, 4, 3);
    Time t6(2010, 9, 8, 7, 6, 5);

    // Merge with position
    c.merge(reftime::Position(t1));
    wassert(actual(c.begin) == t1);
    wassert(actual(c.end.isValid()).isfalse());

    // Merge with a second position
    c.merge(reftime::Position(t2));
    wassert(actual(c.begin) == t1);
    wassert(actual(c.end) == t2);

    // Merge with a period
    c.merge(reftime::Period(t3, t4));
    wassert(actual(c.begin) == t1);
    wassert(actual(c.end) == t4);

    // Merge with another collector
    reftime::Collector c1;
    c1.merge(reftime::Period(t5, t6));
    wassert(actual(c1.begin) == t5);
    wassert(actual(c1.end) == t6);

    c.merge(c1);
    wassert(actual(c.begin) == t1);
    wassert(actual(c.end) == t6);
}

// Test Lua functions
template<> template<>
void to::test<4>()
{
#ifdef HAVE_LUA
    auto_ptr<Reftime> o = Reftime::createPosition(Time(2007, 6, 5, 4, 3, 2));

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'POSITION' then return 'style is '..o.style..' instead of POSITION' end \n"
		"  t = o.time \n"
		"  if t.year ~= 2007 then return 't.year is '..t.year..' instead of 2007' end \n"
		"  if t.month ~= 6 then return 't.month is '..t.month..' instead of 6' end \n"
		"  if t.day ~= 5 then return 't.day is '..t.day..' instead of 5' end \n"
		"  if t.hour ~= 4 then return 't.hour is '..t.hour..' instead of 4' end \n"
		"  if t.minute ~= 3 then return 't.minute is '..t.minute..' instead of 3' end \n"
		"  if t.second ~= 2 then return 't.second is '..t.second..' instead of 2' end \n"
		"  if tostring(o) ~= '2007-06-05T04:03:02Z' then return 'tostring gave '..tostring(o)..' instead of 2007-06-05T04:03:02Z' end \n"
		"  o1 = arki_reftime.position(arki_time.time(2007, 6, 5, 4, 3, 2))\n"
		"  if o ~= o1 then return 'new reftime is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

    test.pusharg(*o);
    wassert(actual(test.run()) == "");
#endif
}

// Reproduce bugs
template<> template<>
void to::test<5>()
{
    auto_ptr<Type> decoded = decodeString(TYPE_REFTIME, "2005-12-01T18:00:00Z");
    wassert(actual(wibble::str::fmt(*decoded)) == "2005-12-01T18:00:00Z");
}

}

// vim:set ts=4 sw=4:
