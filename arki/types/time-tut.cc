/*
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include "time.h"

#include <sstream>
#include <iostream>

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_time_shar {
};
TESTGRP(arki_types_time);

// Check 'now' time
template<> template<>
void to::test<1>()
{
    auto_ptr<Time> o = Time::createInvalid();
    wassert(actual(o).is_time(0, 0, 0, 0, 0, 0));
    wassert(actual(o->isValid()).isfalse());
    wassert(actual(o) == Time::createInvalid());
    wassert(actual(o) == Time::create(0, 0, 0, 0, 0, 0));
    wassert(actual(o) != Time::create(1789, 7, 14, 12, 0, 0));

    // Test serialisation to ISO-8601
    wassert(actual(o->toISO8601()) == "0000-00-00T00:00:00Z");

    // Test serialisation to SQL
    wassert(actual(o->toSQL()) == "0000-00-00 00:00:00");

    // Test deserialisation from ISO-8601
    wassert(actual(o) == Time::createFromISO8601(o->toISO8601()));

    // Test deserialisation from SQL
    wassert(actual(o) == Time::create_from_SQL(o->toSQL()));

    wassert(actual(o).serializes());
}

// Check an arbitrary time
template<> template<>
void to::test<2>()
{
    auto_ptr<Time> o = Time::create(1, 2, 3, 4, 5, 6);
    wassert(actual(o).is_time(1, 2, 3, 4, 5, 6));

    wassert(actual(o) == Time::create(1, 2, 3, 4, 5, 6));
    wassert(actual(o) != Time::create(1789, 7, 14, 12, 0, 0));

    // Test serialisation to ISO-8601
    wassert(actual(o->toISO8601()) == "0001-02-03T04:05:06Z");

    // Test serialisation to SQL
    wassert(actual(o->toSQL()) == "0001-02-03 04:05:06");

    // Test deserialisation from ISO-8601
    wassert(actual(o) == Time::createFromISO8601(o->toISO8601()));

    // Test deserialisation from SQL
    wassert(actual(o) == Time::create_from_SQL(o->toSQL()));

    wassert(actual(o).serializes());
}

// Check time differences
template<> template<>
void to::test<3>()
{
#if 0
    auto_ptr<Time> t = Time::createDifference(Time(2007, 6, 5, 4, 3, 2), Time(2006, 5, 4, 3, 2, 1));
    wasssert(actual(t) == Time::create(1, 1, 1, 1, 1, 1));

    t = Time::createDifference(Time(2007, 3, 1, 0, 0, 0), Time(2007, 2, 28, 0, 0, 0));
    wassert(actual(t) == Time::create(0, 0, 1, 0, 0, 0));

    t = Time::createDifference(create(2008, 3, 1, 0, 0, 0), create(2008, 2, 28, 0, 0, 0));
    wassert(actual(t) == Time::create(0, 0, 2, 0, 0, 0));

    t = Time::createDifference(create(2008, 3, 1, 0, 0, 0), create(2007, 12, 20, 0, 0, 0));
    wassert(actual(t) == Time::create(0, 2, 10, 0, 0, 0));

    t = Time::createDifference(create(2007, 2, 28, 0, 0, 0), create(2008, 3, 1, 0, 0, 0));
    wassert(actual(t) == Time::create(0, 0, 0, 0, 0, 0));
#endif
}

// Check various type operations
template<> template<>
void to::test<4>()
{
    tests::TestGenericType t("time", "2014-01-01T12:00:00Z");
    t.lower.push_back("0000-00-00T00:00:00Z");
    t.lower.push_back("1789-07-14T12:00:00Z");
    t.higher.push_back("2015-01-01T00:00:00Z");
    t.higher.push_back("2115-01-01T00:00:00Z");
    wassert(t);
}

// Test Lua functions
template<> template<>
void to::test<5>()
{
#ifdef HAVE_LUA
    auto_ptr<Time> o = Time::create(1, 2, 3, 4, 5, 6);

	tests::Lua test(
		"function test(o) \n"
		"  if o.year ~= 1 then return 'o.year is '..o.year..' instead of 1' end \n"
		"  if o.month ~= 2 then return 'o.month is '..o.month..' instead of 2' end \n"
		"  if o.day ~= 3 then return 'o.day is '..o.day..' instead of 3' end \n"
		"  if o.hour ~= 4 then return 'o.hour is '..o.hour..' instead of 4' end \n"
		"  if o.minute ~= 5 then return 'o.minute is '..o.minute..' instead of 5' end \n"
		"  if o.second ~= 6 then return 'o.second is '..o.second..' instead of 6' end \n"
		"  if tostring(o) ~= '0001-02-03T04:05:06Z' then return 'tostring gave '..tostring(o)..' instead of 0001-02-03T04:05:06Z' end \n"
		"  o1 = arki_time.time(1, 2, 3, 4, 5, 6)\n"
		"  if o ~= o1 then return 'new time is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"  o1 = arki_time.iso8601('1-2-3T4:5:6Z')\n"
		"  if o ~= o1 then return 'new time is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"  o1 = arki_time.sql('1-2-3 4:5:6')\n"
		"  if o ~= o1 then return 'new time is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"  o2 = arki_time.now()\n"
		"  if o2 <= o1 then return 'time now is '..tostring(o2)..' which is not later than '..tostring(o1) end\n"
		"end \n"
	);

	test.pusharg(*o);
    wassert(actual(test.run()) == "");
#endif
}

// Test Time::generate
template<> template<>
void to::test<6>()
{
    vector<Time> v = Time::generate(Time(2009, 1, 1, 0, 0, 0), Time(2009, 2, 1, 0, 0, 0), 3600);
    wassert(actual(v.size()) == 31u*24u);
}

// Test Time::range_overlaps
template<> template<>
void to::test<7>()
{
    auto_ptr<Time> topen;
    auto_ptr<Time> t2000(new Time(2000, 1, 1, 0, 0, 0));
    auto_ptr<Time> t2005(new Time(2005, 1, 1, 0, 0, 0));
    auto_ptr<Time> t2007(new Time(2007, 1, 1, 0, 0, 0));
    auto_ptr<Time> t2010(new Time(2010, 1, 1, 0, 0, 0));

    wassert(actual(Time::range_overlaps(topen.get(), topen.get(), topen.get(), topen.get())).istrue());
    wassert(actual(Time::range_overlaps(topen.get(), topen.get(), t2005.get(), t2010.get())).istrue());
    wassert(actual(Time::range_overlaps(t2005.get(), t2010.get(), topen.get(), topen.get())).istrue());
    wassert(actual(Time::range_overlaps(t2005.get(), topen.get(), topen.get(), topen.get())).istrue());
    wassert(actual(Time::range_overlaps(t2005.get(), topen.get(), t2010.get(), topen.get())).istrue());
    wassert(actual(Time::range_overlaps(t2010.get(), topen.get(), t2005.get(), topen.get())).istrue());
    wassert(actual(Time::range_overlaps(topen.get(), t2005.get(), topen.get(), t2010.get())).istrue());
    wassert(actual(Time::range_overlaps(topen.get(), t2010.get(), topen.get(), t2005.get())).istrue());
    wassert(actual(Time::range_overlaps(t2010.get(), topen.get(), topen.get(), t2010.get())).istrue());
    wassert(actual(Time::range_overlaps(t2000.get(), t2005.get(), t2007.get(), t2010.get())).isfalse());
    wassert(actual(Time::range_overlaps(t2007.get(), t2010.get(), t2000.get(), t2005.get())).isfalse());
    wassert(actual(Time::range_overlaps(t2010.get(), topen.get(), topen.get(), t2005.get())).isfalse());
}

// Reproduce bugs
template<> template<>
void to::test<8>()
{
    auto_ptr<Type> decoded = decodeString(TYPE_TIME, "2005-12-01T18:00:00Z");
    wassert(actual(wibble::str::fmt(*decoded)) == "2005-12-01T18:00:00Z");
}

}

// vim:set ts=4 sw=4:
