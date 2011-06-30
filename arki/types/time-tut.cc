/*
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

#include <arki/types/test-utils.h>
#include <arki/types/time.h>

#include <sstream>
#include <iostream>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_types_time_shar {
};
TESTGRP(arki_types_time);

// Check 'now' time
template<> template<>
void to::test<1>()
{
	Item<Time> o = Time::create();
	ensure_equals((*o)[0], 0);
	ensure_equals((*o)[1], 0);
	ensure_equals((*o)[2], 0);
	ensure_equals((*o)[3], 0);
	ensure_equals((*o)[4], 0);
	ensure_equals((*o)[5], 0);
	ensure_equals(o->vals[0], 0);
	ensure_equals(o->vals[1], 0);
	ensure_equals(o->vals[2], 0);
	ensure_equals(o->vals[3], 0);
	ensure_equals(o->vals[4], 0);
	ensure_equals(o->vals[5], 0);

	ensure_equals(o, Item<Time>(Time::create()));
	ensure_equals(o, Item<Time>(Time::create(0, 0, 0, 0, 0, 0)));
	ensure(o != Item<Time>(Time::create(1789, 7, 14, 12, 0, 0)));

	// Test serialisation to ISO-8601
	ensure_equals(o->toISO8601(), "0000-00-00T00:00:00Z");

	// Test serialisation to SQL
	ensure_equals(o->toSQL(), "0000-00-00 00:00:00");

	// Test deserialisation from ISO-8601
	ensure_equals(o, Item<Time>(Time::createFromISO8601(o->toISO8601())));

	// Test deserialisation from SQL
	ensure_equals(o, Item<Time>(Time::createFromSQL(o->toSQL())));

	ensure_serialises(o, types::TYPE_TIME);
}

// Check an arbitrary time
template<> template<>
void to::test<2>()
{
	Item<Time> o = Time::create(1, 2, 3, 4, 5, 6);
	ensure_equals((*o)[0], 1);
	ensure_equals((*o)[1], 2);
	ensure_equals((*o)[2], 3);
	ensure_equals((*o)[3], 4);
	ensure_equals((*o)[4], 5);
	ensure_equals((*o)[5], 6);
	ensure_equals(o->vals[0], 1);
	ensure_equals(o->vals[1], 2);
	ensure_equals(o->vals[2], 3);
	ensure_equals(o->vals[3], 4);
	ensure_equals(o->vals[4], 5);
	ensure_equals(o->vals[5], 6);

	ensure_equals(o, Item<Time>(Time::create(1, 2, 3, 4, 5, 6)));
	ensure(o != Item<Time>(Time::create(1789, 7, 14, 12, 0, 0)));

	// Test serialisation to ISO-8601
	ensure_equals(o->toISO8601(), "0001-02-03T04:05:06Z");

	// Test serialisation to SQL
	ensure_equals(o->toSQL(), "0001-02-03 04:05:06");

	// Test deserialisation from ISO-8601
	ensure_equals(o, Item<Time>(Time::createFromISO8601(o->toISO8601())));

	// Test deserialisation from SQL
	ensure_equals(o, Item<Time>(Time::createFromSQL(o->toSQL())));

	ensure_serialises(o, types::TYPE_TIME);
}

// Check time differences
template<> template<>
void to::test<3>()
{
	Item<Time> t = Time::createDifference(Time::create(2007, 6, 5, 4, 3, 2), Time::create(2006, 5, 4, 3, 2, 1));
	ensure_equals(t, Item<Time>(Time::create(1, 1, 1, 1, 1, 1)));

	t = Time::createDifference(Time::create(2007, 3, 1, 0, 0, 0), Time::create(2007, 2, 28, 0, 0, 0));
	ensure_equals(t, Item<Time>(Time::create(0, 0, 1, 0, 0, 0)));

	t = Time::createDifference(Time::create(2008, 3, 1, 0, 0, 0), Time::create(2008, 2, 28, 0, 0, 0));
	ensure_equals(t, Item<Time>(Time::create(0, 0, 2, 0, 0, 0)));

	t = Time::createDifference(Time::create(2008, 3, 1, 0, 0, 0), Time::create(2007, 12, 20, 0, 0, 0));
	ensure_equals(t, Item<Time>(Time::create(0, 2, 10, 0, 0, 0)));

	t = Time::createDifference(Time::create(2007, 2, 28, 0, 0, 0), Time::create(2008, 3, 1, 0, 0, 0));
	ensure_equals(t, Item<Time>(Time::create(0, 0, 0, 0, 0, 0)));
}

// Check comparisons
template<> template<>
void to::test<4>()
{
	ensure_compares(
		Time::create(2006, 5, 4, 3, 2, 1),
		Time::create(2007, 6, 5, 4, 3, 2),
		Time::create(2007, 6, 5, 4, 3, 2));
}

// Test Lua functions
template<> template<>
void to::test<5>()
{
#ifdef HAVE_LUA
	Item<Time> o = Time::create(1, 2, 3, 4, 5, 6);

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
	ensure_equals(test.run(), "");
#endif
}

// Test Time::generate
template<> template<>
void to::test<6>()
{
	vector< Item<Time> > v = Time::generate(
			*Time::create(2009, 1, 1, 0, 0, 0),
			*Time::create(2009, 2, 1, 0, 0, 0),
			3600);
	ensure_equals(v.size(), 31u*24u);
}

// Test Time::range_overlaps
template<> template<>
void to::test<7>()
{
    UItem<types::Time> topen;
    UItem<types::Time> t2000(Time::create(2000, 1, 1, 0, 0, 0));
    UItem<types::Time> t2005(Time::create(2005, 1, 1, 0, 0, 0));
    UItem<types::Time> t2007(Time::create(2007, 1, 1, 0, 0, 0));
    UItem<types::Time> t2010(Time::create(2010, 1, 1, 0, 0, 0));

    ensure(    Time::range_overlaps(topen, topen, topen, topen));
    ensure(    Time::range_overlaps(topen, topen, t2005, t2010));
    ensure(    Time::range_overlaps(t2005, t2010, topen, topen));
    ensure(    Time::range_overlaps(t2005, topen, topen, topen));
    ensure(    Time::range_overlaps(t2005, topen, t2010, topen));
    ensure(    Time::range_overlaps(t2010, topen, t2005, topen));
    ensure(    Time::range_overlaps(topen, t2005, topen, t2010));
    ensure(    Time::range_overlaps(topen, t2010, topen, t2005));
    ensure(    Time::range_overlaps(t2010, topen, topen, t2010));
    ensure(not Time::range_overlaps(t2000, t2005, t2007, t2010));
    ensure(not Time::range_overlaps(t2007, t2010, t2000, t2005));
    ensure(not Time::range_overlaps(t2010, topen, topen, t2005));
}

}

// vim:set ts=4 sw=4:
