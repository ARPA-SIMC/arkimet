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
#include <arki/types/reftime.h>
#include <arki/matcher.h>

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

struct arki_types_reftime_shar {
};
TESTGRP(arki_types_reftime);

// Check Position
template<> template<>
void to::test<1>()
{
	Item<Reftime> o = reftime::Position::create(Time::create(2007, 6, 5, 4, 3, 2));
	ensure_equals(o->style(), Reftime::POSITION);
	const reftime::Position* v = o->upcast<reftime::Position>();
	ensure_equals(v->time, Item<Time>(Time::create(2007, 6, 5, 4, 3, 2)));

	ensure_equals(o, Item<Reftime>(reftime::Position::create(Time::create(2007, 6, 5, 4, 3, 2))));
	ensure(o != Item<Reftime>(reftime::Position::create(Time::create(2007, 6, 5, 4, 3, 1))));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_REFTIME);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "=2007-06-05T04:03:02Z");
	Matcher m = Matcher::parse("reftime:" + o->exactQuery());
	ensure(m(o));
}

// Check Period
template<> template<>
void to::test<2>()
{
	Item<Reftime> o = reftime::Period::create(
		Time::create(2007, 6, 5, 4, 3, 2),
		Time::create(2008, 7, 6, 5, 4, 3));
	ensure_equals(o->style(), Reftime::PERIOD);
	const reftime::Period* v = o->upcast<reftime::Period>();
	ensure_equals(v->begin, Item<Time>(Time::create(2007, 6, 5, 4, 3, 2)));
	ensure_equals(v->end, Item<Time>(Time::create(2008, 7, 6, 5, 4, 3)));

	ensure_equals(o, Item<Reftime>(reftime::Period::create(
		Time::create(2007, 6, 5, 4, 3, 2),
		Time::create(2008, 7, 6, 5, 4, 3))));
	ensure(o != reftime::Period::create(
		Time::create(2007, 6, 5, 4, 3, 3),
		Time::create(2008, 7, 6, 5, 4, 2)));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_REFTIME);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "=2007-06-05T04:03:02Z");
	Matcher m = Matcher::parse("reftime:" + o->exactQuery());
	ensure(m(o));
}

// Check collector
template<> template<>
void to::test<3>()
{
	reftime::Collector c;
	Item<Time> t1 = Time::create(2007, 6, 5, 4, 3, 2);
	Item<Time> t2 = Time::create(2008, 7, 6, 5, 4, 3);
	Item<Time> t3 = Time::create(2007, 7, 6, 5, 4, 3);
	Item<Time> t4 = Time::create(2009, 8, 7, 6, 5, 4);
	Item<Time> t5 = Time::create(2009, 7, 6, 5, 4, 3);
	Item<Time> t6 = Time::create(2010, 9, 8, 7, 6, 5);

	// Merge with position
	Item<Reftime> o = reftime::Position::create(t1);
	c.merge(o);
	ensure_equals(c.begin, t1);
	ensure(!c.end.defined());
	//ensure_equals(c.end, t1);

	// Merge with a second position
	o = reftime::Position::create(t2);
	c.merge(o);
	ensure_equals(c.begin, t1);
	ensure_equals(c.end, t2);
	
	// Merge with a period
	o = reftime::Period::create(t3, t4);
	c.merge(o);
	ensure_equals(c.begin, t1);
	ensure_equals(c.end, t4);

	// Merge with another collector
	reftime::Collector c1;
	o = reftime::Period::create(t5, t6);
	c1.merge(o);
	ensure_equals(c1.begin, t5);
	ensure_equals(c1.end, t6);

	c.merge(c1);
	ensure_equals(c.begin, t1);
	ensure_equals(c.end, t6);
}

// Test Lua functions
template<> template<>
void to::test<4>()
{
#ifdef HAVE_LUA
	Item<Reftime> o = reftime::Position::create(Time::create(2007, 6, 5, 4, 3, 2));

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
	ensure_equals(test.run(), "");
#endif
}

}

// vim:set ts=4 sw=4:
