/*
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/assigneddataset.h>

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

struct arki_types_assigneddataset_shar {
};
TESTGRP(arki_types_assigneddataset);

// Check AssignedDataset, created with current time
template<> template<>
void to::test<1>()
{
	Item<Time> pre = Time::createNow();
	Item<AssignedDataset> o = AssignedDataset::create("testname", "testid");
	Item<Time> post = Time::createNow();
	ensure_equals(o->name, "testname");
	ensure_equals(o->id, "testid");
	ensure(o->changed >= pre);
	ensure(o->changed <= post);

	ensure_equals(o, Item<AssignedDataset>(AssignedDataset::create(o->changed, "testname", "testid")));
	ensure(o != Item<AssignedDataset>(AssignedDataset::create(o->changed, "testname1", "testid")));
	ensure(o != Item<AssignedDataset>(AssignedDataset::create(o->changed, "testname", "testid1")));
	// Comparison should not care about the attribution time
	ensure_equals(o, Item<AssignedDataset>(AssignedDataset::create(Time::create(1800, 1, 2, 3, 4, 5), "testname", "testid")));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_ASSIGNEDDATASET);
}

// Check AssignedDataset, created with arbitrary time
template<> template<>
void to::test<2>()
{
	Item<Time> time = Time::create(2007, 6, 5, 4, 3, 2);
	Item<AssignedDataset> o = AssignedDataset::create(time, "testname", "testid");
	ensure_equals(o->changed, time);
	ensure_equals(o->name, "testname");
	ensure_equals(o->id, "testid");

	ensure_equals(o, Item<AssignedDataset>(AssignedDataset::create(time, "testname", "testid")));
	ensure(o != Item<AssignedDataset>(AssignedDataset::create(time, "testname1", "testid")));
	ensure(o != Item<AssignedDataset>(AssignedDataset::create(time, "testname", "testid1")));
	// Comparison should not care about the attribution time
	ensure_equals(o, Item<AssignedDataset>(AssignedDataset::create(Time::create(2007, 6, 5, 4, 3, 1), "testname", "testid")));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_ASSIGNEDDATASET);
}

#ifdef HAVE_LUA
// Test Lua functions
template<> template<>
void to::test<3>()
{
	Item<Time> time = Time::create(2007, 6, 5, 4, 3, 2);
	Item<AssignedDataset> o = AssignedDataset::create(time, "testname", "testid");

	tests::Lua test(
		"function test(o) \n"
		"  if o.name ~= 'testname' then return 'name is '..o.name..' instead of testname' end \n"
		"  if o.id ~= 'testid' then return 'id is '..o.id..' instead of testid' end \n"
		"  t = o.changed \n"
		"  if t.year ~= 2007 then return 't.year is '..t.year..' instead of 2007' end \n"
		"  if t.month ~= 6 then return 't.month is '..t.month..' instead of 6' end \n"
		"  if t.day ~= 5 then return 't.day is '..t.day..' instead of 5' end \n"
		"  if t.hour ~= 4 then return 't.hour is '..t.hour..' instead of 4' end \n"
		"  if t.minute ~= 3 then return 't.minute is '..t.minute..' instead of 3' end \n"
		"  if t.second ~= 2 then return 't.second is '..t.second..' instead of 2' end \n"
		"  if tostring(o) ~= 'testname as testid imported on 2007-06-05T04:03:02Z' then return 'tostring gave '..tostring(o)..' instead of \\'testname as testid imported on 2007-06-05T04:03:02Z\\'' end \n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
}
#endif

}
// vim:set ts=4 sw=4:
