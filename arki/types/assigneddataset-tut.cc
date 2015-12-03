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

#include <arki/types/tests.h>
#include <arki/types/assigneddataset.h>
#include <arki/tests/lua.h>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_assigneddataset_shar {
};
TESTGRP(arki_types_assigneddataset);

// Check AssignedDataset, created with current time
template<> template<>
void to::test<1>()
{
    tests::TestGenericType t("assigneddataset", "testname as testid imported on 2015-01-03T00:00:00");
    // Comparison should not care about the attribution time
    t.alternates.push_back("testname as testid imported on 2015-01-02T00:00:00");
    t.alternates.push_back("testname as testid imported on 2015-01-04T00:00:00");
    t.lower.push_back("pestname as testid imported on 2015-01-03T00:00:00");
    t.lower.push_back("testname as pestid imported on 2015-01-03T00:00:00");
    t.higher.push_back("zestname as testid imported on 2015-01-03T00:00:00");
    t.higher.push_back("testname as zestid imported on 2015-01-03T00:00:00");
    wassert(t.check());
}

#ifdef HAVE_LUA
// Test Lua functions
template<> template<>
void to::test<2>()
{
    unique_ptr<AssignedDataset> o = AssignedDataset::create(Time(2007, 6, 5, 4, 3, 2), "testname", "testid");

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
    wassert(actual(test.run()) == "");
}
#endif

}
// vim:set ts=4 sw=4:
