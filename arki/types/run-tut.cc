/*
 * Copyright (C) 2008--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/run.h>
#include <arki/tests/lua.h>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_run_shar {
};
TESTGRP(arki_types_run);

// Check MINUTE
def_test(1)
{
    tests::TestGenericType t("run", "MINUTE(12:00)");
    t.alternates.push_back("MINUTE(12)");
    t.lower.push_back("MINUTE(00)");
    t.lower.push_back("MINUTE(11)");
    t.lower.push_back("MINUTE(11:00)");
    t.higher.push_back("MINUTE(12:01)");
    t.higher.push_back("MINUTE(13)");
    t.exact_query = "MINUTE,12:00";
    wassert(t.check());
}

// Test Lua functions
def_test(2)
{
#ifdef HAVE_LUA
    unique_ptr<Run> o = Run::createMinute(12, 30);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'MINUTE' then return 'style is '..o.style..' instead of MINUTE' end \n"
		"  if o.hour ~= 12 then return 'o.hour is '..o.hour..' instead of 12' end \n"
		"  if o.min ~= 30 then return 'o.min is '..o.min..' instead of 30' end \n"
		"  if tostring(o) ~= 'MINUTE(12:30)' then return 'tostring gave '..tostring(o)..' instead of MINUTE(12:30)' end \n"
		"  local o1 = arki_run.minute(12, 30)\n"
		"  if o ~= o1 then return 'new run is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

    test.pusharg(*o);
    wassert(actual(test.run()) == "");
#endif
}

}
