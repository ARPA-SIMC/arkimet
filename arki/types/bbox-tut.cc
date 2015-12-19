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
#include <arki/types/bbox.h>
#include <arki/tests/lua.h>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_bbox_shar {
};
TESTGRP(arki_types_bbox);

// Check INVALID
def_test(1)
{
    tests::TestGenericType t("bbox", "INVALID");
    wassert(t.check());
}

#ifdef HAVE_LUA
// Test Lua functions
def_test(2)
{
    unique_ptr<BBox> o = BBox::createInvalid();

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'INVALID' then return 'style is '..o.style..' instead of INVALID' end \n"
		"  if tostring(o) ~= 'INVALID()' then return 'tostring gave '..tostring(o)..' instead of INVALID()' end \n"
		"end \n"
	);

    test.pusharg(*o);
    wassert(actual(test.run()) == "");
}
#endif

}

// vim:set ts=4 sw=4:
