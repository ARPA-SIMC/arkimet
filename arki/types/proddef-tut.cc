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
#include <arki/types/proddef.h>
#include <arki/matcher.h>

#include <sstream>
#include <iostream>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_proddef_shar {
};
TESTGRP(arki_types_proddef);

// Check GRIB
def_test(1)
{
    tests::TestGenericType t("proddef", "GRIB(uno=1,due=2,tre=-3,supercazzola=-1234567,pippo=pippo,pluto=\"12\",cippo=)");
    t.higher.push_back("GRIB(dieci=10,undici=11,dodici=-12)");
    t.exact_query = "GRIB:cippo=, due=2, pippo=pippo, pluto=\"12\", supercazzola=-1234567, tre=-3, uno=1";
    wassert(t.check());
}

// Test Lua functions
def_test(2)
{
#ifdef HAVE_LUA
    ValueBag test1;
    test1.set("uno", Value::createInteger(1));
    test1.set("pippo", Value::createString("pippo"));
    unique_ptr<Proddef> o = Proddef::createGRIB(test1);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'GRIB' then return 'style is '..o.style..' instead of GRIB' end \n"
		"  v = o.val \n"
		"  if v['uno'] ~= 1 then return 'v[\\'uno\\'] is '..v['uno']..' instead of 1' end \n"
		"  if v['pippo'] ~= 'pippo' then return 'v[\\'pippo\\'] is '..v['pippo']..' instead of \\'pippo\\'' end \n"
		"  if tostring(o) ~= 'GRIB(pippo=pippo, uno=1)' then return 'tostring gave '..tostring(o)..' instead of GRIB(pippo=pippo, uno=1)' end \n"
		"  o1 = arki_proddef.grib{uno=1, pippo='pippo'}\n"
		"  if o ~= o1 then return 'new proddef is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

    test.pusharg(*o);
    wassert(actual(test.run()) == "");
#endif
}

// Check two more samples
def_test(3)
{
    tests::TestGenericType t("proddef", "GRIB(count=1,pippo=pippo)");
    t.higher.push_back("GRIB(count=2,pippo=pippo)");
    wassert(t.check());
}

}
