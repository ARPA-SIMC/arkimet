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
#include <arki/types/area.h>
#include <arki/tests/lua.h>

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_area_shar {
};
TESTGRP(arki_types_area);

// Check GRIB
template<> template<>
void to::test<1>()
{
    tests::TestGenericType t("area", "GRIB(uno=1,due=2,tre=-3,supercazzola=-1234567,pippo=pippo,pluto=\"12\",cippo=)");
    t.higher.push_back("GRIB(dieci=10,undici=11,dodici=-12)");
    t.exact_query = "GRIB:cippo=, due=2, pippo=pippo, pluto=\"12\", supercazzola=-1234567, tre=-3, uno=1";
    wassert(t);
}

// Test Lua functions
template<> template<>
void to::test<2>()
{
#ifdef HAVE_LUA
    ValueBag test1;
    test1.set("uno", Value::createInteger(1));
    test1.set("pippo", Value::createString("pippo"));
    unique_ptr<Area> o = Area::createGRIB(test1);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'GRIB' then return 'style is '..o.style..' instead of GRIB' end \n"
		"  v = o.val \n"
		"  if v['uno'] ~= 1 then return 'v[\\'uno\\'] is '..v['uno']..' instead of 1' end \n"
		"  if v['pippo'] ~= 'pippo' then return 'v[\\'pippo\\'] is '..v['pippo']..' instead of \\'pippo\\'' end \n"
		"  if tostring(o) ~= 'GRIB(pippo=pippo, uno=1)' then return 'tostring gave '..tostring(o)..' instead of GRIB(pippo=pippo, uno=1)' end \n"
		"  o1 = arki_area.grib{uno=1, pippo='pippo'}\n"
		"  if o ~= o1 then return 'new area is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

    test.pusharg(*o);
    wassert(actual(test.run()) == "");
#endif
}

// Check two more samples
template<> template<>
void to::test<3>()
{
    tests::TestGenericType t("area", "GRIB(count=1,pippo=pippo)");
    t.higher.push_back("GRIB(count=2,pippo=pippo)");
    wassert(t);
}

// Check ODIMH5
template<> template<>
void to::test<4>()
{
    tests::TestGenericType t("area", "ODIMH5(uno=1,due=2,tre=-3,supercazzola=-1234567,pippo=pippo,pluto=\"12\",cippo=)");
    t.higher.push_back("ODIMH5(dieci=10,undici=11,dodici=-12)");
    t.exact_query = "ODIMH5:cippo=, due=2, pippo=pippo, pluto=\"12\", supercazzola=-1234567, tre=-3, uno=1";
    wassert(t);
}

// Check VM2
template<> template<>
void to::test<5>()
{
    tests::TestGenericType t("area", "VM2(1)");
    t.higher.push_back("VM2(2)");
    t.exact_query = "VM2,1:lat=4460016, lon=1207738, rep=locali";
    wassert(t);

    // Test derived values
    ValueBag vb1 = ValueBag::parse("lon=1207738,lat=4460016,rep=locali");
    wassert(actual(area::VM2::create(1)->derived_values() == vb1).istrue());
    ValueBag vb2 = ValueBag::parse("lon=1207738,lat=4460016,rep=NONONO");
    wassert(actual(area::VM2::create(1)->derived_values() != vb2).istrue());
}

}
