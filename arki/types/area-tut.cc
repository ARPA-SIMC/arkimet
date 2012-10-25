/*
 * Copyright (C) 2007--2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/area.h>
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

struct arki_types_area_shar {
};
TESTGRP(arki_types_area);

// Check GRIB
template<> template<>
void to::test<1>()
{
	ValueBag test1;
	test1.set("uno", Value::createInteger(1));
	test1.set("due", Value::createInteger(2));
	test1.set("tre", Value::createInteger(-3));
	test1.set("supercazzola", Value::createInteger(-1234567));
	test1.set("pippo", Value::createString("pippo"));
	test1.set("pluto", Value::createString("12"));
	test1.set("cippo", Value::createString(""));
	ValueBag test2;
	test2.set("dieci", Value::createInteger(10));
	test2.set("undici", Value::createInteger(11));
	test2.set("dodici", Value::createInteger(-12));

	Item<Area> o = area::GRIB::create(test1);
	ensure_equals(o->style(), Area::GRIB);
	const area::GRIB* v = o->upcast<area::GRIB>();
	ensure_equals(v->values().size(), 7u);
	ensure_equals(v->values(), test1);

	ensure_equals(o, Item<Area>(area::GRIB::create(test1)));
	ensure(o != area::GRIB::create(test2));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_AREA);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB:cippo=, due=2, pippo=pippo, pluto=\"12\", supercazzola=-1234567, tre=-3, uno=1");
	Matcher m = Matcher::parse("area:" + o->exactQuery());
	ensure(m(o));
}

// Test Lua functions
template<> template<>
void to::test<2>()
{
#ifdef HAVE_LUA
	ValueBag test1;
	test1.set("uno", Value::createInteger(1));
	test1.set("pippo", Value::createString("pippo"));
	Item<Area> o = area::GRIB::create(test1);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'GRIB' then return 'style is '..o.style..' instead of GRIB1' end \n"
		"  v = o.val \n"
		"  if v['uno'] ~= 1 then return 'v[\\'uno\\'] is '..v['uno']..' instead of 1' end \n"
		"  if v['pippo'] ~= 'pippo' then return 'v[\\'pippo\\'] is '..v['pippo']..' instead of \\'pippo\\'' end \n"
		"  if tostring(o) ~= 'GRIB(pippo=pippo, uno=1)' then return 'tostring gave '..tostring(o)..' instead of GRIB(pippo=pippo, uno=1)' end \n"
		"  o1 = arki_area.grib{uno=1, pippo='pippo'}\n"
		"  if o ~= o1 then return 'new area is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
#endif
}

// Check comparisons
template<> template<>
void to::test<3>()
{
	ValueBag test1;
	test1.set("count", Value::createInteger(1));
	test1.set("pippo", Value::createString("pippo"));
	ValueBag test2;
	test2.set("count", Value::createInteger(2));
	test2.set("pippo", Value::createString("pippo"));
	ensure_compares(
		area::GRIB::create(test1),
		area::GRIB::create(test2),
		area::GRIB::create(test2));
}

// Check ODIMH5
template<> template<>
void to::test<4>()
{
	ValueBag test1;
	test1.set("uno", Value::createInteger(1));
	test1.set("due", Value::createInteger(2));
	test1.set("tre", Value::createInteger(-3));
	test1.set("supercazzola", Value::createInteger(-1234567));
	test1.set("pippo", Value::createString("pippo"));
	test1.set("pluto", Value::createString("12"));
	test1.set("cippo", Value::createString(""));
	ValueBag test2;
	test2.set("dieci", Value::createInteger(10));
	test2.set("undici", Value::createInteger(11));
	test2.set("dodici", Value::createInteger(-12));

	Item<Area> o = area::ODIMH5::create(test1);
	ensure_equals(o->style(), Area::ODIMH5);
	const area::ODIMH5* v = o->upcast<area::ODIMH5>();
	ensure_equals(v->values().size(), 7u);
	ensure_equals(v->values(), test1);

	ensure_equals(o, Item<Area>(area::ODIMH5::create(test1)));
	ensure(o != area::ODIMH5::create(test2));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_AREA);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "ODIMH5:cippo=, due=2, pippo=pippo, pluto=\"12\", supercazzola=-1234567, tre=-3, uno=1");
	Matcher m = Matcher::parse("area:" + o->exactQuery());
	ensure(m(o));
}

// Check VM2
template<> template<>
void to::test<5>()
{
	Item<Area> o = area::VM2::create(1);
	ensure_equals(o->style(), Area::VM2);
	const area::VM2* v = o->upcast<area::VM2>();
	ensure_equals(v->station_id(), 1u);

	ensure_equals(o, Item<Area>(area::VM2::create(1)));
	ensure(o != area::VM2::create(2));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_AREA);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "VM2,1");
	Matcher m = Matcher::parse("area:" + o->exactQuery());
	ensure(m(o));
}





}

// vim:set ts=4 sw=4:
