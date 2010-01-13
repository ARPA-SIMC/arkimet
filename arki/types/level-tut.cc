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
#include <arki/types/level.h>
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

struct arki_types_level_shar {
};
TESTGRP(arki_types_level);

// Check GRIB1 without l1 and l2
template<> template<>
void to::test<1>()
{
	Item<Level> o = level::GRIB1::create(1);
	ensure_equals(o->style(), Level::GRIB1);
	const level::GRIB1* v = o->upcast<level::GRIB1>();
	ensure_equals(v->type, 1);
	ensure_equals(v->l1, 0);
	ensure_equals(v->l2, 0);
	ensure_equals(v->valType(), 0);

	ensure_equals(o, Item<Level>(level::GRIB1::create(1)));
	ensure_equals(o, Item<Level>(level::GRIB1::create(1, 1, 0)));
	ensure_equals(o, Item<Level>(level::GRIB1::create(1, 1, 1)));
	ensure(o != level::GRIB1::create(2));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_LEVEL);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB1,1");
	Matcher m = Matcher::parse("level:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB1 with an 8 bit l1
template<> template<>
void to::test<2>()
{
	Item<Level> o = level::GRIB1::create(103, 132);
	ensure_equals(o->style(), Level::GRIB1);
	const level::GRIB1* v = o->upcast<level::GRIB1>();
	ensure_equals(v->type, 103);
	ensure_equals(v->l1, 132);
	ensure_equals(v->l2, 0);
	ensure_equals(v->valType(), 1);

	ensure_equals(o, Item<Level>(level::GRIB1::create(103, 132)));
	ensure_equals(o, Item<Level>(level::GRIB1::create(103, 132, 0)));
	ensure_equals(o, Item<Level>(level::GRIB1::create(103, 132, 1)));
	ensure(o != level::GRIB1::create(1));
	ensure(o != level::GRIB1::create(100, 132));
	ensure(o != level::GRIB1::create(103, 133));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_LEVEL);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB1,103,132");
	Matcher m = Matcher::parse("level:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB1 with a 16 bit l1
template<> template<>
void to::test<3>()
{
	Item<Level> o = level::GRIB1::create(103, 13200);
	ensure_equals(o->style(), Level::GRIB1);
	const level::GRIB1* v = o->upcast<level::GRIB1>();
	ensure_equals(v->type, 103);
	ensure_equals(v->l1, 13200);
	ensure_equals(v->l2, 0);
	ensure_equals(v->valType(), 1);

	ensure_equals(o, Item<Level>(level::GRIB1::create(103, 13200)));
	ensure(o != level::GRIB1::create(1));
	ensure(o != level::GRIB1::create(100, 13200));
	ensure(o != level::GRIB1::create(103, 13201));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_LEVEL);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB1,103,13200");
	Matcher m = Matcher::parse("level:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB1 with a layer
template<> template<>
void to::test<4>()
{
	Item<Level> o = level::GRIB1::create(104, 132, 231);
	ensure_equals(o->style(), Level::GRIB1);
	const level::GRIB1* v = o->upcast<level::GRIB1>();
	ensure_equals(v->type, 104);
	ensure_equals(v->l1, 132);
	ensure_equals(v->l2, 231);
	ensure_equals(v->valType(), 2);

	ensure_equals(o, Item<Level>(level::GRIB1::create(104, 132, 231)));
	ensure(o != level::GRIB1::create(1));
	ensure(o != level::GRIB1::create(100, 132));
	ensure(o != level::GRIB1::create(104, 133, 231));
	ensure(o != level::GRIB1::create(104, 132, 232));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_LEVEL);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB1,104,132,231");
	Matcher m = Matcher::parse("level:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB2S
template<> template<>
void to::test<5>()
{
	Item<Level> o = level::GRIB2S::create(100, 100, 500);
	ensure_equals(o->style(), Level::GRIB2S);
	const level::GRIB2S* v = o->upcast<level::GRIB2S>();
	ensure_equals(v->type, 100);
	ensure_equals(v->scale, 100);
	ensure_equals(v->value, 500);

	ensure_equals(o, Item<Level>(level::GRIB2S::create(100, 100, 500)));
	ensure(o != level::GRIB2S::create(100, 0, 50000));
	ensure(o != level::GRIB2S::create(101, 100, 500));
	ensure(o != level::GRIB2S::create(100, 1, 500));
	ensure(o != level::GRIB1::create(100));
	ensure(o != level::GRIB1::create(100, 50000));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_LEVEL);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB2S,100,100,500");
	Matcher m = Matcher::parse("level:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB2D
template<> template<>
void to::test<6>()
{
	Item<Level> o = level::GRIB2D::create(100, 100, 500, 100, 100, 1000);
	ensure_equals(o->style(), Level::GRIB2D);
	const level::GRIB2D* v = o->upcast<level::GRIB2D>();
	ensure_equals(v->type1, 100);
	ensure_equals(v->scale1, 100);
	ensure_equals(v->value1, 500);
	ensure_equals(v->type2, 100);
	ensure_equals(v->scale2, 100);
	ensure_equals(v->value2, 1000);

	ensure_equals(o, Item<Level>(level::GRIB2D::create(100, 100, 500, 100, 100, 1000)));
	ensure(o != level::GRIB1::create(100));
	ensure(o != level::GRIB2S::create(100, 100, 500));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_LEVEL);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB2D,100,100,500,100,100,1000");
	Matcher m = Matcher::parse("level:" + o->exactQuery());
	ensure(m(o));
}

#ifdef HAVE_LUA
// Test Lua functions
template<> template<>
void to::test<7>()
{
	Item<Level> o = level::GRIB1::create(104, 132, 231);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style() ~= 'GRIB1' then return 'style is '..o.style()..' instead of GRIB1' end \n"
		"  a, b, c = o.grib1() \n"
		"  if a ~= 104 then return 'o.grib1() first item is '..a..' instead of 104' end \n"
		"  if b ~= 132 then return 'o.grib1() second item is '..b..' instead of 132' end \n"
		"  if c ~= 231 then return 'o.grib1() third item is '..c..' instead of 231' end \n"
		"  if o.grib2() ~= nil then return 'o.grib2() gave '..o.grib2()..' instead of nil' end \n"
		"  if o.bufr() ~= nil then return 'o.bufr() gave '..o.bufr()..' instead of nil' end \n"
		"  if tostring(o) ~= 'GRIB1(104, 132, 231)' then return 'tostring gave '..tostring(o)..' instead of GRIB1(104, 132, 231)' end \n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
}
#endif

}

// vim:set ts=4 sw=4:
