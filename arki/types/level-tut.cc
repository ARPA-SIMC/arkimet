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
	ensure_equals(v->type(), 1u);
	ensure_equals(v->l1(), 0u);
	ensure_equals(v->l2(), 0u);
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
	ensure_equals(v->type(), 103u);
	ensure_equals(v->l1(), 132u);
	ensure_equals(v->l2(), 0u);
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
	ensure_equals(v->type(), 103u);
	ensure_equals(v->l1(), 13200u);
	ensure_equals(v->l2(), 0u);
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
	ensure_equals(v->type(), 104u);
	ensure_equals(v->l1(), 132u);
	ensure_equals(v->l2(), 231u);
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
	ensure_equals(v->type(), 100u);
	ensure_equals(v->scale(), 100u);
	ensure_equals(v->value(), 500u);

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
	ensure_equals(v->type1(), 100u);
	ensure_equals(v->scale1(), 100u);
	ensure_equals(v->value1(), 500u);
	ensure_equals(v->type2(), 100u);
	ensure_equals(v->scale2(), 100u);
	ensure_equals(v->value2(), 1000u);

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

// Test Lua functions
template<> template<>
void to::test<7>()
{
#ifdef HAVE_LUA
	Item<Level> o = level::GRIB1::create(104, 132, 231);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'GRIB1' then return 'style is '..o.style..' instead of GRIB1' end \n"
		"  if o.type ~= 104 then return 'o.type first item is '..o.type..' instead of 104' end \n"
		"  if o.l1 ~= 132 then return 'o.l1 second item is '..o.l1..' instead of 132' end \n"
		"  if o.l2 ~= 231 then return 'o.l2 third item is '..o.l2..' instead of 231' end \n"
		"  if tostring(o) ~= 'GRIB1(104, 132, 231)' then return 'tostring gave '..tostring(o)..' instead of GRIB1(104, 132, 231)' end \n"
		"  o1 = arki_level.grib1(104, 132, 231)\n"
		"  if o ~= o1 then return 'new level is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
#endif
}

// Check comparisons
template<> template<>
void to::test<8>()
{
	ensure_compares(
		level::GRIB1::create(104, 132, 231),
		level::GRIB1::create(104, 132, 232),
		level::GRIB1::create(104, 132, 232));
}

// Check ODIMH5
template<> template<>
void to::test<9>()
{
	Item<Level> o = level::ODIMH5::create(10.123, 20.123);
	ensure_equals(o->style(), Level::ODIMH5);
	const level::ODIMH5* v = o->upcast<level::ODIMH5>();
	ensure(v->max() == 20.123);
	ensure(v->min() == 10.123);

	ensure_equals(v->max(), 20.123);
	ensure_equals(v->min(), 10.123);

	ensure_equals(o, Item<Level>(level::ODIMH5::create(10.123, 20.123)));
	ensure(o != level::GRIB1::create(100));
	ensure(o != level::GRIB2S::create(100, 100, 500));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_LEVEL);
}

// Check ODIMH5
template<> template<>
void to::test<10>()
{
	Item<Level> o = level::ODIMH5::create(10.123, 20.123);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "ODIMH5,range 10.123 20.123");
	Matcher m = Matcher::parse("level:" + o->exactQuery());
	ensure(m(o));
}



}

// vim:set ts=4 sw=4:
