/*
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/timerange.h>
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

struct arki_types_timerange_shar {
};
TESTGRP(arki_types_timerange);

// Check GRIB1 with seconds
template<> template<>
void to::test<1>()
{
	Item<Timerange> o = timerange::GRIB1::create(2, 254, 2, 3);
	ensure_equals(o->style(), Timerange::GRIB1);
	const timerange::GRIB1* v = o->upcast<timerange::GRIB1>();
	ensure_equals(v->type(), 2u);
	ensure_equals(v->unit(), 254u);
	ensure_equals(v->p1(), 2u);
	ensure_equals(v->p2(), 3u);

	timerange::GRIB1::Unit u;
	int t, p1, p2;
	v->getNormalised(t, u, p1, p2);
	ensure_equals(t, 2);
	ensure_equals(u, timerange::GRIB1::SECOND);
	ensure_equals(p1, 2);
	ensure_equals(p2, 3);

	ensure_equals(o, Item<Timerange>(timerange::GRIB1::create(2, 254, 2, 3)));

	ensure(o != timerange::GRIB1::create(2, 254, 3, 4));
	ensure(o != timerange::GRIB1::create(2, 1, 2, 3));
	ensure(o != timerange::GRIB1::create(2, 4, 2, 3));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_TIMERANGE);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB1, 002, 002s, 003s");
	Matcher m = Matcher::parse("timerange:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB1 with hours
template<> template<>
void to::test<2>()
{
	Item<Timerange> o = timerange::GRIB1::create(2, 1, 2, 3);
	ensure_equals(o->style(), Timerange::GRIB1);
	const timerange::GRIB1* v = o->upcast<timerange::GRIB1>();
	ensure_equals(v->type(), 2u);
	ensure_equals(v->unit(), 1u);
	ensure_equals(v->p1(), 2u);
	ensure_equals(v->p2(), 3u);

	timerange::GRIB1::Unit u;
	int t, p1, p2;
	v->getNormalised(t, u, p1, p2);
	ensure_equals(t, 2);
	ensure_equals(u, timerange::GRIB1::SECOND);
	ensure_equals(p1, 2 * 3600);
	ensure_equals(p2, 3 * 3600);

	ensure_equals(o, Item<Timerange>(timerange::GRIB1::create(2, 1, 2, 3)));

	ensure(o != timerange::GRIB1::create(2, 254, 2, 3));
	ensure(o != timerange::GRIB1::create(2, 1, 3, 4));
	ensure(o != timerange::GRIB1::create(2, 4, 2, 3));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_TIMERANGE);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB1, 002, 002h, 003h");
	Matcher m = Matcher::parse("timerange:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB1 with years
template<> template<>
void to::test<3>()
{
	Item<Timerange> o = timerange::GRIB1::create(2, 4, 2, 3);
	ensure_equals(o->style(), Timerange::GRIB1);
	const timerange::GRIB1* v = o->upcast<timerange::GRIB1>();
	ensure_equals(v->type(), 2u);
	ensure_equals(v->unit(), 4u);
	ensure_equals(v->p1(), 2u);
	ensure_equals(v->p2(), 3u);

	timerange::GRIB1::Unit u;
	int t, p1, p2;
	v->getNormalised(t, u, p1, p2);
	ensure_equals(t, 2);
	ensure_equals(u, timerange::GRIB1::MONTH);
	ensure_equals(p1, 2 * 12);
	ensure_equals(p2, 3 * 12);

	ensure_equals(o, Item<Timerange>(timerange::GRIB1::create(2, 4, 2, 3)));

	ensure(o != timerange::GRIB1::create(2, 4, 3, 4));
	ensure(o != timerange::GRIB1::create(2, 254, 2, 3));
	ensure(o != timerange::GRIB1::create(2, 1, 2, 3));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_TIMERANGE);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB1, 002, 002y, 003y");
	Matcher m = Matcher::parse("timerange:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB1 with unknown values
template<> template<>
void to::test<4>()
{
	Item<Timerange> o = timerange::GRIB1::create(250, 1, 124, 127);
	ensure_equals(o->style(), Timerange::GRIB1);
	const timerange::GRIB1* v = o->upcast<timerange::GRIB1>();
	ensure_equals(v->type(), 250u);
	ensure_equals(v->unit(), 1u);
	ensure_equals(v->p1(), 124);
	ensure_equals(v->p2(), 127);

	timerange::GRIB1::Unit u;
	int t, p1, p2;
	v->getNormalised(t, u, p1, p2);
	ensure_equals(t, 250);
	ensure_equals(u, timerange::GRIB1::SECOND);
	ensure_equals(p1, 124 * 3600);
	ensure_equals(p2, 127 * 3600);

	ensure_equals(o, Item<Timerange>(timerange::GRIB1::create(250, 1, 124, 127)));

	ensure(o != timerange::GRIB1::create(250, 1, 124, 126));
	ensure(o != timerange::GRIB1::create(250, 4, 124, 127));
	ensure(o != timerange::GRIB1::create(250, 254, 124, 127));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_TIMERANGE);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB1, 250, 124h, 127h");
	Matcher m = Matcher::parse("timerange:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB2 with seconds
template<> template<>
void to::test<5>()
{
	Item<Timerange> o = timerange::GRIB2::create(2, 254, 2, 3);
	ensure_equals(o->style(), Timerange::GRIB2);
	const timerange::GRIB2* v = o->upcast<timerange::GRIB2>();
	ensure_equals(v->type(), 2u);
	ensure_equals(v->unit(), 254u);
	ensure_equals(v->p1(), 2u);
	ensure_equals(v->p2(), 3u);

	ensure_equals(o, Item<Timerange>(timerange::GRIB2::create(2, 254, 2, 3)));

	ensure(o != timerange::GRIB2::create(2, 254, 3, 4));
	ensure(o != timerange::GRIB2::create(2, 1, 2, 3));
	ensure(o != timerange::GRIB2::create(2, 4, 2, 3));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_TIMERANGE);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB2,2,254,2,3");
	Matcher m = Matcher::parse("timerange:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB2 with hours
template<> template<>
void to::test<6>()
{
	Item<Timerange> o = timerange::GRIB2::create(2, 1, 2, 3);
	ensure_equals(o->style(), Timerange::GRIB2);
	const timerange::GRIB2* v = o->upcast<timerange::GRIB2>();
	ensure_equals(v->type(), 2u);
	ensure_equals(v->unit(), 1u);
	ensure_equals(v->p1(), 2u);
	ensure_equals(v->p2(), 3u);

	ensure_equals(o, Item<Timerange>(timerange::GRIB2::create(2, 1, 2, 3)));

	ensure(o != timerange::GRIB2::create(2, 254, 2, 3));
	ensure(o != timerange::GRIB2::create(2, 1, 3, 4));
	ensure(o != timerange::GRIB2::create(2, 4, 2, 3));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_TIMERANGE);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB2,2,1,2,3");
	Matcher m = Matcher::parse("timerange:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB2 with years
template<> template<>
void to::test<7>()
{
	Item<Timerange> o = timerange::GRIB2::create(2, 4, 2, 3);
	ensure_equals(o->style(), Timerange::GRIB2);
	const timerange::GRIB2* v = o->upcast<timerange::GRIB2>();
	ensure_equals(v->type(), 2u);
	ensure_equals(v->unit(), 4u);
	ensure_equals(v->p1(), 2u);
	ensure_equals(v->p2(), 3u);

	ensure_equals(o, Item<Timerange>(timerange::GRIB2::create(2, 4, 2, 3)));

	ensure(o != timerange::GRIB2::create(2, 4, 3, 4));
	ensure(o != timerange::GRIB2::create(2, 254, 2, 3));
	ensure(o != timerange::GRIB2::create(2, 1, 2, 3));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_TIMERANGE);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB2,2,4,2,3");
	Matcher m = Matcher::parse("timerange:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB2 with negative values
template<> template<>
void to::test<8>()
{
	Item<Timerange> o = timerange::GRIB2::create(2, 1, -2, -3);
	ensure_equals(o->style(), Timerange::GRIB2);
	const timerange::GRIB2* v = o->upcast<timerange::GRIB2>();
	ensure_equals(v->type(), 2u);
	ensure_equals(v->unit(), 1u);
	ensure_equals((int)v->p1(), -2);
	ensure_equals((int)v->p2(), -3);

	ensure_equals(o, Item<Timerange>(timerange::GRIB2::create(2, 1, -2, -3)));

	ensure(o != timerange::GRIB2::create(2, 1, 2, 3));
	ensure(o != timerange::GRIB2::create(2, 4, -2, -3));
	ensure(o != timerange::GRIB2::create(2, 254, -2, -3));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_TIMERANGE);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB2,2,1,-2,-3");
	Matcher m = Matcher::parse("timerange:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB2 with unknown values
template<> template<>
void to::test<9>()
{
	Item<Timerange> o = timerange::GRIB2::create(250, 1, -2, -3);
	ensure_equals(o->style(), Timerange::GRIB2);
	const timerange::GRIB2* v = o->upcast<timerange::GRIB2>();
	ensure_equals(v->type(), 250u);
	ensure_equals(v->unit(), 1u);
	ensure_equals((int)v->p1(), -2);
	ensure_equals((int)v->p2(), -3);

	ensure_equals(o, Item<Timerange>(timerange::GRIB2::create(250, 1, -2, -3)));

	ensure(o != timerange::GRIB2::create(250, 1, 2, 3));
	ensure(o != timerange::GRIB2::create(250, 4, -2, -3));
	ensure(o != timerange::GRIB2::create(250, 254, -2, -3));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_TIMERANGE);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB2,250,1,-2,-3");
	Matcher m = Matcher::parse("timerange:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB2 with some values that used to fail
template<> template<>
void to::test<10>()
{
	Item<Timerange> o1 = timerange::GRIB2::create(11, 1, 3, 3);
	Item<Timerange> o2 = timerange::GRIB2::create(11, 1, 3, 6);
	const timerange::GRIB2* v1 = o1->upcast<timerange::GRIB2>();
	const timerange::GRIB2* v2 = o2->upcast<timerange::GRIB2>();
	ensure_equals(v1->type(), 11u);
	ensure_equals(v1->unit(), 1u);
	ensure_equals(v1->p1(), 3u);
	ensure_equals(v1->p2(), 3u);
	ensure_equals(v2->type(), 11u);
	ensure_equals(v2->unit(), 1u);
	ensure_equals(v2->p1(), 3u);
	ensure_equals(v2->p2(), 6u);

	ensure(o1 != o2);

	ensure(o1.encode() != o2.encode());
	ensure(o1 < o2);
	ensure(o2 > o1);
}

// Check BUFR
template<> template<>
void to::test<11>()
{
	Item<Timerange> o = timerange::BUFR::create(6, 1);
	ensure_equals(o->style(), Timerange::BUFR);
	const timerange::BUFR* v = o->upcast<timerange::BUFR>();
	ensure_equals(v->unit(), 1u);
	ensure_equals(v->value(), 6u);

	ensure_equals(o, Item<Timerange>(timerange::BUFR::create(6, 1)));
	ensure_equals(o, Item<Timerange>(timerange::BUFR::create(6*60, 0)));
	ensure_equals(o, Item<Timerange>(timerange::BUFR::create(6*3600, 254)));

	ensure(o != timerange::GRIB1::create(2, 254, 3, 4));
	ensure(o != timerange::BUFR::create(5, 1));
	ensure(o != timerange::BUFR::create(6, 0));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_TIMERANGE);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "BUFR,6h");
	Matcher m = Matcher::parse("timerange:" + o->exactQuery());
	ensure(m(o));
}


// Test Lua functions
template<> template<>
void to::test<12>()
{
#ifdef HAVE_LUA
	Item<Timerange> o = timerange::GRIB1::create(2, 254, 2, 3);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'GRIB1' then return 'style is '..o.style..' instead of GRIB1' end \n"
		"  if o.type ~= 2 then return 'o.type is '..o.type..' instead of 2' end \n"
		"  if o.unit ~= 'second' then return 'o.unit is '..o.unit..' instead of \\'second\\'' end \n"
		"  if o.p1 ~= 2 then return 'o.p1 is '..o.p1..' instead of 2' end \n"
		"  if o.p2 ~= 3 then return 'o.p2 is '..o.p2..' instead of 3' end \n"
		"  if tostring(o) ~= 'GRIB1(002, 002s, 003s)' then return 'tostring gave '..tostring(o)..' instead of GRIB1(002, 002s, 003s)' end \n"
		"  o1 = arki_timerange.grib1(2, 254, 2, 3)\n"
		"  if o ~= o1 then return 'new timerange is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
#endif
}

// Check comparisons
template<> template<>
void to::test<13>()
{
	ensure_compares(
		timerange::GRIB1::create(2, 254, 2, 3),
		timerange::GRIB1::create(2, 254, 2, 4),
		timerange::GRIB1::create(2, 254, 2, 4));
}

// Test computing timedef information
template<> template<>
void to::test<14>()
{
    int val;
    bool issec;

    {
        // GRIB1, forecast at +60min
        Item<Timerange> tr(timerange::GRIB1::create(1, 0, 60, 0));

        ensure(tr->get_forecast_step(val, issec));
        ensure_equals(val, 3600);
        ensure_equals(issec, true);
        ensure(tr->get_proc_type());

        ensure_equals(tr->get_proc_type(), 254);

        ensure(tr->get_proc_duration(val, issec));
        ensure_equals(val, 0);
        ensure_equals(issec, true);
    }

    {
        // GRIB1, average between rt+1h and rt+3h
        Item<Timerange> tr(timerange::GRIB1::create(3, 1, 1, 3));

        ensure(tr->get_forecast_step(val, issec));
        ensure_equals(val, 3 * 3600);
        ensure_equals(issec, true);

        ensure_equals(tr->get_proc_type(), 0);

        ensure(tr->get_proc_duration(val, issec));
        ensure_equals(val, 2 * 3600);
        ensure_equals(issec, true);
    }
}


}

// vim:set ts=4 sw=4:

