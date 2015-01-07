/*
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/timerange.h>
#include <arki/tests/lua.h>

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_timerange_shar {
};
TESTGRP(arki_types_timerange);

// Check GRIB1 with seconds
template<> template<>
void to::test<1>()
{
    tests::TestGenericType tgt("timerange", "GRIB1(2, 254, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 2, 3)");
    tgt.higher.push_back("GRIB1(2, 254, 3, 4)");
    tgt.exact_query = "GRIB1, 002, 002s, 003s";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB1(2, 254, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB1);
    const timerange::GRIB1* v = dynamic_cast<timerange::GRIB1*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 254);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);

    timerange::GRIB1Unit u;
    int t, p1, p2;
    bool use_p1, use_p2;
    v->getNormalised(t, u, p1, p2, use_p1, use_p2);
    wassert(actual(t) == 2);
    wassert(actual(u) == timerange::SECOND);
    wassert(actual(p1) == 2);
    wassert(actual(p2) == 3);
}

// Check GRIB1 with hours
template<> template<>
void to::test<2>()
{
    tests::TestGenericType tgt("timerange", "GRIB1(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 2, 3)");
    tgt.higher.push_back("GRIB1(2, 1, 3, 4)");
    tgt.higher.push_back("GRIB1(2, 254, 3, 4)");
    tgt.exact_query = "GRIB1, 002, 002h, 003h";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB1(2, 1, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB1);
    const timerange::GRIB1* v = dynamic_cast<timerange::GRIB1*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 1);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);

    timerange::GRIB1Unit u;
    int t, p1, p2;
    bool use_p1, use_p2;
    v->getNormalised(t, u, p1, p2, use_p1, use_p2);
    wassert(actual(t) == 2);
    wassert(actual(u) == timerange::SECOND);
    wassert(actual(p1) == 2 * 3600);
    wassert(actual(p2) == 3 * 3600);
}

// Check GRIB1 with years
template<> template<>
void to::test<3>()
{
    tests::TestGenericType tgt("timerange", "GRIB1(2, 4, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 3, 4)");
    tgt.higher.push_back("GRIB1(2, 1, 2, 3)");
    tgt.higher.push_back("GRIB1(2, 254, 2, 3)");
    tgt.exact_query = "GRIB1, 002, 002y, 003y";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB1(2, 4, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB1);
    const timerange::GRIB1* v = dynamic_cast<timerange::GRIB1*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 4);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);

    timerange::GRIB1Unit u;
    int t, p1, p2;
    bool use_p1, use_p2;
    v->getNormalised(t, u, p1, p2, use_p1, use_p2);
    wassert(actual(t) == 2);
    wassert(actual(u) == timerange::MONTH);
    wassert(actual(p1) == 2 * 12);
    wassert(actual(p2) == 3 * 12);
}

// Check GRIB1 with unknown values
template<> template<>
void to::test<4>()
{
    tests::TestGenericType tgt("timerange", "GRIB1(250, 1, 124, 127)");
    tgt.lower.push_back("GRIB1(250, 1, 124, 126)");
    tgt.higher.push_back("GRIB1(250, 4, 124, 127)");
    tgt.higher.push_back("GRIB1(250, 254, 124, 127)");
    tgt.exact_query = "GRIB1, 250, 124h, 127h";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB1(250, 1, 124, 127);
    wassert(actual(o->style()) == Timerange::GRIB1);
    const timerange::GRIB1* v = dynamic_cast<timerange::GRIB1*>(o.get());
    wassert(actual(v->type()) == 250);
    wassert(actual(v->unit()) == 1);
    wassert(actual(v->p1()) == 124);
    wassert(actual(v->p2()) == 127);

    timerange::GRIB1Unit u;
    int t, p1, p2;
    bool use_p1, use_p2;
    v->getNormalised(t, u, p1, p2, use_p1, use_p2);
    wassert(actual(t) == 2);
    wassert(actual(u) == timerange::SECOND);
    wassert(actual(p1) == 124 * 3600);
    wassert(actual(p2) == 127 * 3600);
}

// Check GRIB2 with seconds
template<> template<>
void to::test<5>()
{
    tests::TestGenericType tgt("timerange", "GRIB2(2, 254, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.higher.push_back("GRIB1(2, 254, 3, 4)");
    tgt.exact_query = "GRIB2,2,254,2,3";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB2(2, 254, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB2);
    const timerange::GRIB2* v = dynamic_cast<timerange::GRIB2*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 254);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);
}

// Check GRIB2 with hours
template<> template<>
void to::test<6>()
{
    tests::TestGenericType tgt("timerange", "GRIB2(2, 1, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 1, 3, 4)");
    tgt.higher.push_back("GRIB1(2, 254, 2, 3)");
    tgt.exact_query = "GRIB2,2,1,2,3";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB2(2, 1, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB2);
    const timerange::GRIB2* v = dynamic_cast<timerange::GRIB2*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 1);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);
}

// Check GRIB2 with years
template<> template<>
void to::test<7>()
{
    tests::TestGenericType tgt("timerange", "GRIB2(2, 4, 2, 3)");
    tgt.lower.push_back("GRIB1(2, 4, 3, 4)");
    tgt.lower.push_back("GRIB1(2, 1, 3, 4)");
    tgt.higher.push_back("GRIB1(2, 254, 2, 3)");
    tgt.exact_query = "GRIB2,2,4,2,3";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB2(2, 1, 2, 3);
    wassert(actual(o->style()) == Timerange::GRIB2);
    const timerange::GRIB2* v = dynamic_cast<timerange::GRIB2*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 4);
    wassert(actual(v->p1()) == 2);
    wassert(actual(v->p2()) == 3);
}

// Check GRIB2 with negative values
template<> template<>
void to::test<8>()
{
    tests::TestGenericType tgt("timerange", "GRIB2(2, 1, -2, -3)");
    tgt.lower.push_back("GRIB1(2, 4, -2, -3)");
    tgt.lower.push_back("GRIB1(2, 1, 2, 3)");
    tgt.higher.push_back("GRIB1(2, 254, -2, -3)");
    tgt.exact_query = "GRIB2,2,1,-2,-3";
    wassert(tgt);

    auto_ptr<Timerange> o = Timerange::createGRIB2(2, 1, -2, -3);
    wassert(actual(o->style()) == Timerange::GRIB2);
    const timerange::GRIB2* v = dynamic_cast<timerange::GRIB2*>(o.get());
    wassert(actual(v->type()) == 2);
    wassert(actual(v->unit()) == 1);
    wassert(actual((int)v->p1()) == -2);
    wassert(actual((int)v->p2()) == -3);
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
    wassert(actual(o).serializes());

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

// Check Timedef with step and statistical processing
template<> template<>
void to::test<11>()
{
    using namespace timerange;
    Item<Timedef> v = Timedef::createFromYaml("6h,2,60m");
    ensure_equals(v->style(), Timerange::TIMEDEF);
    ensure_equals(v->step_unit(), Timedef::UNIT_HOUR);
    ensure_equals(v->step_len(), 6u);
    ensure_equals(v->stat_type(), 2);
    ensure_equals(v->stat_unit(), Timedef::UNIT_MINUTE);
    ensure_equals(v->stat_len(), 60u);

    ensure_equals(v, Item<Timerange>(timerange::Timedef::create(6, Timedef::UNIT_HOUR, 2, 60, Timedef::UNIT_MINUTE)));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("360m, 2, 1h")));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("21600s,2,3600s")));

    ensure(v != timerange::Timedef::createFromYaml("5h,2,60m"));
    ensure(v != timerange::Timedef::createFromYaml("6h"));
    ensure(v != timerange::Timedef::createFromYaml("6h,3,60m"));
    ensure(v != timerange::Timedef::createFromYaml("6h,2"));
    ensure(v != timerange::Timedef::createFromYaml("6h,2,61m"));
    ensure(v != timerange::Timedef::createFromYaml("6mo,2,60m"));

    // Test encoding/decoding
    Item<Timerange> o(v);
    wassert(actual(o).serializes());

    // Test generating a matcher expression
    ensure_equals(v->exactQuery(), "Timedef,6h,2,60m");
    Matcher m = Matcher::parse("timerange:" + v->exactQuery());
    ensure(m(v));
}

// Check Timedef with step and statistical processing, in months
template<> template<>
void to::test<12>()
{
    using namespace timerange;
    Item<Timedef> v = timerange::Timedef::createFromYaml("1y,2,3mo");
    ensure_equals(v->style(), Timerange::TIMEDEF);
    ensure_equals(v->step_unit(), Timedef::UNIT_YEAR);
    ensure_equals(v->step_len(), 1u);
    ensure_equals(v->stat_type(), 2);
    ensure_equals(v->stat_unit(), Timedef::UNIT_MONTH);
    ensure_equals(v->stat_len(), 3u);

    ensure_equals(v, Item<Timerange>(timerange::Timedef::create(1, Timedef::UNIT_YEAR, 2, 3, Timedef::UNIT_MONTH)));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("12mo, 2, 3mo")));

    ensure(v != timerange::Timedef::createFromYaml("2y,2,3mo"));
    ensure(v != timerange::Timedef::createFromYaml("1y"));
    ensure(v != timerange::Timedef::createFromYaml("1y,3,3mo"));
    ensure(v != timerange::Timedef::createFromYaml("1y,2"));
    ensure(v != timerange::Timedef::createFromYaml("1y,2,4mo"));

    // Test encoding/decoding
    Item<Timerange> o(v);
    wassert(actual(o).serializes());

    // Test generating a matcher expression
    ensure_equals(v->exactQuery(), "Timedef,1y,2,3mo");
    Matcher m = Matcher::parse("timerange:" + v->exactQuery());
    ensure(m(v));
}

// Check Timedef with step only
template<> template<>
void to::test<13>()
{
    using namespace timerange;
    Item<Timedef> v = timerange::Timedef::createFromYaml("1d");
    ensure_equals(v->style(), Timerange::TIMEDEF);
    ensure_equals(v->step_unit(), Timedef::UNIT_DAY);
    ensure_equals(v->step_len(), 1u);
    ensure_equals(v->stat_type(), 255);
    ensure_equals(v->stat_unit(), Timedef::UNIT_MISSING);
    ensure_equals(v->stat_len(), 0u);

    ensure_equals(v, Item<Timerange>(timerange::Timedef::create(1, Timedef::UNIT_DAY)));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("24h")));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("1440m")));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("86400s")));

    ensure(v != timerange::Timedef::createFromYaml("2d"));
    ensure(v != timerange::Timedef::createFromYaml("1d,0"));
    ensure(v != timerange::Timedef::createFromYaml("1d,0,0s"));
    ensure(v != timerange::Timedef::createFromYaml("1mo"));

    // Test encoding/decoding
    Item<Timerange> o(v);
    wassert(actual(o).serializes());

    // Test generating a matcher expression
    ensure_equals(v->exactQuery(), "Timedef,1d,-");
    Matcher m = Matcher::parse("timerange:" + v->exactQuery());
    ensure(m(v));
}

// Check Timedef with step only, in months
template<> template<>
void to::test<14>()
{
    using namespace timerange;
    Item<Timedef> v = timerange::Timedef::createFromYaml("2ce");
    ensure_equals(v->style(), Timerange::TIMEDEF);
    ensure_equals(v->step_unit(), Timedef::UNIT_CENTURY);
    ensure_equals(v->step_len(), 2u);
    ensure_equals(v->stat_type(), 255);
    ensure_equals(v->stat_unit(), Timedef::UNIT_MISSING);
    ensure_equals(v->stat_len(), 0u);

    ensure_equals(v, Item<Timerange>(timerange::Timedef::create(2, Timedef::UNIT_CENTURY)));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("20de")));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("200y")));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("2400mo")));

    ensure(v != timerange::Timedef::createFromYaml("1ce"));
    ensure(v != timerange::Timedef::createFromYaml("2ce,0"));
    ensure(v != timerange::Timedef::createFromYaml("2ce,0,0s"));
    ensure(v != timerange::Timedef::createFromYaml("2d"));

    // Test encoding/decoding
    Item<Timerange> o(v);
    wassert(actual(o).serializes());

    // Test generating a matcher expression
    ensure_equals(v->exactQuery(), "Timedef,2ce,-");
    Matcher m = Matcher::parse("timerange:" + v->exactQuery());
    ensure(m(v));
}

// Check Timedef2 with step, and only statistical process type
template<> template<>
void to::test<15>()
{
    using namespace timerange;
    Item<Timedef> v = Timedef::createFromYaml("6h,2");
    ensure_equals(v->style(), Timerange::TIMEDEF);
    ensure_equals(v->step_unit(), Timedef::UNIT_HOUR);
    ensure_equals(v->step_len(), 6u);
    ensure_equals(v->stat_type(), 2);
    ensure_equals(v->stat_unit(), Timedef::UNIT_MISSING);
    ensure_equals(v->stat_len(), 0u);

    ensure_equals(v, Item<Timerange>(timerange::Timedef::create(6, Timedef::UNIT_HOUR, 2, 0, Timedef::UNIT_MISSING)));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("360m, 2")));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("21600s,2")));

    ensure(v != timerange::Timedef::createFromYaml("5h,2"));
    ensure(v != timerange::Timedef::createFromYaml("6h"));
    ensure(v != timerange::Timedef::createFromYaml("6h,3"));
    ensure(v != timerange::Timedef::createFromYaml("6h,2,60m"));

    // Test encoding/decoding
    Item<Timerange> o(v);
    wassert(actual(o).serializes());

    // Test generating a matcher expression
    ensure_equals(v->exactQuery(), "Timedef,6h,2,-");
    Matcher m = Matcher::parse("timerange:" + v->exactQuery());
    ensure(m(v));
}

// Check Timedef with step in months, and only statistical process type
template<> template<>
void to::test<16>()
{
    using namespace timerange;// Check Timedef with seconds
    Item<Timedef> v = Timedef::createFromYaml("6no,2");
    ensure_equals(v->style(), Timerange::TIMEDEF);
    ensure_equals(v->step_unit(), Timedef::UNIT_NORMAL);
    ensure_equals(v->step_len(), 6u);
    ensure_equals(v->stat_type(), 2);
    ensure_equals(v->stat_unit(), Timedef::UNIT_MISSING);
    ensure_equals(v->stat_len(), 0u);

    ensure_equals(v, Item<Timerange>(timerange::Timedef::create(6, Timedef::UNIT_NORMAL, 2, 0, Timedef::UNIT_MISSING)));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("180y, 2")));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("18de, 2")));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("2160mo, 2")));

    ensure(v != timerange::Timedef::createFromYaml("5no,2"));
    ensure(v != timerange::Timedef::createFromYaml("6no"));
    ensure(v != timerange::Timedef::createFromYaml("6no,3"));
    ensure(v != timerange::Timedef::createFromYaml("6no,2,60d"));

    // Test encoding/decoding
    Item<Timerange> o(v);
    wassert(actual(o).serializes());

    // Test generating a matcher expression
    ensure_equals(v->exactQuery(), "Timedef,6no,2,-");
    Matcher m = Matcher::parse("timerange:" + v->exactQuery());
    ensure(m(v));
}

// Check Timedef with step and statistical processing, one in seconds and one in months
template<> template<>
void to::test<17>()
{
    using namespace timerange;
    Item<Timedef> v = timerange::Timedef::createFromYaml("1y,2,3d");
    ensure_equals(v->style(), Timerange::TIMEDEF);
    ensure_equals(v->step_unit(), Timedef::UNIT_YEAR);
    ensure_equals(v->step_len(), 1u);
    ensure_equals(v->stat_type(), 2);
    ensure_equals(v->stat_unit(), Timedef::UNIT_DAY);
    ensure_equals(v->stat_len(), 3u);

    ensure_equals(v, Item<Timerange>(timerange::Timedef::create(1, Timedef::UNIT_YEAR, 2, 3, Timedef::UNIT_DAY)));
    ensure_equals(v, Item<Timerange>(timerange::Timedef::createFromYaml("12mo, 2, 72h")));

    ensure(v != timerange::Timedef::createFromYaml("2y,2,3d"));
    ensure(v != timerange::Timedef::createFromYaml("1y"));
    ensure(v != timerange::Timedef::createFromYaml("1y,3,3d"));
    ensure(v != timerange::Timedef::createFromYaml("1y,2"));
    ensure(v != timerange::Timedef::createFromYaml("1y,2,4d"));

    // Test encoding/decoding
    Item<Timerange> o(v);
    wassert(actual(o).serializes());

    // Test generating a matcher expression
    ensure_equals(v->exactQuery(), "Timedef,1y,2,3d");
    Matcher m = Matcher::parse("timerange:" + v->exactQuery());
    ensure(m(v));
}

// Check BUFR
template<> template<>
void to::test<18>()
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
    wassert(actual(o).serializes());

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "BUFR,6h");
	Matcher m = Matcher::parse("timerange:" + o->exactQuery());
	ensure(m(o));
}


// Test Lua functions
template<> template<>
void to::test<19>()
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
void to::test<20>()
{
    wassert(actual(timerange::GRIB1::create(2, 254, 2, 3)).compares(
                timerange::GRIB1::create(2, 254, 2, 4),
                timerange::GRIB1::create(2, 254, 2, 4)));
}

// Test computing timedef information
template<> template<>
void to::test<21>()
{
    int val;
    bool issec;

    {
        // GRIB1, forecast at +60min
        Item<Timerange> tr(timerange::GRIB1::create(0, 0, 60, 0));

        ensure(tr->get_forecast_step(val, issec));
        ensure_equals(val, 3600);
        ensure_equals(issec, true);

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

// Test Timedef's validity_time_to_emission_time
template<> template<>
void to::test<22>()
{
    using namespace timerange;
    using namespace reftime;
    Item<Timedef> v = Timedef::createFromYaml("6h");
    Item<Position> p = Reftime::decodeString("2009-02-13 12:00:00").upcast<Position>();
    Item<Position> p1 = v->validity_time_to_emission_time(p);
    ensure_equals(p1->time->vals[0], 2009);
    ensure_equals(p1->time->vals[1], 2);
    ensure_equals(p1->time->vals[2], 13);
    ensure_equals(p1->time->vals[3], 6);
    ensure_equals(p1->time->vals[4], 0);
    ensure_equals(p1->time->vals[5], 0);
}

}

// vim:set ts=4 sw=4:

