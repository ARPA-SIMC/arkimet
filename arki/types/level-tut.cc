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
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_level_shar {
};
TESTGRP(arki_types_level);

// Check GRIB1 without l1 and l2
template<> template<>
void to::test<1>()
{
    tests::TestGenericType t("level", "GRIB1(1)");
    t.lower.push_back("GRIB1(0)");
    t.higher.push_back("GRIB1(1, 1, 0)");
    t.higher.push_back("GRIB1(1, 1, 1)");
    t.higher.push_back("GRIB1(2)");
    t.higher.push_back("GRIB1(2, 3, 4)");
    t.higher.push_back("GRIB2S(100, 100, 500)");
    t.exact_query = "GRIB1,1";
    wassert(t);

    auto_ptr<Level> o = Level::createGRIB1(1);
    wassert(actual(o->style()) == Level::GRIB1);
    const level::GRIB1* v = dynamic_cast<level::GRIB1*>(o.get());
    wassert(actual(v->type()) == 1u);
    wassert(actual(v->l1()) == 0u);
    wassert(actual(v->l2()) == 0u);
    wassert(actual(v->valType()) == 0);
}

// Check GRIB1 with an 8 bit l1
template<> template<>
void to::test<2>()
{
    tests::TestGenericType t("level", "GRIB1(103, 132)");
    t.lower.push_back("GRIB1(1)");
    t.lower.push_back("GRIB1(100, 132)");
    t.higher.push_back("GRIB1(103, 133)");
    t.higher.push_back("GRIB2S(100, 100, 500)");
    t.exact_query = "GRIB1,103,132";
    wassert(t);

    auto_ptr<Level> o = Level::createGRIB1(103, 132);
    wassert(actual(o->style()) == Level::GRIB1);
    const level::GRIB1* v = dynamic_cast<level::GRIB1*>(o.get());
    wassert(actual(v->type()) == 103);
    wassert(actual(v->l1()) == 132);
    wassert(actual(v->l2()) == 0);
    wassert(actual(v->valType()) == 1);

    wassert(actual(o) == Level::createGRIB1(103, 132));
    wassert(actual(o) == Level::createGRIB1(103, 132, 0));
    wassert(actual(o) == Level::createGRIB1(103, 132, 1));
}

// Check GRIB1 with a 16 bit l1
template<> template<>
void to::test<3>()
{
    tests::TestGenericType t("level", "GRIB1(103, 13200)");
    t.lower.push_back("GRIB1(1)");
    t.lower.push_back("GRIB1(100, 132)");
    t.higher.push_back("GRIB1(103, 13201)");
    t.higher.push_back("GRIB2S(100, 100, 500)");
    t.exact_query = "GRIB1,103,13200";
    wassert(t);

    auto_ptr<Level> o = Level::createGRIB1(103, 13200);
    wassert(actual(o->style()) == Level::GRIB1);
    const level::GRIB1* v = dynamic_cast<level::GRIB1*>(o.get());
    wassert(actual(v->type()) == 103);
    wassert(actual(v->l1()) == 13200);
    wassert(actual(v->l2()) == 0);
    wassert(actual(v->valType()) == 1);

    wassert(actual(o) == Level::createGRIB1(103, 132));
}

// Check GRIB1 with a layer
template<> template<>
void to::test<4>()
{
    tests::TestGenericType t("level", "GRIB1(104, 132, 231)");
    t.lower.push_back("GRIB1(1)");
    t.lower.push_back("GRIB1(100, 132)");
    t.lower.push_back("GRIB1(103, 13201)");
    t.higher.push_back("GRIB1(104, 133, 231)");
    t.higher.push_back("GRIB1(104, 132, 232)");
    t.higher.push_back("GRIB2S(100, 100, 500)");
    t.exact_query = "GRIB1,104,132,231";
    wassert(t);

    auto_ptr<Level> o = Level::createGRIB1(104, 132, 231);
    wassert(actual(o->style()) == Level::GRIB1);
    const level::GRIB1* v = dynamic_cast<level::GRIB1*>(o.get());
    wassert(actual(v->type()) == 104);
    wassert(actual(v->l1()) == 132);
    wassert(actual(v->l2()) == 231);
    wassert(actual(v->valType()) == 2);
}

// Check GRIB2S
template<> template<>
void to::test<5>()
{
    tests::TestGenericType t("level", "GRIB2S(100, 100, 500)");
    t.lower.push_back("GRIB1(1)");
    t.lower.push_back("GRIB1(100)");
    t.lower.push_back("GRIB1(100, 100)");
    t.lower.push_back("GRIB2S( 99, 100, 500)");
    t.lower.push_back("GRIB2S(100,  99, 500)");
    t.lower.push_back("GRIB2S(100, 100, 499)");
    t.higher.push_back("GRIB2S(101, 100, 500)");
    t.higher.push_back("GRIB2S(100, 101, 500)");
    t.higher.push_back("GRIB2S(100, 100, 501)");
    t.exact_query = "GRIB2S,100,100,500";
    wassert(t);

    auto_ptr<Level> o = Level::createGRIB2S(100, 100, 500);
    wassert(actual(o->style()) == Level::GRIB2S);
    const level::GRIB2S* v = dynamic_cast<level::GRIB2S*>(o.get());
    wassert(actual(v->type()) == 100);
    wassert(actual(v->scale()) == 100);
    wassert(actual(v->value()) == 500);
}

// Check GRIB2S with missing values
template<> template<>
void to::test<6>()
{
    tests::TestGenericType t("level", "GRIB2S(-, -, -)");
    t.lower.push_back("GRIB1(1)");
    t.lower.push_back("GRIB1(100)");
    t.lower.push_back("GRIB1(100, 100)");
    t.higher.push_back("GRIB2S(1, -, -)");
    t.higher.push_back("GRIB2S(-, 1, -)");
    t.higher.push_back("GRIB2S(-, -, 1)");
    t.higher.push_back("GRIB2S(1, 1, 5)");
    t.exact_query = "GRIB2S,-,-,-";
    wassert(t);

    auto_ptr<Level> o = Level::createGRIB2S(level::GRIB2S::MISSING_TYPE, level::GRIB2S::MISSING_SCALE, level::GRIB2S::MISSING_VALUE);
    wassert(actual(o->style()) == Level::GRIB2S);
    const level::GRIB2S* v = dynamic_cast<level::GRIB2S*>(o.get());
    wassert(actual(v->type()) == level::GRIB2S::MISSING_TYPE);
    wassert(actual(v->scale()) == level::GRIB2S::MISSING_SCALE);
    wassert(actual(v->value()) == level::GRIB2S::MISSING_VALUE);
}

// Check GRIB2D
template<> template<>
void to::test<7>()
{
    tests::TestGenericType t("level", "GRIB2D(100, 100, 500, 100, 100, 1000)");
    t.lower.push_back("GRIB1(1)");
    t.lower.push_back("GRIB1(100)");
    t.lower.push_back("GRIB1(100, 100)");
    t.lower.push_back("GRIB2S(1, -, -)");
    t.higher.push_back("GRIB2S(101, 100, 500, 100, 100, 1000)");
    t.higher.push_back("GRIB2S(100, 101, 500, 100, 100, 1000)");
    t.higher.push_back("GRIB2S(100, 100, 501, 100, 100, 1000)");
    t.higher.push_back("GRIB2S(100, 100, 500, 101, 100, 1000)");
    t.higher.push_back("GRIB2S(100, 100, 500, 100, 101, 1000)");
    t.higher.push_back("GRIB2S(100, 100, 500, 100, 100, 1001)");
    t.exact_query = "GRIB2D,100,100,500,100,100,1000";
    wassert(t);

    auto_ptr<Level> o = Level::createGRIB2D(100, 100, 500, 100, 100, 1000);
    wassert(actual(o->style()) == Level::GRIB2D);
    const level::GRIB2D* v = dynamic_cast<level::GRIB2D*>(o.get());
    wassert(actual(v->type1()) == 100);
    wassert(actual(v->scale1()) == 100);
    wassert(actual(v->value1()) == 500);
    wassert(actual(v->type1()) == 100);
    wassert(actual(v->scale1()) == 100);
    wassert(actual(v->value1()) == 1000);
}

// Check GRIB2D with missing values
template<> template<>
void to::test<8>()
{
    tests::TestGenericType t("level", "GRIB2D(-, -, -, -, -, -)");
    t.lower.push_back("GRIB1(1)");
    t.lower.push_back("GRIB1(100)");
    t.lower.push_back("GRIB1(100, 100)");
    t.lower.push_back("GRIB2S(1, -, -)");
    t.higher.push_back("GRIB2S(1, -, -, -, -, -)");
    t.higher.push_back("GRIB2S(-, 1, -, -, -, -)");
    t.higher.push_back("GRIB2S(-, -, 1, -, -, -)");
    t.higher.push_back("GRIB2S(-, -, -, 1, -, -)");
    t.higher.push_back("GRIB2S(-, -, -, -, 1, -)");
    t.higher.push_back("GRIB2S(-, -, -, -, -, 1)");
    t.higher.push_back("GRIB2S(100, 100, 500, 100, 100, 1000)");
    t.exact_query = "GRIB2D,-,-,-,-,-,-";
    wassert(t);

    auto_ptr<Level> o = Level::createGRIB2D(
            level::GRIB2S::MISSING_TYPE, level::GRIB2S::MISSING_SCALE, level::GRIB2S::MISSING_VALUE,
            level::GRIB2S::MISSING_TYPE, level::GRIB2S::MISSING_SCALE, level::GRIB2S::MISSING_VALUE);
    wassert(actual(o->style()) == Level::GRIB2D);
    const level::GRIB2D* v = dynamic_cast<level::GRIB2D*>(o.get());
    wassert(actual(v->type1()) == level::GRIB2S::MISSING_TYPE);
    wassert(actual(v->scale1()) == level::GRIB2S::MISSING_SCALE);
    wassert(actual(v->value1()) == level::GRIB2S::MISSING_VALUE);
    wassert(actual(v->type1()) == level::GRIB2S::MISSING_TYPE);
    wassert(actual(v->scale1()) == level::GRIB2S::MISSING_SCALE);
    wassert(actual(v->value1()) == level::GRIB2S::MISSING_VALUE);
}


// Check ODIMH5
template<> template<>
void to::test<9>()
{
    tests::TestGenericType t("level", "ODIMH5(10.123, 20.123)");
    t.lower.push_back("GRIB1(1)");
    t.lower.push_back("GRIB1(100)");
    t.lower.push_back("GRIB1(100, 100)");
    t.lower.push_back("GRIB2S(1, -, -)");
    t.lower.push_back("GRIB2S(101, 100, 500, 100, 100, 1000)");
    t.higher.push_back("ODIMH5(10.124, 20.123)");
    t.higher.push_back("ODIMH5(10.123, 20.124)");
    t.exact_query = "ODIMH5,range 10.123 20.123";
    wassert(t);

    auto_ptr<Level> o = Level::createODIMH5(10.123, 20.123);
    wassert(actual(o->style()) == Level::ODIMH5);
    const level::ODIMH5* v = dynamic_cast<level::ODIMH5*>(o.get());
    wassert(actual(v->max()) == 20.123);
    wassert(actual(v->min()) == 10.123);
}

// Test Lua functions
template<> template<>
void to::test<10>()
{
#ifdef HAVE_LUA
    auto_ptr<Level> o = Level::createGRIB1(104, 132, 231);

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
    wassert(actual(test.run()) == "");
#endif
}

}

// vim:set ts=4 sw=4:
