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
#include <arki/types/origin.h>
#include <arki/tests/lua.h>

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_origin_shar {
};
TESTGRP(arki_types_origin);

// Check GRIB1
template<> template<>
void to::test<1>()
{
    tests::TestGenericType t("origin", "GRIB1(1, 2, 3)");
    t.lower.push_back("GRIB1(1, 1, 1)");
    t.lower.push_back("GRIB1(1, 2, 2)");
    t.higher.push_back("GRIB1(1, 2, 4)");
    t.higher.push_back("GRIB1(2, 3, 4)");
    t.higher.push_back("GRIB2(1, 2, 3, 4, 5)");
    t.higher.push_back("BUFR(1, 2)");
    t.exact_query = "GRIB1,1,2,3";
    wassert(t);

    auto_ptr<Origin> o = Origin::createGRIB1(1, 2, 3);
    wassert(actual(o->style()) == Origin::GRIB1);
    const origin::GRIB1* v = dynamic_cast<origin::GRIB1*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->subcentre()) == 2u);
    wassert(actual(v->process()) == 3u);
}

// Check GRIB2
template<> template<>
void to::test<2>()
{
    tests::TestGenericType t("origin", "GRIB2(1, 2, 3, 4, 5)");
    t.lower.push_back("GRIB1(1, 2, 3)");
    t.lower.push_back("GRIB2(1, 2, 3, 4, 4)");
    t.higher.push_back("GRIB2(1, 2, 3, 4, 6)");
    t.higher.push_back("GRIB2(2, 3, 4, 5, 6)");
    t.higher.push_back("BUFR(1, 2)");
    t.exact_query = "GRIB2,1,2,3,4,5";
    wassert(t);

    auto_ptr<Origin> o = Origin::createGRIB2(1, 2, 3, 4, 5);
    wassert(actual(o->style()) == Origin::GRIB2);
    const origin::GRIB2* v = dynamic_cast<origin::GRIB2*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->subcentre()) == 2u);
    wassert(actual(v->processtype()) == 3u);
    wassert(actual(v->bgprocessid()) == 4u);
    wassert(actual(v->processid()) == 5u);
}

// Check BUFR
template<> template<>
void to::test<3>()
{
    tests::TestGenericType t("origin", "BUFR(1, 2)");
    t.lower.push_back("GRIB1(1, 2, 3)");
    t.lower.push_back("GRIB2(1, 2, 3, 4, 5)");
    t.lower.push_back("BUFR(0, 2)");
    t.lower.push_back("BUFR(1, 1)");
    t.higher.push_back("BUFR(1, 3)");
    t.higher.push_back("BUFR(2, 1)");
    t.exact_query = "BUFR,1,2";
    wassert(t);

    auto_ptr<Origin> o = Origin::createBUFR(1, 2);
    wassert(actual(o->style()) == Origin::BUFR);
    const origin::BUFR* v = dynamic_cast<origin::BUFR*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->subcentre()) == 2u);
}

#ifdef HAVE_LUA
// Test Lua functions
template<> template<>
void to::test<4>()
{
    auto_ptr<Origin> o = Origin::createGRIB1(1, 2, 3);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'GRIB1' then return 'style is '..o.style..' instead of GRIB1' end \n"
		"  if o.centre ~= 1 then return 'o.centre first item is '..o.centre..' instead of 1' end \n"
		"  if o.subcentre ~= 2 then return 'o.subcentre first item is '..o.subcentre..' instead of 2' end \n"
		"  if o.process ~= 3 then return 'o.process first item is '..o.process..' instead of 3' end \n"
		"  if tostring(o) ~= 'GRIB1(001, 002, 003)' then return 'tostring gave '..tostring(o)..' instead of GRIB1(001, 002, 003)' end \n"
		"  local o1 = arki_origin.grib1(1, 2, 3)\n"
		"  if o ~= o1 then return 'new origin is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

    test.pusharg(*o);
    wassert(actual(test.run()) == "");
}
#endif

// Check ODIMH5
template<> template<>
void to::test<5>()
{
    tests::TestGenericType t("origin", "ODIMH5(1, 2, 3)");
    t.lower.push_back("GRIB1(1, 2, 3)");
    t.lower.push_back("GRIB2(1, 2, 3, 4, 5)");
    t.lower.push_back("BUFR(0, 2)");
    t.higher.push_back("ODIMH5(1a, 2, 3)");
    t.higher.push_back("ODIMH5(1, 2a, 3)");
    t.higher.push_back("ODIMH5(1, 2, 3a)");
    t.higher.push_back("ODIMH5(1a, 2a, 3a)");
    t.exact_query = "ODIMH5,1,2,3";
    wassert(t);

    auto_ptr<Origin> o = Origin::createODIMH5("1", "2", "3");
    wassert(actual(o->style()) == Origin::ODIMH5);
    const origin::ODIMH5* v = dynamic_cast<origin::ODIMH5*>(o.get());
    wassert(actual(v->getWMO()) == "1");
    wassert(actual(v->getRAD()) == "2");
    wassert(actual(v->getPLC()) == "3");
}

// Check ODIMH5 with empty strings
template<> template<>
void to::test<6>()
{
    tests::TestGenericType t("origin", "ODIMH5(, 2, 3)");
    t.lower.push_back("GRIB1(1, 2, 3)");
    t.lower.push_back("GRIB2(1, 2, 3, 4, 5)");
    t.lower.push_back("BUFR(0, 2)");
    t.higher.push_back("ODIMH5(1a, 2, 3)");
    t.higher.push_back("ODIMH5(1, 2a, 3)");
    t.higher.push_back("ODIMH5(1, 2, 3a)");
    t.higher.push_back("ODIMH5(1a, 2a, 3a)");
    t.exact_query = "ODIMH5,,2,3";
    wassert(t);

    auto_ptr<Origin> o = Origin::createODIMH5("", "2", "3");
    wassert(actual(o->style()) == Origin::ODIMH5);
    const origin::ODIMH5* v = dynamic_cast<origin::ODIMH5*>(o.get());
    wassert(actual(v->getWMO()) == "");
    wassert(actual(v->getRAD()) == "2");
    wassert(actual(v->getPLC()) == "3");
}

}
