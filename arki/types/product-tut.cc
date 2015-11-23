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
#include <arki/types/product.h>
#include <arki/tests/lua.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace wibble::tests;

struct arki_types_product_shar {
};
TESTGRP(arki_types_product);

// Check GRIB1
template<> template<>
void to::test<1>()
{
    tests::TestGenericType t("product", "GRIB1(1, 2, 3)");
    t.lower.push_back("GRIB1(1, 1, 1)");
    t.lower.push_back("GRIB1(1, 2, 2)");
    t.higher.push_back("GRIB1(1, 2, 4)");
    t.higher.push_back("GRIB1(2, 3, 4)");
    t.higher.push_back("GRIB2(1, 2, 3, 4)");
    t.higher.push_back("BUFR(1, 2, 3)");
    t.higher.push_back("BUFR(1, 2, 3, name=antani)");
    t.exact_query = "GRIB1,1,2,3";
    wassert(t);

    unique_ptr<Product> o = Product::createGRIB1(1, 2, 3);
    wassert(actual(o->style()) == Product::GRIB1);
    const product::GRIB1* v = dynamic_cast<product::GRIB1*>(o.get());
    wassert(actual(v->origin()) == 1u);
    wassert(actual(v->table()) == 2u);
    wassert(actual(v->product()) == 3u);
}

// Check GRIB2
template<> template<>
void to::test<2>()
{
    tests::TestGenericType t("product", "GRIB2(1, 2, 3, 4)");
    t.lower.push_back("GRIB1(1, 2, 3)");
    t.higher.push_back("GRIB2(1, 2, 3, 4, 5)");
    t.higher.push_back("GRIB2(2, 3, 4, 5)");
    t.higher.push_back("BUFR(1, 2, 3)");
    t.higher.push_back("BUFR(1, 2, 3, name=antani)");
    t.exact_query = "GRIB2,1,2,3,4";
    wassert(t);

    unique_ptr<Product> o = Product::createGRIB2(1, 2, 3, 4);
    wassert(actual(o->style()) == Product::GRIB2);
    const product::GRIB2* v = dynamic_cast<product::GRIB2*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->discipline()) == 2u);
    wassert(actual(v->category()) == 3u);
    wassert(actual(v->number()) == 4u);
    wassert(actual(v->table_version()) == 4u);
    wassert(actual(v->local_table_version()) == 255u);
}

// Check GRIB2 with different table version
template<> template<>
void to::test<3>()
{
    tests::TestGenericType t("product", "GRIB2(1, 2, 3, 4, 5)");
    t.lower.push_back("GRIB1(1, 2, 3)");
    t.lower.push_back("GRIB2(1, 2, 3, 4)");
    t.higher.push_back("GRIB2(1, 2, 3, 4, 6)");
    t.higher.push_back("GRIB2(2, 3, 4, 5)");
    t.higher.push_back("BUFR(1, 2, 3)");
    t.higher.push_back("BUFR(1, 2, 3, name=antani)");
    t.exact_query = "GRIB2,1,2,3,4,5";
    wassert(t);

    unique_ptr<Product> o = Product::createGRIB2(1, 2, 3, 4, 5);
    wassert(actual(o->style()) == Product::GRIB2);
    const product::GRIB2* v = dynamic_cast<product::GRIB2*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->discipline()) == 2u);
    wassert(actual(v->category()) == 3u);
    wassert(actual(v->number()) == 4u);
    wassert(actual(v->table_version()) == 5u);
    wassert(actual(v->local_table_version()) == 255u);
}

// Check GRIB2 with different table version or local table version
template<> template<>
void to::test<4>()
{
    tests::TestGenericType t("product", "GRIB2(1, 2, 3, 4, 4, 5)");
    t.lower.push_back("GRIB1(1, 2, 3)");
    t.lower.push_back("GRIB2(1, 2, 3, 4, 4, 4)");
    t.higher.push_back("GRIB2(1, 2, 3, 4)");
    t.higher.push_back("GRIB2(1, 2, 3, 4, 4, 6)");
    t.higher.push_back("GRIB2(2, 3, 4, 5)");
    t.higher.push_back("BUFR(1, 2, 3)");
    t.higher.push_back("BUFR(1, 2, 3, name=antani)");
    t.exact_query = "GRIB2,1,2,3,4,4,5";
    wassert(t);

    unique_ptr<Product> o = Product::createGRIB2(1, 2, 3, 4, 4, 5);
    wassert(actual(o->style()) == Product::GRIB2);
    const product::GRIB2* v = dynamic_cast<product::GRIB2*>(o.get());
    wassert(actual(v->centre()) == 1u);
    wassert(actual(v->discipline()) == 2u);
    wassert(actual(v->category()) == 3u);
    wassert(actual(v->number()) == 4u);
    wassert(actual(v->table_version()) == 4u);
    wassert(actual(v->local_table_version()) == 5u);
}

// Check BUFR
template<> template<>
void to::test<5>()
{
    tests::TestGenericType t("product", "BUFR(1, 2, 3, name=antani)");
    t.lower.push_back("GRIB1(1, 2, 3)");
    t.lower.push_back("GRIB2(2, 3, 4, 5)");
    t.lower.push_back("BUFR(1, 2, 3)");
    t.higher.push_back("BUFR(1, 2, 3, name=antani1)");
    t.higher.push_back("BUFR(1, 2, 3, name1=antani)");
    t.exact_query = "BUFR,1,2,3:name=antani";
    wassert(t);

    ValueBag vb;
    vb.set("name", Value::createString("antani"));
    unique_ptr<Product> o = Product::createBUFR(1, 2, 3, vb);
    wassert(actual(o->style()) == Product::BUFR);
    product::BUFR* v = dynamic_cast<product::BUFR*>(o.get());
    wassert(actual(v->type()) == 1);
    wassert(actual(v->subtype()) == 2);
    wassert(actual(v->localsubtype()) == 3);
    wassert(actual(v->values()) == vb);

    ValueBag vb2;
    vb2.set("val", Value::createString("blinda"));
    v->addValues(vb2);
	stringstream tmp;
	tmp << *o;
    wassert(actual(tmp.str()) == "BUFR(001, 002, 003, name=antani, val=blinda)");
}

// Check VM2
template<> template<>
void to::test<6>()
{
    tests::TestGenericType t("product", "VM2(1)");
    t.lower.push_back("GRIB1(1, 2, 3)");
    t.lower.push_back("GRIB2(2, 3, 4, 5)");
    t.lower.push_back("BUFR(1, 2, 3)");
    t.lower.push_back("BUFR(1, 2, 3, name=antani)");
    t.higher.push_back("VM2(2)");
    t.exact_query = "VM2,1:bcode=B20013, l1=0, l2=0, lt1=256, lt2=258, p1=0, p2=0, tr=254, unit=m";
    wassert(t);

    unique_ptr<Product> o = Product::createVM2(1);
    wassert(actual(o->style()) == Product::VM2);
    const product::VM2* v = dynamic_cast<product::VM2*>(o.get());
    wassert(actual(v->variable_id()) == 1ul);

    // Test derived values
    ValueBag vb1 = ValueBag::parse("bcode=B20013,lt1=256,l1=0,lt2=258,l2=0,tr=254,p1=0,p2=0,unit=m");
    ensure(product::VM2::create(1)->derived_values() == vb1);
    ValueBag vb2 = ValueBag::parse("bcode=NONONO,lt1=256,l1=0,lt2=258,tr=254,p1=0,p2=0,unit=m");
    ensure(product::VM2::create(1)->derived_values() != vb2);
}

// Test Lua functions
template<> template<>
void to::test<7>()
{
#ifdef HAVE_LUA
    unique_ptr<Product> o = Product::createGRIB1(1, 2, 3);

	arki::tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'GRIB1' then return 'style is '..o.style..' instead of GRIB1' end \n"
		"  if o.origin ~= 1 then return 'o.origin first item is '..o.origin..' instead of 1' end \n"
		"  if o.table ~= 2 then return 'o.table first item is '..o.table..' instead of 2' end \n"
		"  if o.product ~= 3 then return 'o.product first item is '..o.product..' instead of 3' end \n"
		"  if tostring(o) ~= 'GRIB1(001, 002, 003)' then return 'tostring gave '..tostring(o)..' instead of GRIB1(001, 002, 003)' end \n"
		"  o1 = arki_product.grib1(1, 2, 3)\n"
		"  if o ~= o1 then return 'new product is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

    test.pusharg(*o);
    wassert(actual(test.run()) == "");
#endif
}

// Test GRIB2 Lua constructor
template<> template<>
void to::test<8>()
{
#ifdef HAVE_LUA
    arki::tests::Lua test(
        "function test(o) \n"
        "  p = arki_product.grib2(1, 2, 3, 4)\n"
        "  if p.table_version ~= 4 then return 'p.table_version '..p.table_version..' instead of 4' end \n"
        "  if p.local_table_version ~= 255 then return 'p.local_table_version '..p.local_table_version..' instead of 255' end \n"

        "  p = arki_product.grib2(1, 2, 3, 4, 5)\n"
        "  if p.table_version ~= 5 then return 'p.table_version '..p.table_version..' instead of 5' end \n"
        "  if p.local_table_version ~= 255 then return 'p.local_table_version '..p.local_table_version..' instead of 255' end \n"

        "  p = arki_product.grib2(1, 2, 3, 4, 5, 6)\n"
        "  if p.table_version ~= 5 then return 'p.table_version '..p.table_version..' instead of 5' end \n"
        "  if p.local_table_version ~= 6 then return 'p.local_table_version '..p.local_table_version..' instead of 6' end \n"

        "end \n"
    );

    wassert(actual(test.run()) == "");
#endif
}

}

// vim:set ts=4 sw=4:
