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
#include <arki/types/product.h>
#include <arki/matcher.h>
#include <wibble/string.h>

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
using namespace wibble;

struct arki_types_product_shar {
};
TESTGRP(arki_types_product);

// Check GRIB1
template<> template<>
void to::test<1>()
{
	Item<Product> o = product::GRIB1::create(1, 2, 3);
	ensure_equals(o->style(), Product::GRIB1);
	const product::GRIB1* v = o->upcast<product::GRIB1>();
	ensure_equals(v->origin(), 1u);
	ensure_equals(v->table(), 2u);
	ensure_equals(v->product(), 3u);

	ensure_equals(o, Item<Product>(product::GRIB1::create(1, 2, 3)));

	ensure(o != product::GRIB1::create(2, 3, 4));
	ensure(o != product::GRIB2::create(1, 2, 3, 4));
	ensure(o != product::BUFR::create(1, 2, 3));
	ValueBag vb;
	vb.set("name", Value::createString("antani"));
	ensure(o != product::BUFR::create(1, 2, 3, vb));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_PRODUCT);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB1,1,2,3");
	Matcher m = Matcher::parse("product:" + o->exactQuery());
	ensure(m(o));
}

// Check GRIB2
template<> template<>
void to::test<2>()
{
	Item<Product> o = product::GRIB2::create(1, 2, 3, 4);
	ensure_equals(o->style(), Product::GRIB2);
	const product::GRIB2* v = o->upcast<product::GRIB2>();
	ensure_equals(v->centre(), 1u);
	ensure_equals(v->discipline(), 2u);
	ensure_equals(v->category(), 3u);
	ensure_equals(v->number(), 4u);

	ensure_equals(o, Item<Product>(product::GRIB2::create(1, 2, 3, 4)));

	ensure(o != product::GRIB1::create(1, 2, 3));
	ensure(o != product::GRIB2::create(2, 3, 4, 5));
	ensure(o != product::BUFR::create(1, 2, 3));
	ValueBag vb;
	vb.set("name", Value::createString("antani"));
	ensure(o != product::BUFR::create(1, 2, 3, vb));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_PRODUCT);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "GRIB2,1,2,3,4");
	Matcher m = Matcher::parse("product:" + o->exactQuery());
	ensure(m(o));
}

// Check BUFR
template<> template<>
void to::test<3>()
{
	ValueBag vb;
	vb.set("name", Value::createString("antani"));
	ValueBag vb1;
	vb1.set("name", Value::createString("antani1"));

	Item<Product> o = product::BUFR::create(1, 2, 3, vb);
	ensure_equals(o->style(), Product::BUFR);
	const product::BUFR* v = o->upcast<product::BUFR>();
	ensure_equals(v->values(), vb);

	ensure_equals(o, Item<Product>(product::BUFR::create(1, 2, 3, vb)));

	ensure(o != product::GRIB1::create(1, 2, 3));
	ensure(o != product::GRIB2::create(1, 2, 3, 4));
	ensure(o != product::BUFR::create(1, 2, 3));
	ensure(o != product::BUFR::create(1, 2, 3, vb1));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_PRODUCT);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "BUFR,1,2,3:name=antani");
	Matcher m = Matcher::parse("product:" + o->exactQuery());
	ensure(m(o));

	ValueBag vb2;
	vb2.set("val", Value::createString("blinda"));
	o = v->addValues(vb2);
	ensure_equals(str::fmt(o), "BUFR(001, 002, 003, name=antani, val=blinda)");
}

// Test Lua functions
template<> template<>
void to::test<4>()
{
#ifdef HAVE_LUA
	Item<Product> o = product::GRIB1::create(1, 2, 3);

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
	ensure_equals(test.run(), "");
#endif
}

// Check comparisons
template<> template<>
void to::test<5>()
{
	ValueBag vb;
	vb.set("name", Value::createString("antani"));
	ValueBag vb1;
	vb1.set("name", Value::createString("blinda"));

	ensure_compares(
		product::GRIB1::create(1, 2, 3),
		product::GRIB1::create(2, 3, 4),
		product::GRIB1::create(2, 3, 4));
	ensure_compares(
		product::GRIB2::create(1, 2, 3, 4),
		product::GRIB2::create(2, 3, 4, 5),
		product::GRIB2::create(2, 3, 4, 5));
	ensure_compares(
		product::BUFR::create(1, 2, 3),
		product::BUFR::create(1, 2, 4),
		product::BUFR::create(1, 2, 4));
	ensure_compares(
		product::BUFR::create(1, 2, 3, vb),
		product::BUFR::create(1, 2, 3, vb1),
		product::BUFR::create(1, 2, 3, vb1));
    ensure_compares(
        product::VM2::create(1),
        product::VM2::create(2),
        product::VM2::create(2));
}

// Check VM2
template<> template<>
void to::test<6>()
{
	Item<Product> o = product::VM2::create(1);
	ensure_equals(o->style(), Product::VM2);
	const product::VM2* v = o->upcast<product::VM2>();
	ensure_equals(v->variable_id(), 1ul);

	ensure_equals(o, Item<Product>(product::VM2::create(1)));

	ensure(o != product::VM2::create(2));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_PRODUCT);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "VM2,1");
	Matcher m = Matcher::parse("product:" + o->exactQuery());
	ensure(m(o));
}

}

// vim:set ts=4 sw=4:
