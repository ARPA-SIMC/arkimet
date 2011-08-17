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

#include "config.h"

#include <arki/tests/test-utils.h>
#include <arki/values.h>
#include <arki/utils/codec.h>

#include <memory>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils::codec;

struct arki_values_shar {
};
TESTGRP(arki_values);

// Check comparison
template<> template<>
void to::test<1>()
{
	std::auto_ptr<Value> vi1(Value::createInteger(-1));
	std::auto_ptr<Value> vi2(Value::createInteger(-1));
	std::auto_ptr<Value> vi3(Value::createInteger(1));
	std::auto_ptr<Value> vs1(Value::createString("antani"));
	std::auto_ptr<Value> vs2(Value::createString("antani"));
	std::auto_ptr<Value> vs3(Value::createString("blinda"));

	ensure_equals(*vi1, *vi1);
	ensure_equals(*vi1, *vi2);
	ensure(*vi1 != *vi3);
	ensure(*vi2 != *vi3);

	ensure_equals(*vs1, *vs1);
	ensure_equals(*vs1, *vs2);
	ensure(*vs1 != *vs3);
	ensure(*vs2 != *vs3);

	ensure(*vi1 < *vi3);
	ensure(not (*vi1 < *vi1));
	ensure(not (*vi3 < *vi1));
	ensure(*vs1 < *vs3);
	ensure(not (*vs1 < *vs1));
	ensure(not (*vs3 < *vs1));
}

// Check encoding
template<> template<>
void to::test<2>()
{
	std::auto_ptr<Value> zero(Value::createInteger(0));
	std::auto_ptr<Value> one(Value::createInteger(1));
	std::auto_ptr<Value> minusOne(Value::createInteger(-1));
	std::auto_ptr<Value> u6bit(Value::createInteger(30));
	std::auto_ptr<Value> s6bit(Value::createInteger(-31));
	std::auto_ptr<Value> onemillion(Value::createInteger(1000000));
	std::auto_ptr<Value> bignegative(Value::createInteger(-1234567));
	std::auto_ptr<Value> empty(Value::createString(""));
	std::auto_ptr<Value> onechar(Value::createString("a"));
	std::auto_ptr<Value> numstr(Value::createString("12"));
	std::auto_ptr<Value> longname(Value::createString("thisIsAVeryLongNameButFitsIn64byesBecauseIts55BytesLong"));
	std::auto_ptr<Value> escaped(Value::createString("\"\\\'pippo"));
	// Not 6 bits, but 1 byte
	std::auto_ptr<Value> fourtythree(Value::createInteger(43));
	std::auto_ptr<Value> minusfourtythree(Value::createInteger(-43));
	// 2 bytes
	std::auto_ptr<Value> tenthousand(Value::createInteger(10000));
	std::auto_ptr<Value> minustenthousand(Value::createInteger(-10000));

#define TESTENC(var, encsize) do { \
		std::auto_ptr<Value> v; \
		string enc; \
		size_t decsize; \
		Encoder e(enc); \
		(var)->encode(e); \
		ensure_equals(enc.size(), (encsize)); \
		v.reset(Value::decode(enc.data(), enc.size(), decsize)); \
		ensure_equals(decsize, (encsize)); \
		ensure_equals(*v, *(var)); \
		\
		enc = (var)->toString(); \
		v.reset(Value::parse(enc)); \
		ensure_equals(*v, *(var)); \
	} while (0)

	TESTENC(zero, 1u);
	TESTENC(one, 1u);
	TESTENC(minusOne, 1u);
	TESTENC(u6bit, 1u);
	TESTENC(s6bit, 1u);
	TESTENC(onemillion, 4u);
	TESTENC(bignegative, 4u);
	TESTENC(empty, 1u);
	TESTENC(onechar, 2u);
	TESTENC(numstr, 3u);
	TESTENC(longname, 56u);
	TESTENC(escaped, 9u);
	TESTENC(fourtythree, 2u);
	TESTENC(minusfourtythree, 2u);
	TESTENC(tenthousand, 3u);
	TESTENC(minustenthousand, 3u);
}

// Check ValueBag
template<> template<>
void to::test<3>()
{
	ValueBag v1;
	ValueBag v2;
	auto_ptr<Value> val;

	v1.set("test1", Value::createInteger(1));
	v1.set("test2", Value::createInteger(1000000));
	v1.set("test3", Value::createInteger(-20));
	v1.set("test4", Value::createString("1"));

	// Test accessors
	val.reset(Value::createInteger(1));
	ensure_equals(*v1.get("test1"), *val);
	val.reset(Value::createInteger(1000000));
	ensure_equals(*v1.get("test2"), *val);
	val.reset(Value::createInteger(-20));
	ensure_equals(*v1.get("test3"), *val);
	val.reset(Value::createString("1"));
	ensure_equals(*v1.get("test4"), *val);

	ensure_equals(v1.size(), 4u);
	ensure_equals(v2.size(), 0u);

	// Test copy and comparison
	ensure(v1 != v2);
	v2 = v1;

	ensure_equals(v1.size(), 4u);
	ensure_equals(v2.size(), 4u);

	ensure_equals(v1, v2);

	// Test clear
	v2.clear();
	ensure_equals(v1.size(), 4u);
	ensure_equals(v2.size(), 0u);

	// Test encoding and decoding
	std::string enc;
	Encoder e(enc);

	v1.encode(e);
	v2 = ValueBag::decode(enc.data(), enc.size());
	ensure_equals(v1, v2);

	ensure_equals(v1.size(), 4u);
	ensure_equals(v2.size(), 4u);

	v2.clear();
	ensure_equals(v1.size(), 4u);
	ensure_equals(v2.size(), 0u);

	enc = v1.toString();
	ensure_equals(enc, "test1=1, test2=1000000, test3=-20, test4=\"1\"");
	v2 = ValueBag::parse(enc);

	ensure_equals(v1.size(), 4u);
	ensure_equals(v2.size(), 4u);

	ensure_equals(v2, v1);
}

// Check a case where ValueBag seemed to fail (but it actually didn't, the
// problem was elsewhere)
template<> template<>
void to::test<4>()
{
	ValueBag v1;
	ValueBag v2;

	v1.set("blo", Value::createInteger(10));
	v1.set("lat", Value::createInteger(5480000));
	v1.set("lon", Value::createInteger(895000));
	v1.set("sta", Value::createInteger(22));

	v2.set("sta", Value::createInteger(88));

	ensure(!v1.contains(v2));
}

}

// vim:set ts=4 sw=4:
