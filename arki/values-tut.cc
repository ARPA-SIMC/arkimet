/*
 * Copyright (C) 2007  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>
#include <arki/values.h>

#include <memory>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

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
	std::auto_ptr<Value> v;
	std::string enc;
	size_t decsize;


	enc = zero->encode();
	ensure_equals(enc.size(), 1u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 1u);
	ensure_equals(*v, *zero);

	enc = zero->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *zero);


	enc = one->encode();
	ensure_equals(enc.size(), 1u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 1u);
	ensure_equals(*v, *one);

	enc = one->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *one);


	enc = minusOne->encode();
	ensure_equals(enc.size(), 1u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 1u);
	ensure_equals(*v, *minusOne);

	enc = minusOne->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *minusOne);


	enc = u6bit->encode();
	ensure_equals(enc.size(), 1u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 1u);
	ensure_equals(*v, *u6bit);

	enc = u6bit->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *u6bit);


	enc = s6bit->encode();
	ensure_equals(enc.size(), 1u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 1u);
	ensure_equals(*v, *s6bit);

	enc = s6bit->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *s6bit);


	enc = onemillion->encode();
	ensure_equals(enc.size(), 4u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 4u);
	ensure_equals(*v, *onemillion);

	enc = onemillion->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *onemillion);


	enc = bignegative->encode();
	ensure_equals(enc.size(), 4u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 4u);
	ensure_equals(*v, *bignegative);

	enc = bignegative->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *bignegative);


	enc = empty->encode();
	ensure_equals(enc.size(), 1u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 1u);
	ensure_equals(*v, *empty);

	enc = empty->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *empty);


	enc = onechar->encode();
	ensure_equals(enc.size(), 2u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 2u);
	ensure_equals(*v, *onechar);

	enc = onechar->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *onechar);


	enc = numstr->encode();
	ensure_equals(enc.size(), 3u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 3u);
	ensure_equals(*v, *numstr);

	enc = numstr->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *numstr);


	enc = longname->encode();
	ensure_equals(enc.size(), 56u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 56u);
	ensure_equals(*v, *longname);

	enc = longname->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *longname);


	enc = escaped->encode();
	ensure_equals(enc.size(), 9u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 9u);
	ensure_equals(*v, *escaped);

	enc = escaped->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *escaped);


	enc = fourtythree->encode();
	ensure_equals(enc.size(), 2u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 2u);
	ensure_equals(*v, *fourtythree);

	enc = fourtythree->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *fourtythree);


	enc = minusfourtythree->encode();
	ensure_equals(enc.size(), 2u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 2u);
	ensure_equals(*v, *minusfourtythree);

	enc = minusfourtythree->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *minusfourtythree);


	enc = tenthousand->encode();
	ensure_equals(enc.size(), 3u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 3u);
	ensure_equals(*v, *tenthousand);

	enc = tenthousand->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *tenthousand);


	enc = minustenthousand->encode();
	ensure_equals(enc.size(), 3u);
	v.reset(Value::decode(enc.data(), enc.size(), decsize));
	ensure_equals(decsize, 3u);
	ensure_equals(*v, *minustenthousand);

	enc = minustenthousand->toString();
	v.reset(Value::parse(enc));
	ensure_equals(*v, *minustenthousand);
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

	enc = v1.encode();
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

}

// vim:set ts=4 sw=4:
