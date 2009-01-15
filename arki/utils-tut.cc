/*
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/utils.h>
#include <arki/types/origin.h>
#include <arki/types/run.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

struct arki_utils_shar {
};
TESTGRP(arki_utils);

// Check integer serialisation
template<> template<>
void to::test<1>()
{
	using namespace utils;
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0, 1).data(), 1), 0u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(1, 1).data(), 1), 1u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(10, 1).data(), 1), 10u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(80, 1).data(), 1), 80u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(127, 1).data(), 1), 127u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(128, 1).data(), 1), 128u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(200, 1).data(), 1), 200u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(255, 1).data(), 1), 255u);

	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(0, 1).data(), 1), 0);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(1, 1).data(), 1), 1);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(10, 1).data(), 1), 10);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(80, 1).data(), 1), 80);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(127, 1).data(), 1), 127);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-1, 1).data(), 1), -1);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-100, 1).data(), 1), -100);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-127, 1).data(), 1), -127);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-128, 1).data(), 1), -128);


	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0, 2).data(), 2), 0u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(1, 2).data(), 2), 1u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(128, 2).data(), 2), 128u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(256, 2).data(), 2), 256u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(10000, 2).data(), 2), 10000u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(32000, 2).data(), 2), 32000u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(33000, 2).data(), 2), 33000u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(65535, 2).data(), 2), 65535u);

	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(0, 2).data(), 2), 0);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(1, 2).data(), 2), 1);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(10000, 2).data(), 2), 10000);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(30000, 2).data(), 2), 30000);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(32767, 2).data(), 2), 32767);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-1, 2).data(), 2), -1);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-30000, 2).data(), 2), -30000);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-32767, 2).data(), 2), -32767);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-32768, 2).data(), 2), -32768);


	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0, 3).data(), 3), 0u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(1, 3).data(), 3), 1u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(128, 3).data(), 3), 128u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(256, 3).data(), 3), 256u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(33000, 3).data(), 3), 33000u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(65535, 3).data(), 3), 65535u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(100000, 3).data(), 3), 100000u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0x7fffff, 3).data(), 3), 0x7fffffu);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0xabcdef, 3).data(), 3), 0xabcdefu);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0xffffff, 3).data(), 3), 0xffffffu);


	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0, 4).data(), 4), 0u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(1, 4).data(), 4), 1u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(128, 4).data(), 4), 128u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(256, 4).data(), 4), 256u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(10000, 4).data(), 4), 10000u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0xabcdef, 4).data(), 4), 0xabcdefu);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0xffffff, 4).data(), 4), 0xffffffu);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0x23456789, 4).data(), 4), 0x23456789u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0x7fffffff, 4).data(), 4), 0x7fffffffu);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0xabcdef01, 4).data(), 4), 0xabcdef01u);
	ensure_equals(decodeUInt((const unsigned char*)encodeUInt(0xffffffff, 4).data(), 4), 0xffffffffu);

	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(0, 4).data(), 4), 0);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(1, 4).data(), 4), 1);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(10000, 4).data(), 4), 10000);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-10000, 4).data(), 4), -10000);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(0x23456789, 4).data(), 4), 0x23456789);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-0x23456789, 4).data(), 4), -0x23456789);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(0x7fffffff, 4).data(), 4), 0x7fffffff);
	ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-0x7fffffff, 4).data(), 4), -0x7fffffff);
	// g++ generates a comparison between signed and unsigned integer expressions warning here
	//ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-0x80000000, 4).data(), 4), -0x80000000);

	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(0, 8).data(), 8), 0ull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(1, 8).data(), 8), 1ull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(128, 8).data(), 8), 128ull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(256, 8).data(), 8), 256ull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(10000, 8).data(), 8), 10000ull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(0xabcdef, 8).data(), 8), 0xabcdefull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(0xffffff, 8).data(), 8), 0xffffffull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(0x23456789, 8).data(), 8), 0x23456789ull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(0x7fffffffull, 8).data(), 8), 0x7fffffffull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(0xabcdef01ull, 8).data(), 8), 0xabcdef01ull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(0xffffffffull, 8).data(), 8), 0xffffffffull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(0x2345678901234567ull, 8).data(), 8), 0x2345678901234567ull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(0x7fffffffffffffffull, 8).data(), 8), 0x7fffffffffffffffull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(0xabcdef0102030405ull, 8).data(), 8), 0xabcdef0102030405ull);
	ensure_equals(decodeULInt((const unsigned char*)encodeULInt(0xffffffffffffffffull, 8).data(), 8), 0xffffffffffffffffull);
}

// Check compareMaps
template<> template<>
void to::test<2>()
{
	using namespace utils;
	map<string, UItem<> > a;
	map<string, UItem<> > b;

	a["antani"] = b["antani"] = types::origin::GRIB1::create(1, 2, 3);
	a["pippo"] = types::run::Minute::create(12);

	ensure_equals(compareMaps(a, b), 1);
	ensure_equals(compareMaps(b, a), -1);
}

}

// vim:set ts=4 sw=4:
