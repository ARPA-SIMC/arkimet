/*
 * Copyright (C) 2007--2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/utils/codec.h>
#include <arki/types/origin.h>
#include <arki/types/run.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

struct arki_utils_codec_shar {
};
TESTGRP(arki_utils_codec);

// Check integer serialisation
template<> template<>
void to::test<1>()
{
	using namespace utils::codec;

#define TEST_CODEC(name, val, encsize) do { \
		string enc; \
		Encoder e(enc); \
		e.add ## name((val), (encsize)); \
		ensure_equals(enc.size(), (encsize)); \
		\
		Decoder dec((const unsigned char*)enc.data(), enc.size()); \
		ensure_equals(dec.pop ## name((encsize), "value"), (val)); \
	} while (0)

	TEST_CODEC(UInt, 0u, 1);
	TEST_CODEC(UInt, 1u, 1);
	TEST_CODEC(UInt, 10u, 1);
	TEST_CODEC(UInt, 80u, 1);
	TEST_CODEC(UInt, 127u, 1);
	TEST_CODEC(UInt, 128u, 1);
	TEST_CODEC(UInt, 200u, 1);
	TEST_CODEC(UInt, 255u, 1);

	TEST_CODEC(SInt, 0, 1);
	TEST_CODEC(SInt, 1, 1);
	TEST_CODEC(SInt, 10, 1);
	TEST_CODEC(SInt, 80, 1);
	TEST_CODEC(SInt, 127, 1);
	TEST_CODEC(SInt, -1, 1);
	TEST_CODEC(SInt, -100, 1);
	TEST_CODEC(SInt, -127, 1);
	TEST_CODEC(SInt, -128, 1);


	TEST_CODEC(UInt, 0u, 2);
	TEST_CODEC(UInt, 1u, 2);
	TEST_CODEC(UInt, 128u, 2);
	TEST_CODEC(UInt, 256u, 2);
	TEST_CODEC(UInt, 10000u, 2);
	TEST_CODEC(UInt, 32000u, 2);
	TEST_CODEC(UInt, 33000u, 2);
	TEST_CODEC(UInt, 65535u, 2);

	TEST_CODEC(SInt, 0, 2);
	TEST_CODEC(SInt, 1, 2);
	TEST_CODEC(SInt, 10000, 2);
	TEST_CODEC(SInt, 30000, 2);
	TEST_CODEC(SInt, 32767, 2);
	TEST_CODEC(SInt, -1, 2);
	TEST_CODEC(SInt, -30000, 2);
	TEST_CODEC(SInt, -32767, 2);
	TEST_CODEC(SInt, -32768, 2);


	TEST_CODEC(UInt, 0u, 3);
	TEST_CODEC(UInt, 1u, 3);
	TEST_CODEC(UInt, 128u, 3);
	TEST_CODEC(UInt, 256u, 3);
	TEST_CODEC(UInt, 33000u, 3);
	TEST_CODEC(UInt, 65535u, 3);
	TEST_CODEC(UInt, 100000u, 3);
	TEST_CODEC(UInt, 0x7fffffu, 3);
	TEST_CODEC(UInt, 0xabcdefu, 3);
	TEST_CODEC(UInt, 0xffffffu, 3);


	TEST_CODEC(UInt, 0u, 4);
	TEST_CODEC(UInt, 1u, 4);
	TEST_CODEC(UInt, 128u, 4);
	TEST_CODEC(UInt, 256u, 4);
	TEST_CODEC(UInt, 10000u, 4);
	TEST_CODEC(UInt, 0xabcdefu, 4);
	TEST_CODEC(UInt, 0xffffffu, 4);
	TEST_CODEC(UInt, 0x23456789u, 4);
	TEST_CODEC(UInt, 0x7fffffffu, 4);
	TEST_CODEC(UInt, 0xabcdef01u, 4);
	TEST_CODEC(UInt, 0xffffffffu, 4);

	TEST_CODEC(SInt, 0, 4);
	TEST_CODEC(SInt, 1, 4);
	TEST_CODEC(SInt, 10000, 4);
	TEST_CODEC(SInt, -10000, 4);
	TEST_CODEC(SInt, 0x23456789, 4);
	TEST_CODEC(SInt, -0x23456789, 4);
	TEST_CODEC(SInt, 0x7fffffff, 4);
	TEST_CODEC(SInt, -0x7fffffff, 4);
	// g++ generates a comparison between signed and unsigned integer expressions warning here
	//ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-0x80000000, 4).data(), 4), -0x80000000);

	TEST_CODEC(ULInt, 0ull, 8);
	TEST_CODEC(ULInt, 1ull, 8);
	TEST_CODEC(ULInt, 128ull, 8);
	TEST_CODEC(ULInt, 256ull, 8);
	TEST_CODEC(ULInt, 10000ull, 8);
	TEST_CODEC(ULInt, 0xabcdefull, 8);
	TEST_CODEC(ULInt, 0xffffffull, 8);
	TEST_CODEC(ULInt, 0x23456789ull, 8);
	TEST_CODEC(ULInt, 0x7fffffffull, 8);
	TEST_CODEC(ULInt, 0xabcdef01ull, 8);
	TEST_CODEC(ULInt, 0xffffffffull, 8);
	TEST_CODEC(ULInt, 0x2345678901234567ull, 8);
	TEST_CODEC(ULInt, 0x7fffffffffffffffull, 8);
	TEST_CODEC(ULInt, 0xabcdef0102030405ull, 8);
	TEST_CODEC(ULInt, 0xffffffffffffffffull, 8);

#undef TEST_CODEC
#define TEST_CODEC(name, val, encsize) do { \
		string enc; \
		Encoder e(enc); \
		e.add ## name((val)); \
		ensure_equals(enc.size(), (encsize)); \
		\
		Decoder dec((const unsigned char*)enc.data(), enc.size()); \
		ensure_equals(dec.pop ## name("value"), (val)); \
	} while (0)
	TEST_CODEC(Float, 0.0f, 4);
	TEST_CODEC(Float, 0.1f, 4);
	TEST_CODEC(Float, 10.0f, 4);
	TEST_CODEC(Float, 0.0000001f, 4);
	TEST_CODEC(Float, 10000000.0f, 4);
	TEST_CODEC(Double, 0.0, 8);
	TEST_CODEC(Double, 0.1, 8);
	TEST_CODEC(Double, 10.0, 8);
	TEST_CODEC(Double, 0.00000000000000001, 8);
	TEST_CODEC(Double, 10000000000000000.0, 8);

}

// Check varints
template<> template<>
void to::test<2>()
{
	using namespace utils::codec;

#define TEST_VARINT(TYPE, val, encsize) do { \
		TYPE v = (val); \
		string enc; \
		Encoder e(enc); \
		e.addVarint(v); \
		ensure_equals(enc.size(), (encsize)); \
		ensure((enc[encsize-1] & 0x80) == 0); \
		TYPE v1; \
		size_t dsize = decodeVarint(enc.data(), enc.size(), v1); \
		ensure_equals(dsize, (encsize)); \
		ensure_equals(v, v1); \
	} while (0)

	TEST_VARINT(uint8_t, 0x1, 1u);
	TEST_VARINT(uint8_t, 27, 1u);
	TEST_VARINT(uint8_t, 0x7f, 1u);
	TEST_VARINT(uint8_t, 0x80, 2u);
	TEST_VARINT(uint8_t, 0xFF, 2u);
	TEST_VARINT(uint16_t, 0x100, 2u);
	TEST_VARINT(uint16_t, 0x7FFF, 3u);
	TEST_VARINT(uint16_t, 0xFFFF, 3u);
	TEST_VARINT(uint32_t, 0x1fFFFF, 3u);
	TEST_VARINT(uint32_t, 0x200000, 4u);
	TEST_VARINT(uint32_t, 0x7fffFFFF, 5u);
	TEST_VARINT(uint32_t, 0xffffFFFF, 5u);
	TEST_VARINT(uint64_t, 0xffffFFFF, 5u);
	TEST_VARINT(uint64_t, 0x1ffffFFFF, 5u);
	TEST_VARINT(uint64_t, 0x7ffffFFFF, 5u);
	TEST_VARINT(uint64_t, 0x800000000, 6u);
	TEST_VARINT(uint64_t, 0x3FFffffFFFF, 6u);
	TEST_VARINT(uint64_t, 0x40000000000, 7u);
	TEST_VARINT(uint64_t, 0x1FFFFffffFFFF, 7u);
	TEST_VARINT(uint64_t, 0x2000000000000, 8u);
	TEST_VARINT(uint64_t, 0xffFFFFffffFFFF, 8u);
	TEST_VARINT(uint64_t, 0x100FFFFffffFFFF, 9u);
	TEST_VARINT(uint64_t, 0x7fffFFFFffffFFFF, 9u);
	TEST_VARINT(uint64_t, 0x8000FFFFffffFFFF, 10u);
	TEST_VARINT(uint64_t, 0xffffFFFFffffFFFF, 10u);
}

}

// vim:set ts=4 sw=4:
