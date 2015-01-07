/*
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/string.h>
#include <arki/tests/tests.h>
#include <arki/utils/codec.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;

struct arki_utils_codec_shar {
};
TESTGRP(arki_utils_codec);

// Check integer serialisation
template<> template<>
void to::test<1>()
{
    using namespace wibble::tests;
	using namespace utils::codec;

#define TEST_CODEC(name, val, encsize) do { \
		string enc; \
		Encoder e(enc); \
		e.add ## name((val), (encsize)); \
        wassert(actual(enc.size()) == (encsize)); \
        \
        Decoder dec((const unsigned char*)enc.data(), enc.size()); \
        wassert(actual(dec.pop ## name((encsize), "value")) ==  (val)); \
	} while (0)

	TEST_CODEC(UInt, 0u, 1u);
	TEST_CODEC(UInt, 1u, 1u);
	TEST_CODEC(UInt, 10u, 1u);
	TEST_CODEC(UInt, 80u, 1u);
	TEST_CODEC(UInt, 127u, 1u);
	TEST_CODEC(UInt, 128u, 1u);
	TEST_CODEC(UInt, 200u, 1u);
	TEST_CODEC(UInt, 255u, 1u);

	TEST_CODEC(SInt, 0, 1u);
	TEST_CODEC(SInt, 1, 1u);
	TEST_CODEC(SInt, 10, 1u);
	TEST_CODEC(SInt, 80, 1u);
	TEST_CODEC(SInt, 127, 1u);
	TEST_CODEC(SInt, -1, 1u);
	TEST_CODEC(SInt, -100, 1u);
	TEST_CODEC(SInt, -127, 1u);
	TEST_CODEC(SInt, -128, 1u);


	TEST_CODEC(UInt, 0u, 2u);
	TEST_CODEC(UInt, 1u, 2u);
	TEST_CODEC(UInt, 128u, 2u);
	TEST_CODEC(UInt, 256u, 2u);
	TEST_CODEC(UInt, 10000u, 2u);
	TEST_CODEC(UInt, 32000u, 2u);
	TEST_CODEC(UInt, 33000u, 2u);
	TEST_CODEC(UInt, 65535u, 2u);

	TEST_CODEC(SInt, 0, 2u);
	TEST_CODEC(SInt, 1, 2u);
	TEST_CODEC(SInt, 10000, 2u);
	TEST_CODEC(SInt, 30000, 2u);
	TEST_CODEC(SInt, 32767, 2u);
	TEST_CODEC(SInt, -1, 2u);
	TEST_CODEC(SInt, -30000, 2u);
	TEST_CODEC(SInt, -32767, 2u);
	TEST_CODEC(SInt, -32768, 2u);


	TEST_CODEC(UInt, 0u, 3u);
	TEST_CODEC(UInt, 1u, 3u);
	TEST_CODEC(UInt, 128u, 3u);
	TEST_CODEC(UInt, 256u, 3u);
	TEST_CODEC(UInt, 33000u, 3u);
	TEST_CODEC(UInt, 65535u, 3u);
	TEST_CODEC(UInt, 100000u, 3u);
	TEST_CODEC(UInt, 0x7fffffu, 3u);
	TEST_CODEC(UInt, 0xabcdefu, 3u);
	TEST_CODEC(UInt, 0xffffffu, 3u);


	TEST_CODEC(UInt, 0u, 4u);
	TEST_CODEC(UInt, 1u, 4u);
	TEST_CODEC(UInt, 128u, 4u);
	TEST_CODEC(UInt, 256u, 4u);
	TEST_CODEC(UInt, 10000u, 4u);
	TEST_CODEC(UInt, 0xabcdefu, 4u);
	TEST_CODEC(UInt, 0xffffffu, 4u);
	TEST_CODEC(UInt, 0x23456789u, 4u);
	TEST_CODEC(UInt, 0x7fffffffu, 4u);
	TEST_CODEC(UInt, 0xabcdef01u, 4u);
	TEST_CODEC(UInt, 0xffffffffu, 4u);

	TEST_CODEC(SInt, 0, 4u);
	TEST_CODEC(SInt, 1, 4u);
	TEST_CODEC(SInt, 10000, 4u);
	TEST_CODEC(SInt, -10000, 4u);
	TEST_CODEC(SInt, 0x23456789, 4u);
	TEST_CODEC(SInt, -0x23456789, 4u);
	TEST_CODEC(SInt, 0x7fffffff, 4u);
	TEST_CODEC(SInt, -0x7fffffff, 4u);
	// g++ generates a comparison between signed and unsigned integer expressions warning here
	//ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-0x80000000, 4).data(), 4), -0x80000000);

	TEST_CODEC(ULInt, 0ull, 8u);
	TEST_CODEC(ULInt, 1ull, 8u);
	TEST_CODEC(ULInt, 128ull, 8u);
	TEST_CODEC(ULInt, 256ull, 8u);
	TEST_CODEC(ULInt, 10000ull, 8u);
	TEST_CODEC(ULInt, 0xabcdefull, 8u);
	TEST_CODEC(ULInt, 0xffffffull, 8u);
	TEST_CODEC(ULInt, 0x23456789ull, 8u);
	TEST_CODEC(ULInt, 0x7fffffffull, 8u);
	TEST_CODEC(ULInt, 0xabcdef01ull, 8u);
	TEST_CODEC(ULInt, 0xffffffffull, 8u);
	TEST_CODEC(ULInt, 0x2345678901234567ull, 8u);
	TEST_CODEC(ULInt, 0x7fffffffffffffffull, 8u);
	TEST_CODEC(ULInt, 0xabcdef0102030405ull, 8u);
	TEST_CODEC(ULInt, 0xffffffffffffffffull, 8u);

#undef TEST_CODEC
#define TEST_CODEC(name, val, encsize) do { \
		string enc; \
		Encoder e(enc); \
		e.add ## name((val)); \
        wassert(actual(enc.size()) == (encsize)); \
        \
        Decoder dec((const unsigned char*)enc.data(), enc.size()); \
        wassert(actual(dec.pop ## name("value")) == (val)); \
	} while (0)
	TEST_CODEC(Float, 0.0f, 4u);
	TEST_CODEC(Float, 0.1f, 4u);
	TEST_CODEC(Float, 10.0f, 4u);
	TEST_CODEC(Float, 0.0000001f, 4u);
	TEST_CODEC(Float, 10000000.0f, 4u);
	TEST_CODEC(Double, 0.0, 8u);
	TEST_CODEC(Double, 0.1, 8u);
	TEST_CODEC(Double, 10.0, 8u);
	TEST_CODEC(Double, 0.00000000000000001, 8u);
	TEST_CODEC(Double, 10000000000000000.0, 8u);

}

// Check varints
template<> template<>
void to::test<2>()
{
    using namespace wibble::tests;
	using namespace utils::codec;

#define TEST_VARINT(TYPE, val, encsize) do { \
		TYPE v = (val); \
		string enc; \
		Encoder e(enc); \
		e.addVarint(v); \
        wassert(actual(enc.size()) == (encsize)); \
        wassert(actual(enc[encsize-1] & 0x80) == 0); \
        TYPE v1; \
        size_t dsize = decodeVarint(enc.data(), enc.size(), v1); \
        wassert(actual(dsize) == (encsize)); \
        wassert(actual(v) == v1); \
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
	TEST_VARINT(uint64_t, 0x1ffffFFFFLLU, 5u);
	TEST_VARINT(uint64_t, 0x7ffffFFFFLLU, 5u);
	TEST_VARINT(uint64_t, 0x800000000LLU, 6u);
	TEST_VARINT(uint64_t, 0x3FFffffFFFFLLU, 6u);
	TEST_VARINT(uint64_t, 0x40000000000LLU, 7u);
	TEST_VARINT(uint64_t, 0x1FFFFffffFFFFLLU, 7u);
	TEST_VARINT(uint64_t, 0x2000000000000LLU, 8u);
	TEST_VARINT(uint64_t, 0xffFFFFffffFFFFLLU, 8u);
	TEST_VARINT(uint64_t, 0x100FFFFffffFFFFLLU, 9u);
	TEST_VARINT(uint64_t, 0x7fffFFFFffffFFFFLLU, 9u);
	TEST_VARINT(uint64_t, 0x8000FFFFffffFFFFLLU, 10u);
	TEST_VARINT(uint64_t, 0xffffFFFFffffFFFFLLU, 10u);
}

}

// vim:set ts=4 sw=4:
