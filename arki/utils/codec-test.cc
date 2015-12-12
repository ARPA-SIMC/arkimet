#include "config.h"
#include <arki/tests/tests.h>
#include <arki/utils/codec.h>
#include <sstream>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override
    {
        add_method("integer", []() {
            // Check integer serialisation
            using namespace utils::codec;

#define TEST_CODEC(name, val, encsize) do { \
                vector<uint8_t> enc; \
                Encoder e(enc); \
                e.add ## name((val), (encsize)); \
                wassert(actual(enc.size()) == (encsize)); \
                \
                Decoder dec(enc.data(), enc.size()); \
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
                vector<uint8_t> enc; \
                Encoder e(enc); \
                e.add ## name((val)); \
                wassert(actual(enc.size()) == (encsize)); \
                \
                Decoder dec(enc.data(), enc.size()); \
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
        });
        add_method("varints", []() {
            // Check varints
            using namespace utils::codec;

#define TEST_VARINT(TYPE, val, encsize) do { \
                TYPE v = (val); \
                vector<uint8_t> enc; \
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
        });
        add_method("strings", []() {
#define TEST_STRING(val) do { \
                string s(val); \
                vector<uint8_t> enc; \
                codec::Encoder e(enc); \
                e.addString(s); \
                wassert(actual(enc.size()) == s.size()); \
                codec::Decoder dec(enc.data(), enc.size()); \
                wassert(actual(dec.popString(s.size(), "test")) == val); \
            } while (0)

            TEST_STRING("");
            TEST_STRING("f");
            TEST_STRING("fo");
            TEST_STRING("foo bar baz");
        });
        add_method("cstrings", []() {
#define TEST_CSTRING(val) do { \
                string s(val); \
                vector<uint8_t> enc; \
                codec::Encoder e(enc); \
                e.addString(val); \
                wassert(actual(enc.size()) == s.size()); \
                codec::Decoder dec(enc.data(), enc.size()); \
                wassert(actual(dec.popString(s.size(), "test")) == val); \
            } while (0)

            TEST_CSTRING("");
            TEST_CSTRING("f");
            TEST_CSTRING("fo");
            TEST_CSTRING("foo bar baz");
        });
    }
} test("arki_utils_codec");

}