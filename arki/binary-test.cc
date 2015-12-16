#include "config.h"
#include <arki/tests/tests.h>
#include <arki/binary.h>
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
#define TEST_CODEC(encname, decname, val, encsize) do { \
                vector<uint8_t> enc; \
                BinaryEncoder e(enc); \
                e.add_ ## encname((val), (encsize)); \
                wassert(actual(enc.size()) == (encsize)); \
                \
                BinaryDecoder dec(enc); \
                wassert(actual(dec.pop_ ## decname((encsize), "value")) ==  (val)); \
            } while (0)

            TEST_CODEC(unsigned, uint, 0u, 1u);
            TEST_CODEC(unsigned, uint, 1u, 1u);
            TEST_CODEC(unsigned, uint, 10u, 1u);
            TEST_CODEC(unsigned, uint, 80u, 1u);
            TEST_CODEC(unsigned, uint, 127u, 1u);
            TEST_CODEC(unsigned, uint, 128u, 1u);
            TEST_CODEC(unsigned, uint, 200u, 1u);
            TEST_CODEC(unsigned, uint, 255u, 1u);

            TEST_CODEC(signed, sint, 0, 1u);
            TEST_CODEC(signed, sint, 1, 1u);
            TEST_CODEC(signed, sint, 10, 1u);
            TEST_CODEC(signed, sint, 80, 1u);
            TEST_CODEC(signed, sint, 127, 1u);
            TEST_CODEC(signed, sint, -1, 1u);
            TEST_CODEC(signed, sint, -100, 1u);
            TEST_CODEC(signed, sint, -127, 1u);
            TEST_CODEC(signed, sint, -128, 1u);


            TEST_CODEC(unsigned, uint, 0u, 2u);
            TEST_CODEC(unsigned, uint, 1u, 2u);
            TEST_CODEC(unsigned, uint, 128u, 2u);
            TEST_CODEC(unsigned, uint, 256u, 2u);
            TEST_CODEC(unsigned, uint, 10000u, 2u);
            TEST_CODEC(unsigned, uint, 32000u, 2u);
            TEST_CODEC(unsigned, uint, 33000u, 2u);
            TEST_CODEC(unsigned, uint, 65535u, 2u);

            TEST_CODEC(signed, sint, 0, 2u);
            TEST_CODEC(signed, sint, 1, 2u);
            TEST_CODEC(signed, sint, 10000, 2u);
            TEST_CODEC(signed, sint, 30000, 2u);
            TEST_CODEC(signed, sint, 32767, 2u);
            TEST_CODEC(signed, sint, -1, 2u);
            TEST_CODEC(signed, sint, -30000, 2u);
            TEST_CODEC(signed, sint, -32767, 2u);
            TEST_CODEC(signed, sint, -32768, 2u);


            TEST_CODEC(unsigned, uint, 0u, 3u);
            TEST_CODEC(unsigned, uint, 1u, 3u);
            TEST_CODEC(unsigned, uint, 128u, 3u);
            TEST_CODEC(unsigned, uint, 256u, 3u);
            TEST_CODEC(unsigned, uint, 33000u, 3u);
            TEST_CODEC(unsigned, uint, 65535u, 3u);
            TEST_CODEC(unsigned, uint, 100000u, 3u);
            TEST_CODEC(unsigned, uint, 0x7fffffu, 3u);
            TEST_CODEC(unsigned, uint, 0xabcdefu, 3u);
            TEST_CODEC(unsigned, uint, 0xffffffu, 3u);


            TEST_CODEC(unsigned, uint, 0u, 4u);
            TEST_CODEC(unsigned, uint, 1u, 4u);
            TEST_CODEC(unsigned, uint, 128u, 4u);
            TEST_CODEC(unsigned, uint, 256u, 4u);
            TEST_CODEC(unsigned, uint, 10000u, 4u);
            TEST_CODEC(unsigned, uint, 0xabcdefu, 4u);
            TEST_CODEC(unsigned, uint, 0xffffffu, 4u);
            TEST_CODEC(unsigned, uint, 0x23456789u, 4u);
            TEST_CODEC(unsigned, uint, 0x7fffffffu, 4u);
            TEST_CODEC(unsigned, uint, 0xabcdef01u, 4u);
            TEST_CODEC(unsigned, uint, 0xffffffffu, 4u);

            TEST_CODEC(signed, sint, 0, 4u);
            TEST_CODEC(signed, sint, 1, 4u);
            TEST_CODEC(signed, sint, 10000, 4u);
            TEST_CODEC(signed, sint, -10000, 4u);
            TEST_CODEC(signed, sint, 0x23456789, 4u);
            TEST_CODEC(signed, sint, -0x23456789, 4u);
            TEST_CODEC(signed, sint, 0x7fffffff, 4u);
            TEST_CODEC(signed, sint, -0x7fffffff, 4u);
            // g++ generates a comparison between signed and unsigned integer expressions warning here
            //ensure_equals(decodeSInt((const unsigned char*)encodeSInt(-0x80000000, 4).data(), 4), -0x80000000);

            TEST_CODEC(unsigned, ulint, 0ull, 8u);
            TEST_CODEC(unsigned, ulint, 1ull, 8u);
            TEST_CODEC(unsigned, ulint, 128ull, 8u);
            TEST_CODEC(unsigned, ulint, 256ull, 8u);
            TEST_CODEC(unsigned, ulint, 10000ull, 8u);
            TEST_CODEC(unsigned, ulint, 0xabcdefull, 8u);
            TEST_CODEC(unsigned, ulint, 0xffffffull, 8u);
            TEST_CODEC(unsigned, ulint, 0x23456789ull, 8u);
            TEST_CODEC(unsigned, ulint, 0x7fffffffull, 8u);
            TEST_CODEC(unsigned, ulint, 0xabcdef01ull, 8u);
            TEST_CODEC(unsigned, ulint, 0xffffffffull, 8u);
            TEST_CODEC(unsigned, ulint, 0x2345678901234567ull, 8u);
            TEST_CODEC(unsigned, ulint, 0x7fffffffffffffffull, 8u);
            TEST_CODEC(unsigned, ulint, 0xabcdef0102030405ull, 8u);
            TEST_CODEC(unsigned, ulint, 0xffffffffffffffffull, 8u);

#undef TEST_CODEC
#define TEST_CODEC(name, val, encsize) do { \
                vector<uint8_t> enc; \
                BinaryEncoder e(enc); \
                e.add_ ## name((val)); \
                wassert(actual(enc.size()) == (encsize)); \
                \
                BinaryDecoder dec(enc.data(), enc.size()); \
                wassert(actual(dec.pop_ ## name("value")) == (val)); \
            } while (0)
            TEST_CODEC(float, 0.0f, 4u);
            TEST_CODEC(float, 0.1f, 4u);
            TEST_CODEC(float, 10.0f, 4u);
            TEST_CODEC(float, 0.0000001f, 4u);
            TEST_CODEC(float, 10000000.0f, 4u);
            TEST_CODEC(double, 0.0, 8u);
            TEST_CODEC(double, 0.1, 8u);
            TEST_CODEC(double, 10.0, 8u);
            TEST_CODEC(double, 0.00000000000000001, 8u);
            TEST_CODEC(double, 10000000000000000.0, 8u);
        });
        add_method("varints", []() {
            // Check varints
#define TEST_VARINT(TYPE, val, encsize) do { \
                TYPE v = (val); \
                vector<uint8_t> enc; \
                BinaryEncoder e(enc); \
                e.add_varint(v); \
                wassert(actual(enc.size()) == (encsize)); \
                wassert(actual(enc[encsize-1] & 0x80) == 0); \
                BinaryDecoder dec(enc); \
                TYPE v1 = dec.pop_varint<TYPE>("test value"); \
                wassert(actual((uint64_t)v) == (uint64_t)v1); \
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
                BinaryEncoder e(enc); \
                e.add_raw(s); \
                wassert(actual(enc.size()) == s.size()); \
                BinaryDecoder dec(enc.data(), enc.size()); \
                wassert(actual(dec.pop_string(s.size(), "test")) == val); \
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
                BinaryEncoder e(enc); \
                e.add_string(val); \
                wassert(actual(enc.size()) == s.size()); \
                BinaryDecoder dec(enc.data(), enc.size()); \
                wassert(actual(dec.pop_string(s.size(), "test")) == val); \
            } while (0)

            TEST_CSTRING("");
            TEST_CSTRING("f");
            TEST_CSTRING("fo");
            TEST_CSTRING("foo bar baz");
        });
    }
} test("arki_binary");

}

