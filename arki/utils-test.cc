#include "config.h"
#include <arki/tests/tests.h>
#include <arki/utils.h>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils");

void Tests::register_tests() {

add_method("parse_size", [] {
    wassert(actual(parse_size("0")) == 0u);
    wassert(actual(parse_size("1")) == 1u);
    wassert(actual(parse_size("1b")) == 1u);
    wassert(actual(parse_size("1B")) == 1u);
    wassert(actual(parse_size("2k")) == 2000u);
    wassert(actual(parse_size("2Ki")) == 2048u);
    wassert(actual(parse_size("2Ki")) == 2048u);
    wassert(actual(parse_size("10M")) == 10000000u);
    wassert(actual(parse_size("10Mi")) == (size_t)10 * 1024 * 1024);
    wassert_throws(std::runtime_error, parse_size("10v"));
    wassert_throws(std::runtime_error, parse_size("10bb"));
    wassert_throws(std::runtime_error, parse_size("10cb"));
    wassert_throws(std::runtime_error, parse_size("10bc"));
    wassert_throws(std::runtime_error, parse_size("10kib"));

    // Compatibility with old parser
    wassert(actual(parse_size("1c")) == (size_t)1);
    wassert(actual(parse_size("1kB")) == (size_t)1000);
    wassert(actual(parse_size("1K")) == (size_t)1000);
    wassert(actual(parse_size("1MB")) == (size_t)1000 * 1000);
    wassert(actual(parse_size("1M")) == (size_t)1000 * 1000);
    wassert(actual(parse_size("1GB")) == (size_t)1000 * 1000 * 1000);
    wassert(actual(parse_size("1G")) == (size_t)1000 * 1000 * 1000);
    wassert(actual(parse_size("1TB")) == (size_t)1000 * 1000 * 1000 * 1000);
    wassert(actual(parse_size("1T")) == (size_t)1000 * 1000 * 1000 * 1000);
    wassert(actual(parse_size("1PB")) == (size_t)1000 * 1000 * 1000 * 1000 * 1000);
    wassert(actual(parse_size("1P")) == (size_t)1000 * 1000 * 1000 * 1000 * 1000);
    wassert(actual(parse_size("1EB")) == (size_t)1000 * 1000 * 1000 * 1000 * 1000 * 1000);
    wassert(actual(parse_size("1E")) == (size_t)1000 * 1000 * 1000 * 1000 * 1000 * 1000);
    wassert(actual(parse_size("1ZB")) == (size_t)1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000);
    wassert(actual(parse_size("1Z")) == (size_t)1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000);
    wassert(actual(parse_size("1YB")) == (size_t)1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000);
    wassert(actual(parse_size("1Y")) == (size_t)1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000);
});

}

}
