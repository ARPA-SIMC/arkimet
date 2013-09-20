/**
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
#ifndef ARKI_TEST_UTILS_H
#define ARKI_TEST_UTILS_H

#include <wibble/tests.h>
#include <wibble/exception.h>
#include <sstream>

namespace arki {
namespace tests {
struct Location;
struct LocationInfo;
}
}

/*
 * These global arguments will be shadowed by local variables in functions that
 * implement tests.
 *
 * They are here to act as default root nodes to fulfill method signatures when
 * tests are called from outside other tests.
 */
extern const arki::tests::Location arki_test_location;
extern const arki::tests::LocationInfo arki_test_location_info;

namespace arki {
namespace tests {

#define ALWAYS_THROWS __attribute__ ((noreturn))

class Location
{
    const Location* parent;
    const arki::tests::LocationInfo* info;
    const char* file;
    int line;
    const char* args;

    Location(const Location* parent, const arki::tests::LocationInfo& info, const char* file, int line, const char* args);

public:
    Location();
    Location nest(const arki::tests::LocationInfo& info, const char* file, int line, const char* args=0) const;

    void fail_test(const std::string& msg) const ALWAYS_THROWS;
    void backtrace(std::ostream& out) const;
};


struct LocationInfo : public std::stringstream
{
    /**
     * Clear the stringstream and return self.
     *
     * Example usage:
     *
     * test_function(...)
     * {
     *    ARKI_TEST_INFO(info);
     *    for (unsigned i = 0; i < 10; ++i)
     *    {
     *       info() << "Iteration #" << i;
     *       ...
     *    }
     * }
     */
    std::ostream& operator()();
};

#define ARKI_TEST_LOCPRM arki::tests::Location arki_test_location

/// Use this to declare a local variable with the given name that will be
/// picked up by tests as extra local info
#define ARKI_TEST_INFO(name) \
    arki::tests::LocationInfo arki_test_location_info; \
    arki::tests::LocationInfo& name = arki_test_location_info

#define ensure_contains(x, y) arki::tests::impl_ensure_contains(wibble::tests::Location(__FILE__, __LINE__, #x " == " #y), (x), (y))
#define inner_ensure_contains(x, y) arki::tests::impl_ensure_contains(wibble::tests::Location(loc, __FILE__, __LINE__, #x " == " #y), (x), (y))
void impl_ensure_contains(const wibble::tests::Location& loc, const std::string& haystack, const std::string& needle);

#define ensure_not_contains(x, y) arki::tests::impl_ensure_not_contains(wibble::tests::Location(__FILE__, __LINE__, #x " == " #y), (x), (y))
#define inner_ensure_not_contains(x, y) arki::tests::impl_ensure_not_contains(wibble::tests::Location(loc, __FILE__, __LINE__, #x " == " #y), (x), (y))
void impl_ensure_not_contains(const wibble::tests::Location& loc, const std::string& haystack, const std::string& needle);

#define ensure_md_equals(md, type, strval) \
    ensure_equals((md).get<type>(), type::decodeString(strval))

void test_assert_istrue(ARKI_TEST_LOCPRM, bool val);

template <class Expected, class Actual>
void test_assert_equals(ARKI_TEST_LOCPRM, const Expected& expected, const Actual& actual)
{
    if (expected != actual)
    {
        std::stringstream ss;
        ss << "expected '" << expected << "' actual '" << actual << "'";
        arki_test_location.fail_test(ss.str());
    }
}

template <class Expected, class Actual>
void test_assert_gt(ARKI_TEST_LOCPRM, const Expected& expected, const Actual& actual)
{
    if (actual > expected) return;
    std::stringstream ss;
    ss << "value '" << actual << "' not greater than the expected '" << expected << "'";
    arki_test_location.fail_test(ss.str());
}

template <class Expected, class Actual>
void test_assert_gte(ARKI_TEST_LOCPRM, const Expected& expected, const Actual& actual)
{
    if (actual >= expected) return;
    std::stringstream ss;
    ss << "value '" << actual << "' not greater or equal than the expected '" << expected << "'";
    arki_test_location.fail_test(ss.str());
}

template <class Expected, class Actual>
void test_assert_lt(ARKI_TEST_LOCPRM, const Expected& expected, const Actual& actual)
{
    if (actual < expected) return;
    std::stringstream ss;
    ss << "value '" << actual << "' not less than the expected '" << expected << "'";
    arki_test_location.fail_test(ss.str());
}

template <class Expected, class Actual>
void test_assert_lte(ARKI_TEST_LOCPRM, const Expected& expected, const Actual& actual)
{
    if (actual <= expected) return;
    std::stringstream ss;
    ss << "value '" << actual << "' not less than or equal than the expected '" << expected << "'";
    arki_test_location.fail_test(ss.str());
}

void test_assert_startswith(ARKI_TEST_LOCPRM, const std::string& expected, const std::string& actual);
void test_assert_endswith(ARKI_TEST_LOCPRM, const std::string& expected, const std::string& actual);
void test_assert_contains(ARKI_TEST_LOCPRM, const std::string& expected, const std::string& actual);
void test_assert_re_match(ARKI_TEST_LOCPRM, const std::string& regexp, const std::string& actual);
void test_assert_file_exists(ARKI_TEST_LOCPRM, const std::string& fname);
void test_assert_not_file_exists(ARKI_TEST_LOCPRM, const std::string& fname);

#define test_runner(loc, func, ...) \
    do { try { \
        func(loc, ##__VA_ARGS__); \
    } catch (wibble::exception::Generic& e) { \
        loc.fail_test(e.what()); \
    } } while(0)

#if 0
// arki::tests::test_assert_* test
#define atest(test, ...) test_runner(arki::tests::Location(__FILE__, __LINE__, #test ": " #__VA_ARGS__), arki::tests::test_assert_##test, ##__VA_ARGS__)

// function test, just runs the function without mangling its name
#define ftest(test, ...) test_runner(arki::tests::Location(__FILE__, __LINE__, "function: " #test "(" #__VA_ARGS__ ")"), test, ##__VA_ARGS__)
#endif

// arki::tests::test_assert_* test
#define atest(test, ...) test_runner(arki_test_location.nest(arki_test_location_info, __FILE__, __LINE__, #test ": " #__VA_ARGS__), arki::tests::test_assert_##test, ##__VA_ARGS__)

// function test, just runs the function without mangling its name
#define ftest(test, ...) test_runner(arki_test_location.nest(arki_test_location_info, __FILE__, __LINE__, "function: " #test "(" #__VA_ARGS__ ")"), test, ##__VA_ARGS__)

// internal arkimet test, passes existing 'loc'
#define iatest(test, ...) test_runner(arki_test_location.nest(arki_test_location_info, __FILE__, __LINE__, #test ": " #__VA_ARGS__), arki::tests::test_assert_##test, ##__VA_ARGS__)

// internal function test, passes existing 'loc'
#define iftest(test, ...) test_runner(arki_test_location.nest(arki_test_location_info, __FILE__, __LINE__, "function: " #test "(" #__VA_ARGS__ ")"), test, ##__VA_ARGS__)

}
}

#endif
