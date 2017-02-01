/*
 * @author Enrico Zini <enrico@enricozini.org>, Peter Rockai (mornfall) <me@mornfall.net>
 * @brief Utility functions for the unit tests
 *
 * Copyright (C) 2006--2007  Peter Rockai (mornfall) <me@mornfall.net>
 * Copyright (C) 2003--2015  Enrico Zini <enrico@debian.org>
 */

#include "tests.h"
#include "string.h"
#include "sys.h"
#include <fnmatch.h>
#include <cmath>
#include <iomanip>
#include <sys/types.h>
#include <fcntl.h>
#include <regex.h>

using namespace std;
using namespace arki::utils;

const arki::utils::tests::LocationInfo arki_utils_test_location_info;

namespace arki {
namespace utils {
namespace tests {

/*
 * TestStackFrame
 */

std::string TestStackFrame::format() const
{
    std::stringstream ss;
    format(ss);
    return ss.str();
}

void TestStackFrame::format(std::ostream& out) const
{
    out << file << ":" << line << ":" << call;
    if (!local_info.empty())
        out << " [" << local_info << "]";
    out << endl;
}


/*
 * TestStack
 */

void TestStack::backtrace(std::ostream& out) const
{
    for (const auto& frame: *this)
        frame.format(out);
}

std::string TestStack::backtrace() const
{
    std::stringstream ss;
    backtrace(ss);
    return ss.str();
}


/*
 * TestFailed
 */

TestFailed::TestFailed(const std::exception& e)
    : message(typeid(e).name())
{
   message += ": ";
   message += e.what();
}


#if 0
std::string Location::fail_msg(const std::string& error) const
{
    std::stringstream ss;
    ss << "test failed at:" << endl;
    backtrace(ss);
    ss << file << ":" << line << ":error: " << error << endl;
    return ss.str();
}

std::string Location::fail_msg(std::function<void(std::ostream&)> write_error) const
{
    std::stringstream ss;
    ss << "test failed at:" << endl;
    backtrace(ss);
    ss << file << ":" << line << ":error: ";
    write_error(ss);
    ss << endl;
    return ss.str();
}
#endif

std::ostream& LocationInfo::operator()()
{
    str(std::string());
    clear();
    return *this;
}

/*
 * Assertions
 */

void assert_startswith(const std::string& actual, const std::string& expected)
{
    if (str::startswith(actual, expected)) return;
    std::stringstream ss;
    ss << "'" << actual << "' does not start with '" << expected << "'";
    throw TestFailed(ss.str());
}

void assert_endswith(const std::string& actual, const std::string& expected)
{
    if (str::endswith(actual, expected)) return;
    std::stringstream ss;
    ss << "'" << actual << "' does not end with '" << expected << "'";
    throw TestFailed(ss.str());
}

void assert_contains(const std::string& actual, const std::string& expected)
{
    if (actual.find(expected) != std::string::npos) return;
    std::stringstream ss;
    ss << "'" << actual << "' does not contain '" << expected << "'";
    throw TestFailed(ss.str());
}

void assert_not_contains(const std::string& actual, const std::string& expected)
{
    if (actual.find(expected) == std::string::npos) return;
    std::stringstream ss;
    ss << "'" << actual << "' contains '" << expected << "'";
    throw TestFailed(ss.str());
}

namespace {

struct Regexp
{
    regex_t compiled;

    Regexp(const char* regex)
    {
        if (int err = regcomp(&compiled, regex, REG_EXTENDED | REG_NOSUB))
            raise_error(err);
    }
    ~Regexp()
    {
        regfree(&compiled);
    }

    bool search(const char* s)
    {
        return regexec(&compiled, s, 0, nullptr, 0) != REG_NOMATCH;
    }

    void raise_error(int code)
    {
        // Get the size of the error message string
        size_t size = regerror(code, &compiled, nullptr, 0);

        char* buf = new char[size];
        regerror(code, &compiled, buf, size);
        string msg(buf);
        delete[] buf;
        throw std::runtime_error(msg);
    }
};

}

void assert_re_matches(const std::string& actual, const std::string& expected)
{
    Regexp re(expected.c_str());
    if (re.search(actual.c_str())) return;
    std::stringstream ss;
    ss << "'" << actual << "' does not match '" << expected << "'";
    throw TestFailed(ss.str());
}

void assert_not_re_matches(const std::string& actual, const std::string& expected)
{
    Regexp re(expected.c_str());
    if (!re.search(actual.c_str())) return;
    std::stringstream ss;
    ss << "'" << actual << "' should not match '" << expected << "'";
    throw TestFailed(ss.str());
}

void assert_true(std::nullptr_t actual)
{
    throw TestFailed("actual value nullptr is not true");
};

void assert_false(std::nullptr_t actual)
{
};


static void _actual_must_be_set(const char* actual)
{
    if (!actual)
        throw TestFailed("actual value is the null pointer instead of a valid string");
}

void ActualCString::operator==(const char* expected) const
{
    if (expected && _actual)
        assert_equal<std::string, std::string>(_actual, expected);
    else if (!expected && !_actual)
        ;
    else if (expected)
    {
        std::stringstream ss;
        ss << "actual value is nullptr instead of the expected string \"" << str::encode_cstring(expected) << "\"";
        throw TestFailed(ss.str());
    }
    else
    {
        std::stringstream ss;
        ss << "actual value is the string \"" << str::encode_cstring(_actual) << "\" instead of nullptr";
        throw TestFailed(ss.str());
    }
}

void ActualCString::operator==(const std::string& expected) const
{
    _actual_must_be_set(_actual);
    assert_equal<std::string, std::string>(_actual, expected);
}

void ActualCString::operator!=(const char* expected) const
{
    if (expected && _actual)
        assert_not_equal<std::string, std::string>(_actual, expected);
    else if (!expected && !_actual)
        throw TestFailed("actual and expected values are both nullptr but they should be different");
}

void ActualCString::operator!=(const std::string& expected) const
{
    _actual_must_be_set(_actual);
    assert_not_equal<std::string, std::string>(_actual, expected);
}

void ActualCString::operator<(const std::string& expected) const
{
    _actual_must_be_set(_actual);
    assert_less<std::string, std::string>(_actual, expected);
}

void ActualCString::operator<=(const std::string& expected) const
{
    _actual_must_be_set(_actual);
    assert_less_equal<std::string, std::string>(_actual, expected);
}

void ActualCString::operator>(const std::string& expected) const
{
    _actual_must_be_set(_actual);
    assert_greater<std::string, std::string>(_actual, expected);
}

void ActualCString::operator>=(const std::string& expected) const
{
    _actual_must_be_set(_actual);
    assert_greater_equal<std::string, std::string>(_actual, expected);
}

void ActualCString::matches(const std::string& re) const
{
    _actual_must_be_set(_actual);
    assert_re_matches(_actual, re);
}

void ActualCString::not_matches(const std::string& re) const
{
    _actual_must_be_set(_actual);
    assert_not_re_matches(_actual, re);
}

void ActualCString::startswith(const std::string& expected) const
{
    _actual_must_be_set(_actual);
    assert_startswith(_actual, expected);
}

void ActualCString::endswith(const std::string& expected) const
{
    _actual_must_be_set(_actual);
    assert_endswith(_actual, expected);
}

void ActualCString::contains(const std::string& expected) const
{
    _actual_must_be_set(_actual);
    assert_contains(_actual, expected);
}

void ActualCString::not_contains(const std::string& expected) const
{
    _actual_must_be_set(_actual);
    assert_not_contains(_actual, expected);
}

void ActualStdString::startswith(const std::string& expected) const
{
    assert_startswith(_actual, expected);
}

void ActualStdString::endswith(const std::string& expected) const
{
    assert_endswith(_actual, expected);
}

void ActualStdString::contains(const std::string& expected) const
{
    assert_contains(_actual, expected);
}

void ActualStdString::not_contains(const std::string& expected) const
{
    assert_not_contains(_actual, expected);
}

void ActualStdString::matches(const std::string& re) const
{
    assert_re_matches(_actual, re);
}

void ActualStdString::not_matches(const std::string& re) const
{
    assert_not_re_matches(_actual, re);
}

void ActualDouble::almost_equal(double expected, unsigned places) const
{
    if (round((_actual - expected) * exp10(places)) == 0.0)
        return;
    std::stringstream ss;
    ss << std::setprecision(places) << fixed << _actual << " is different than the expected " << expected;
    throw TestFailed(ss.str());
}

void ActualDouble::not_almost_equal(double expected, unsigned places) const
{
    if (round(_actual - expected * exp10(places)) != 0.0)
        return;
    std::stringstream ss;
    ss << std::setprecision(places) << fixed << _actual << " is the same as the expected " << expected;
    throw TestFailed(ss.str());
}

void ActualFunction::throws(const std::string& what_match) const
{
    bool thrown = false;
    try {
        _actual();
    } catch (std::exception& e) {
        thrown = true;
        wassert(actual(e.what()).matches(what_match));
    }
    if (!thrown)
        throw TestFailed("code did not throw any exception");
}

void ActualFile::exists() const
{
    if (sys::exists(_actual)) return;
    throw TestFailed("file " + _actual + " does not exist and it should");
}

void ActualFile::not_exists() const
{
    if (!sys::exists(_actual)) return;
    throw TestFailed("file " + _actual + " exists and it should not");
}

void ActualFile::startswith(const std::string& data) const
{
    sys::File in(_actual, O_RDONLY);
    string buf(data.size(), 0);
    in.read_all_or_throw((void*)buf.data(), buf.size());
    *((char*)buf.data() + buf.size()) = 0;
    if (buf != data)
        throw TestFailed("file " + _actual + " starts with '" + str::encode_cstring(buf) + "' instead of '" + str::encode_cstring(data) + "'");
}

#if 0
void test_assert_file_exists(WIBBLE_TEST_LOCPRM, const std::string& fname)
{
    if (not sys::fs::exists(fname))
    {
        std::stringstream ss;
        ss << "file '" << fname << "' does not exists";
        arki::utils_test_location.fail_test(ss.str());
    }
}

void test_assert_not_file_exists(WIBBLE_TEST_LOCPRM, const std::string& fname)
{
    if (sys::fs::exists(fname))
    {
        std::stringstream ss;
        ss << "file '" << fname << "' does exists";
        arki::utils_test_location.fail_test(ss.str());
    }
}

#if 0
struct TestFileExists
{
    std::string pathname;
    bool inverted;
    TestFileExists(const std::string& pathname, bool inverted=false) : pathname(pathname), inverted(inverted) {}
    TestFileExists operator!() { return TestFileExists(pathname, !inverted); }
    void check(ARKI_UTILS_TEST_LOCPRM) const;
};
#endif

void TestFileExists::check(WIBBLE_TEST_LOCPRM) const
{
    if (!inverted)
    {
        if (sys::fs::exists(pathname)) return;
        std::stringstream ss;
        ss << "file '" << pathname << "' does not exists";
        arki::utils_test_location.fail_test(ss.str());
    } else {
        if (not sys::fs::exists(pathname)) return;
        std::stringstream ss;
        ss << "file '" << pathname << "' exists";
        arki::utils_test_location.fail_test(ss.str());
    }
}
#endif

TestRegistry& TestRegistry::get()
{
    static TestRegistry* instance = 0;
    if (!instance)
        instance = new TestRegistry();
    return *instance;
}

void TestRegistry::register_test_case(TestCase& test_case)
{
    entries.emplace_back(&test_case);
}

std::vector<TestCaseResult> TestRegistry::run_tests(TestController& controller)
{
    std::vector<TestCaseResult> res;
    for (auto& e: entries)
    {
        e->register_tests();
        // TODO: filter on e.name
        res.emplace_back(std::move(e->run_tests(controller)));
    }
    return res;
}

TestCaseResult TestCase::run_tests(TestController& controller)
{
    TestCaseResult res(name);

    if (!controller.test_case_begin(*this, res))
    {
        res.skipped = true;
        controller.test_case_end(*this, res);
        return res;
    }

    try {
        setup();
    } catch (TestSkipped) {
        res.skipped = true;
        controller.test_case_end(*this, res);
        return res;
    } catch (std::exception& e) {
        res.set_setup_failed(e);
        controller.test_case_end(*this, res);
        return res;
    }

    for (auto& m: methods)
    {
        // TODO: filter on m.name
        res.add_test_method(run_test(controller, m));
    }

    try {
        teardown();
    } catch (std::exception& e) {
        res.set_teardown_failed(e);
    }

    controller.test_case_end(*this, res);
    return res;
}

TestMethodResult TestCase::run_test(TestController& controller, TestMethod& method)
{
    TestMethodResult res(name, method.name);

    if (!controller.test_method_begin(method, res))
    {
        res.skipped = true;
        controller.test_method_end(method, res);
        return res;
    }

    bool run = true;
    try {
        method_setup(res);
    } catch (TestSkipped) {
        res.skipped = true;
        controller.test_method_end(method, res);
        return res;
    } catch (std::exception& e) {
        res.set_setup_exception(e);
        run = false;
    }

    if (run)
    {
        try {
            method.test_function();
        } catch (TestSkipped) {
            res.skipped = true;
        } catch (TestFailed& e) {
            // Location::fail_test() was called
            res.set_failed(e);
        } catch (std::exception& e) {
            // std::exception was thrown
            res.set_exception(e);
        } catch (...) {
            // An unknown exception was thrown
            res.set_unknown_exception();
        }
    }

    try {
        method_teardown(res);
    } catch (std::exception& e) {
        res.set_teardown_exception(e);
    }

    controller.test_method_end(method, res);
    return res;
}

bool SimpleTestController::test_method_should_run(const std::string& fullname) const
{
    if (!whitelist.empty() && fnmatch(whitelist.c_str(), fullname.c_str(), 0) == FNM_NOMATCH)
        return false;

    if (!blacklist.empty() && fnmatch(blacklist.c_str(), fullname.c_str(), 0) != FNM_NOMATCH)
        return false;

    return true;
}

bool SimpleTestController::test_case_begin(const TestCase& test_case, const TestCaseResult& test_case_result)
{
    // Skip test case if all its methods should not run
    bool should_run = false;
    for (const auto& m : test_case.methods)
        should_run |= test_method_should_run(test_case.name + "." + m.name);
    if (!should_run) return false;

    fprintf(stdout, "%s: ", test_case.name.c_str());
    fflush(stdout);
    return true;
}

void SimpleTestController::test_case_end(const TestCase& test_case, const TestCaseResult& test_case_result)
{
    if (test_case_result.skipped)
        ;
    else if (test_case_result.is_success())
        fprintf(stdout, "\n");
    else
        fprintf(stdout, "\n");
    fflush(stdout);
}

bool SimpleTestController::test_method_begin(const TestMethod& test_method, const TestMethodResult& test_method_result)
{
    string name = test_method_result.test_case + "." + test_method.name;
    return test_method_should_run(name);
}

void SimpleTestController::test_method_end(const TestMethod& test_method, const TestMethodResult& test_method_result)
{
    if (test_method_result.skipped)
        putc('s', stdout);
    else if (test_method_result.is_success())
        putc('.', stdout);
    else
        putc('x', stdout);
    fflush(stdout);
}

}
}
}
