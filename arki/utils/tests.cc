/*
 * @author Enrico Zini <enrico@enricozini.org>, Peter Rockai (mornfall) <me@mornfall.net>
 * @brief Utility functions for the unit tests
 *
 * Copyright (C) 2006--2007  Peter Rockai (mornfall) <me@mornfall.net>
 * Copyright (C) 2003--2017  Enrico Zini <enrico@debian.org>
 */

#include "tests.h"
#include "testrunner.h"
#include "string.h"
#include "sys.h"
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


/*
 * TestSkipped
 */

TestSkipped::TestSkipped() {}
TestSkipped::TestSkipped(const std::string& reason) : reason(reason) {}


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
    std::string regex;
    regex_t compiled;
    regmatch_t matches[2];

    Regexp(const char* regex)
        : regex(regex)
    {
        if (int err = regcomp(&compiled, this->regex.c_str(), REG_EXTENDED))
            raise_error(err);
    }
    ~Regexp()
    {
        regfree(&compiled);
    }

    bool search(const char* s)
    {
        return regexec(&compiled, s, 2, matches, 0) != REG_NOMATCH;
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

void ActualStdString::operator==(const std::vector<uint8_t>& expected) const
{
    return operator==(std::string(expected.begin(), expected.end()));
}

void ActualStdString::operator!=(const std::vector<uint8_t>& expected) const
{
    return operator!=(std::string(expected.begin(), expected.end()));
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

void ActualFile::empty() const
{
    size_t size = sys::size(_actual);
    if (size > 0)
        throw TestFailed("file " + _actual + " is " + std::to_string(size) + "b instead of being empty");
}

void ActualFile::not_empty() const
{
    size_t size = sys::size(_actual);
    if (size == 0)
        throw TestFailed("file " + _actual + " is empty and it should not be");
}

void ActualFile::contents_equal(const std::string& data) const
{
    std::string content = sys::read_file(_actual);
    if (content != data)
        throw TestFailed("file " + _actual + " contains '" + str::encode_cstring(content) + "' instead of '" + str::encode_cstring(data) + "'");
}

void ActualFile::contents_equal(const std::vector<uint8_t>& data) const
{
    return contents_equal(std::string(data.begin(), data.end()));
}

void ActualFile::contents_equal(const std::initializer_list<std::string>& lines) const
{
    std::vector<std::string> actual_lines;
    std::string content = str::rstrip(sys::read_file(_actual));

    str::Split splitter(content, "\n");
    std::copy(splitter.begin(), splitter.end(), back_inserter(actual_lines));

    if (actual_lines.size() != lines.size())
        throw TestFailed("file " + _actual + " contains " + std::to_string(actual_lines.size()) + " lines ('" + str::encode_cstring(content) + "') instead of " + std::to_string(lines.size()) + " lines ('" + str::encode_cstring(content) + "')");

    auto ai = actual_lines.begin();
    auto ei = lines.begin();
    for (unsigned i = 0; i < actual_lines.size(); ++i, ++ai, ++ei)
    {
        string actual_line = str::rstrip(*ai);
        string expected_line = str::rstrip(*ei);
        if (*ai != *ei)
            throw TestFailed("file " + _actual + " actual contents differ from expected at line #" + std::to_string(i + 1) + " ('" + str::encode_cstring(actual_line) + "' instead of '" + str::encode_cstring(expected_line) + "')");

    }
}

void ActualFile::contents_match(const std::string& data_re) const
{
    std::string content = sys::read_file(_actual);
    Regexp re(data_re.c_str());
    if (re.search(content.c_str())) return;
    std::stringstream ss;
    ss << "file " + _actual << " contains " << str::encode_cstring(content)
       << " which does not match " << data_re;
    throw TestFailed(ss.str());
}

void ActualFile::contents_match(const std::initializer_list<std::string>& lines_re) const
{
    std::vector<std::string> actual_lines;
    std::string content = str::rstrip(sys::read_file(_actual));

    str::Split splitter(content, "\n");
    std::copy(splitter.begin(), splitter.end(), back_inserter(actual_lines));

    auto ai = actual_lines.begin();
    auto ei = lines_re.begin();
    unsigned lineno = 1;
    while (ei != lines_re.end())
    {
        Regexp re(ei->c_str());
        string actual_line = ai == actual_lines.end() ? "" : str::rstrip(*ai);
        if (re.search(content.c_str()))
        {
            if (re.matches[0].rm_so == re.matches[0].rm_eo)
                ++ei;
            else
            {
                if (ai != actual_lines.end()) ++ai;
                ++ei;
                ++lineno;
            }
            continue;
        }

        std::stringstream ss;
        ss << "file " << _actual << " actual contents differ from expected at line #" << lineno
           << " ('" << str::encode_cstring(actual_line)
           << "' does not match '" << str::encode_cstring(*ei) << "')";
        throw TestFailed(ss.str());
    }
}


/*
 * TestCase
 */

TestCase::TestCase(const std::string& name)
    : name(name)
{
    TestRegistry::get().register_test_case(*this);
}

void TestCase::register_tests_once()
{
    if (tests_registered) return;
    tests_registered = true;
    register_tests();
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

    bool skip_all = false;
    string skip_all_reason;
    try {
        setup();
    } catch (TestSkipped& e) {
        skip_all = true;
        skip_all_reason = e.reason;
    } catch (std::exception& e) {
        res.set_setup_failed(e);
        controller.test_case_end(*this, res);
        return res;
    }

    if (skip_all)
    {
        for (auto& method: methods)
        {
            TestMethodResult tmr(name, method.name);
            controller.test_method_begin(method, tmr);
            tmr.skipped = true;
            tmr.skipped_reason = skip_all_reason;
            controller.test_method_end(method, tmr);
            res.add_test_method(move(tmr));
        }
    } else {
        for (auto& m: methods)
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
    if (!method.test_function)
    {
        // Skip methods with empty test functions
        res.skipped = true;
        controller.test_method_end(method, res);
        return res;
    }

    // Take time now for measuring elapsed time of the test method
    sys::Clock timer(CLOCK_MONOTONIC);

    try {
        method_setup(res);
    } catch (TestSkipped& e) {
        res.skipped = true;
        res.skipped_reason = e.reason;
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
        } catch (TestSkipped& e) {
            res.skipped = true;
            res.skipped_reason = e.reason;
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

    res.elapsed_ns = timer.elapsed();

    controller.test_method_end(method, res);
    return res;
}

}
}
}
