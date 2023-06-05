#ifndef ARKI_UTILS_TESTS_H
#define ARKI_UTILS_TESTS_H

/**
 * @author Enrico Zini <enrico@enricozini.org>, Peter Rockai (mornfall) <me@mornfall.net>
 * @brief Utility functions for the unit tests
 *
 * Copyright (C) 2006--2007  Peter Rockai (mornfall) <me@mornfall.net>
 * Copyright (C) 2003--2017  Enrico Zini <enrico@debian.org>
 */

#include <string>
#include <sstream>
#include <exception>
#include <functional>
#include <vector>
#include <cstdint>

namespace arki {
namespace utils {
namespace tests {
struct LocationInfo;
}
}
}

/*
 * These global arguments will be shadowed by local variables in functions that
 * implement tests.
 *
 * They are here to act as default root nodes to fulfill method signatures when
 * tests are called from outside other tests.
 */
extern const arki::utils::tests::LocationInfo arki_utils_test_location_info;

namespace arki {
namespace utils {
namespace tests {

/**
 * Add information to the test backtrace for the tests run in the current
 * scope.
 *
 * Example usage:
 * \code
 * test_function(...)
 * {
 *    ARKI_UTILS_TEST_INFO(info);
 *    for (unsigned i = 0; i < 10; ++i)
 *    {
 *       info() << "Iteration #" << i;
 *       ...
 *    }
 * }
 * \endcode
 */
struct LocationInfo : public std::stringstream
{
    LocationInfo() {}

    /**
     * Clear the current information and return the output stream to which new
     * information can be sent
     */
    std::ostream& operator()();
};

/// Information about one stack frame in the test execution stack
struct TestStackFrame
{
    const char* file;
    int line;
    const char* call;
    std::string local_info;

    TestStackFrame(const char* file, int line, const char* call)
        : file(file), line(line), call(call)
    {
    }

    TestStackFrame(const char* file, int line, const char* call, const LocationInfo& local_info)
        : file(file), line(line), call(call), local_info(local_info.str())
    {
    }

    std::string format() const;

    void format(std::ostream& out) const;
};

struct TestStack : public std::vector<TestStackFrame>
{
    using vector::vector;

    /// Return the formatted backtrace for this location
    std::string backtrace() const;

    /// Write the formatted backtrace for this location to \a out
    void backtrace(std::ostream& out) const;
};

/**
 * Exception thrown when a test assertion fails, normally by
 * Location::fail_test
 */
struct TestFailed : public std::exception
{
    std::string message;
    TestStack stack;

    TestFailed(const std::exception& e);

    template<typename ...Args>
    TestFailed(const std::exception& e, Args&&... args)
        : TestFailed(e)
    {
        add_stack_info(std::forward<Args>(args)...);
    }

    TestFailed(const std::string& message) : message(message) {}

    template<typename ...Args>
    TestFailed(const std::string& message, Args&&... args)
        : TestFailed(message)
    {
        add_stack_info(std::forward<Args>(args)...);
    }

    const char* what() const noexcept override { return message.c_str(); }

    template<typename ...Args>
    void add_stack_info(Args&&... args) { stack.emplace_back(std::forward<Args>(args)...); }
};

/**
 * Exception thrown when a test or a test case needs to be skipped
 */
struct TestSkipped : public std::exception
{
    std::string reason;

    TestSkipped();
    TestSkipped(const std::string& reason);
};

/**
 * Use this to declare a local variable with the given name that will be
 * picked up by tests as extra local info
 */
#define ARKI_UTILS_TEST_INFO(name) \
    arki::utils::tests::LocationInfo arki_utils_test_location_info; \
    arki::utils::tests::LocationInfo& name = arki_utils_test_location_info


/**
 * The following assert_* functions throw TestFailed without capturing
 * file/line numbers, and need to be used inside wassert to give good error
 * messages. Do not use them in actual test cases, but you can use them to
 * implement test assertions.
 */

/// Test function that ensures that the actual value is true
template<typename A>
void assert_true(const A& actual)
{
    if (actual) return;
    std::stringstream ss;
    ss << "actual value " << actual << " is not true";
    throw TestFailed(ss.str());
}

void assert_true(std::nullptr_t actual);

/// Test function that ensures that the actual value is false
template<typename A>
void assert_false(const A& actual)
{
    if (!actual) return;
    std::stringstream ss;
    ss << "actual value " << actual << " is not false";
    throw TestFailed(ss.str());
}

void assert_false(std::nullptr_t actual);

template<typename LIST>
static inline void _format_list(std::ostream& o, const LIST& list) {
    bool first = true;
    o << "[";
    for (const auto& v: list)
    {
        if (first)
            first = false;
        else
            o << ", ";
        o << v;
    }
    o << "]";
}

template<typename T>
void assert_equal(const std::vector<T>& actual, const std::vector<T>& expected)
{
    if (actual == expected) return;
    std::stringstream ss;
    ss << "value ";
    _format_list(ss, actual);
    ss << " is different than the expected ";
    _format_list(ss, expected);
    throw TestFailed(ss.str());
}

template<typename T>
void assert_equal(const std::vector<T>& actual, const std::initializer_list<T>& expected)
{
    if (actual == expected) return;
    std::stringstream ss;
    ss << "value ";
    _format_list(ss, actual);
    ss << " is different than the expected ";
    _format_list(ss, expected);
    throw TestFailed(ss.str());
}

/**
 * Test function that ensures that the actual value is the same as a reference
 * one
 */
template<typename A, typename E>
void assert_equal(const A& actual, const E& expected)
{
    if (actual == expected) return;
    std::stringstream ss;
    ss << "value '" << actual << "' is different than the expected '" << expected << "'";
    throw TestFailed(ss.str());
}

/**
 * Test function that ensures that the actual value is different than a
 * reference one
 */
template<typename A, typename E>
void assert_not_equal(const A& actual, const E& expected)
{
    if (actual != expected) return;
    std::stringstream ss;
    ss << "value '" << actual << "' is not different than the expected '" << expected << "'";
    throw TestFailed(ss.str());
}

/// Ensure that the actual value is less than the reference value
template<typename A, typename E>
void assert_less(const A& actual, const E& expected)
{
    if (actual < expected) return;
    std::stringstream ss;
    ss << "value '" << actual << "' is not less than the expected '" << expected << "'";
    throw TestFailed(ss.str());
}

/// Ensure that the actual value is less or equal than the reference value
template<typename A, typename E>
void assert_less_equal(const A& actual, const E& expected)
{
    if (actual <= expected) return;
    std::stringstream ss;
    ss << "value '" << actual << "' is not less than or equals to the expected '" << expected << "'";
    throw TestFailed(ss.str());
}

/// Ensure that the actual value is greater than the reference value
template<typename A, typename E>
void assert_greater(const A& actual, const E& expected)
{
    if (actual > expected) return;
    std::stringstream ss;
    ss << "value '" << actual << "' is not greater than the expected '" << expected << "'";
    throw TestFailed(ss.str());
}

/// Ensure that the actual value is greather or equal than the reference value
template<typename A, typename E>
void assert_greater_equal(const A& actual, const E& expected)
{
    if (actual >= expected) return;
    std::stringstream ss;
    ss << "value '" << actual << "' is not greater than or equals to the expected '" << expected << "'";
    throw TestFailed(ss.str());
}

/// Ensure that the string \a actual starts with \a expected
void assert_startswith(const std::string& actual, const std::string& expected);

/// Ensure that the string \a actual ends with \a expected
void assert_endswith(const std::string& actual, const std::string& expected);

/// Ensure that the string \a actual contains \a expected
void assert_contains(const std::string& actual, const std::string& expected);

/// Ensure that the string \a actual does not contain \a expected
void assert_not_contains(const std::string& actual, const std::string& expected);

/**
 * Ensure that the string \a actual matches the extended regular expression
 * \a expected.
 *
 * The syntax is that of extended regular expression (see man regex(7) ).
 */
void assert_re_matches(const std::string& actual, const std::string& expected);

/**
 * Ensure that the string \a actual does not match the extended regular
 * expression \a expected.
 *
 * The syntax is that of extended regular expression (see man regex(7) ).
 */
void assert_not_re_matches(const std::string& actual, const std::string& expected);


template<class A>
struct Actual
{
    A _actual;
    Actual(const A& actual) : _actual(actual) {}
    ~Actual() {}

    void istrue() const { assert_true(_actual); }
    void isfalse() const { assert_false(_actual); }
    template<typename E> void operator==(const E& expected) const { assert_equal(_actual, expected); }
    template<typename E> void operator!=(const E& expected) const { assert_not_equal(_actual, expected); }
    template<typename E> void operator<(const E& expected) const { return assert_less(_actual, expected); }
    template<typename E> void operator<=(const E& expected) const { return assert_less_equal(_actual, expected); }
    template<typename E> void operator>(const E& expected) const { return assert_greater(_actual, expected); }
    template<typename E> void operator>=(const E& expected) const { return assert_greater_equal(_actual, expected); }
};

struct ActualCString
{
    const char* _actual;
    ActualCString(const char* s) : _actual(s) {}

    void istrue() const { return assert_true(_actual); }
    void isfalse() const { return assert_false(_actual); }
    void operator==(const char* expected) const;
    void operator==(const std::string& expected) const;
    void operator!=(const char* expected) const;
    void operator!=(const std::string& expected) const;
    void operator<(const std::string& expected) const;
    void operator<=(const std::string& expected) const;
    void operator>(const std::string& expected) const;
    void operator>=(const std::string& expected) const;
    void startswith(const std::string& expected) const;
    void endswith(const std::string& expected) const;
    void contains(const std::string& expected) const;
    void not_contains(const std::string& expected) const;
    void matches(const std::string& re) const;
    void not_matches(const std::string& re) const;
};

struct ActualStdString : public Actual<std::string>
{
    ActualStdString(const std::string& s) : Actual<std::string>(s) {}

    using Actual<std::string>::operator==;
    void operator==(const std::vector<uint8_t>& expected) const;
    using Actual<std::string>::operator!=;
    void operator!=(const std::vector<uint8_t>& expected) const;
    void startswith(const std::string& expected) const;
    void endswith(const std::string& expected) const;
    void contains(const std::string& expected) const;
    void not_contains(const std::string& expected) const;
    void matches(const std::string& re) const;
    void not_matches(const std::string& re) const;
};

struct ActualDouble : public Actual<double>
{
    using Actual::Actual;

    void almost_equal(double expected, unsigned places) const;
    void not_almost_equal(double expected, unsigned places) const;
};

template<typename A>
inline Actual<A> actual(const A& actual) { return Actual<A>(actual); }
inline ActualCString actual(const char* actual) { return ActualCString(actual); }
inline ActualCString actual(char* actual) { return ActualCString(actual); }
inline ActualStdString actual(const std::string& actual) { return ActualStdString(actual); }
inline ActualStdString actual(const std::vector<uint8_t>& actual) { return ActualStdString(std::string(actual.begin(), actual.end())); }
inline ActualDouble actual(double actual) { return ActualDouble(actual); }

struct ActualFunction : public Actual<std::function<void()>>
{
    using Actual::Actual;

    void throws(const std::string& what_match) const;
};

inline ActualFunction actual_function(std::function<void()> actual) { return ActualFunction(actual); }

struct ActualFile : public Actual<std::string>
{
    using Actual::Actual;

    void exists() const;
    void not_exists() const;
    void startswith(const std::string& data) const;
    void empty() const;
    void not_empty() const;
    void contents_equal(const std::string& data) const;
    void contents_equal(const std::vector<uint8_t>& data) const;
    void contents_equal(const std::initializer_list<std::string>& lines) const;
    void contents_match(const std::string& data_re) const;
    void contents_match(const std::initializer_list<std::string>& lines_re) const;
};

inline ActualFile actual_file(const std::string& pathname) { return ActualFile(pathname); }

/**
 * Run the given command, raising TestFailed with the appropriate backtrace
 * information if it threw an exception.
 *
 * If the command raises TestFailed, it adds the current stack to its stack
 * information.
 */
#define wassert(...) \
    do { try { \
        __VA_ARGS__ ; \
    } catch (arki::utils::tests::TestFailed& e) { \
        e.add_stack_info(__FILE__, __LINE__, #__VA_ARGS__, arki_utils_test_location_info); \
        throw; \
    } catch (std::exception& e) { \
        throw arki::utils::tests::TestFailed(e, __FILE__, __LINE__, #__VA_ARGS__, arki_utils_test_location_info); \
    } } while(0)

/// Shortcut to check that a given expression returns true
#define wassert_true(...) wassert(actual(__VA_ARGS__).istrue())

/// Shortcut to check that a given expression returns false
#define wassert_false(...) wassert(actual(__VA_ARGS__).isfalse())

/**
 * Ensure that the expression throws the given exception.
 *
 * Returns a copy of the exception, which can be used for further evaluation.
 */
#define wassert_throws(exc, ...) \
    [&]() { try { \
        __VA_ARGS__ ; \
        wfail_test(#__VA_ARGS__ " did not throw " #exc); \
    } catch (TestFailed& e) { \
        throw; \
    } catch (exc& e) { \
        return e; \
    } catch (std::exception& e) { \
        std::string msg(#__VA_ARGS__ " did not throw " #exc " but threw "); \
        msg += typeid(e).name(); \
        msg += " instead"; \
        wfail_test(msg); \
    } }()

/**
 * Call a function returning its result, and raising TestFailed with the
 * appropriate backtrace information if it threw an exception.
 *
 * If the function raises TestFailed, it adds the current stack to its stack
 * information.
 */
#define wcallchecked(func) \
    [&]() { try { \
        return func; \
    } catch (arki::utils::tests::TestFailed& e) { \
        e.add_stack_info(__FILE__, __LINE__, #func, arki_utils_test_location_info); \
        throw; \
    } catch (std::exception& e) { \
        throw arki::utils::tests::TestFailed(e, __FILE__, __LINE__, #func, arki_utils_test_location_info); \
    } }()

/**
 * Fail a test with an error message
 */
#define wfail_test(msg) wassert(throw arki::utils::tests::TestFailed((msg)))

struct TestCase;
struct TestController;
struct TestRegistry;
struct TestCaseResult;
struct TestMethod;
struct TestMethodResult;


/**
 * Test method information
 */
struct TestMethod
{
    /// Name of the test method
    std::string name;

    /// Documentation attached to this test method
    std::string doc;

    /**
     * Main body of the test method.
     *
     * If nullptr, the test will be skipped.
     */
    std::function<void()> test_function;

    TestMethod(const std::string& name)
        : name(name) {}

    TestMethod(const std::string& name, std::function<void()> test_function)
        : name(name), test_function(test_function) {}
};


/**
 * Test case collecting several test methods, and self-registering with the
 * singleton instance of TestRegistry.
 */
struct TestCase
{
    /// Name of the test case
    std::string name;

    /// All registered test methods
    std::vector<TestMethod> methods;

    /// Set to true the first time register_tests_once is run
    bool tests_registered = false;


    TestCase(const std::string& name);
    virtual ~TestCase() {}

    /**
     * Idempotent wrapper for register_tests()
     */
    void register_tests_once();

    /**
     * This will be called before running the test case, to populate it with
     * its test methods.
     *
     * This needs to be reimplemented with a function that will mostly be a
     * sequence of calls to add_method().
     */
    virtual void register_tests() = 0;

    /**
     * Set up the test case before it is run.
     */
    virtual void setup() {}

    /**
     * Clean up after the test case is run
     */
    virtual void teardown() {}

    /**
     * Set up before the test method is run
     */
    virtual void method_setup(TestMethodResult&) {}

    /**
     * Clean up after the test method is run
     */
    virtual void method_teardown(TestMethodResult&) {}

    /**
     * Call setup(), run all the tests that have been registered, then
     * call teardown().
     *
     * Exceptions in setup() and teardown() are caught and reported in
     * TestCaseResult. Test are run using run_test().
     */
    virtual TestCaseResult run_tests(TestController& controller);

    /**
     * Run a test method.
     *
     * Call method_setup(), run all the tests that have been registered, then
     * call method_teardown().
     *
     * Exceptions thrown by the test method are caught and reported in
     * TestMethodResult.
     *
     * Exceptions in method_setup() and method_teardown() are caught and
     * reported in TestMethodResult.
     */
    virtual TestMethodResult run_test(TestController& controller, TestMethod& method);

    /**
     * Register a new test method, with the actual test function to be added
     * later
     */
    TestMethod& add_method(const std::string& name)
    {
        methods.emplace_back(name);
        return methods.back();
    }

    /**
     * Register a new test method
     */
    template<typename ...Args>
    TestMethod& add_method(const std::string& name, std::function<void()> test_function)
    {
        methods.emplace_back(name, test_function);
        return methods.back();
    }

    /**
     * Register a new test method, including documentation
     */
    template<typename ...Args>
    TestMethod& add_method(const std::string& name, const std::string& doc, std::function<void()> test_function)
    {
        methods.emplace_back(name, test_function);
        methods.back().doc = doc;
        return methods.back();
    }
};


/**
 * Base class for test fixtures.
 *
 * A fixture will have a constructor and a destructor to do setup/teardown, and
 * a reset() function to be called inbetween tests.
 *
 * Fixtures do not need to descend from Fixture: this implementation is
 * provided as a default for tests that do not need one, or as a base for
 * fixtures that do not need reset().
 */
struct Fixture
{
    // Called before each test
    void test_setup() {}

    // Called after each test
    void test_teardown() {}
};

template<typename Fixture, typename... Args>
static inline Fixture* fixture_factory(Args... args)
{
    return new Fixture(args...);
}

/**
 * Test case that includes a fixture
 */
template<typename FIXTURE>
class FixtureTestCase : public TestCase
{
public:
    typedef FIXTURE Fixture;

    Fixture* fixture = nullptr;
    std::function<Fixture*()> make_fixture;

    template<typename... Args>
    FixtureTestCase(const std::string& name, Args... args)
        : TestCase(name)
    {
        make_fixture = std::bind(fixture_factory<FIXTURE, Args...>, args...);
    }

    void setup() override
    {
        TestCase::setup();
        fixture = make_fixture();
    }

    void teardown() override
    {
        delete fixture;
        fixture = 0;
        TestCase::teardown();
    }

    void method_setup(TestMethodResult& mr) override
    {
        TestCase::method_setup(mr);
        if (fixture) fixture->test_setup();
    }

    void method_teardown(TestMethodResult& mr) override
    {
        if (fixture) fixture->test_teardown();
        TestCase::method_teardown(mr);
    }

    /**
     * Register a new test method that takes a reference to the fixture as
     * argument.
     */
    template<typename ...Args>
    TestMethod& add_method(const std::string& name, std::function<void(FIXTURE&)> test_function)
    {
        return TestCase::add_method(name, [=]() { test_function(*fixture); });
    }

    /**
     * Register a new test method that takes a reference to the fixture as
     * argument, including documentation
     */
    template<typename ...Args>
    TestMethod& add_method(const std::string& name, const std::string& doc, std::function<void(FIXTURE&)> test_function)
    {
        return TestCase::add_method(name, doc, [=]() { test_function(*fixture); });
    }
};

}
}
}
#endif
