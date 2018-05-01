#ifndef ARKI_UTILS_TESTSRUNNER_H
#define ARKI_UTILS_TESTSRUNNER_H

#include <string>
#include <vector>
#include <functional>

namespace arki {
namespace utils {
namespace tests {

struct TestFailed;
struct TestStack;
struct TestCase;
struct TestMethod;


/**
 * Result of running a test method.
 */
struct TestMethodResult
{
    /// Name of the test case
    std::string test_case;

    /// Name of the test method
    std::string test_method;

    /// If non-empty, the test failed with this error
    std::string error_message;

    /// Stack frame of where the error happened
    TestStack* error_stack = nullptr;

    /// If non-empty, the test threw an exception and this is its type ID
    std::string exception_typeid;

    /// True if the test has been skipped
    bool skipped = false;

    /// If the test has been skipped, this is an optional reason
    std::string skipped_reason;


    TestMethodResult(const std::string& test_case, const std::string& test_method)
        : test_case(test_case), test_method(test_method) {}
    ~TestMethodResult();

    void set_failed(TestFailed& e);

    void set_exception(std::exception& e)
    {
        error_message = e.what();
        if (error_message.empty())
            error_message = "test threw an exception with an empty error message";
        exception_typeid = typeid(e).name();
    }

    void set_unknown_exception()
    {
        error_message = "unknown exception caught";
    }

    void set_setup_exception(std::exception& e)
    {
        error_message = "[setup failed: ";
        error_message += e.what();
        error_message += "]";
    }

    void set_teardown_exception(std::exception& e)
    {
        error_message = "[teardown failed: ";
        error_message += e.what();
        error_message += "]";
    }

    bool is_success() const
    {
        return error_message.empty();
    }

    void print_failure_details(FILE* out) const;
};

/**
 * Result of running a whole test case
 */
struct TestCaseResult
{
    /// Name of the test case
    std::string test_case;
    /// Outcome of all the methods that have been run
    std::vector<TestMethodResult> methods;
    /// Set to a non-empty string if the setup method of the test case failed
    std::string fail_setup;
    /// Set to a non-empty string if the teardown method of the test case
    /// failed
    std::string fail_teardown;
    /// Set to true if this test case has been skipped
    bool skipped = false;

    TestCaseResult(const std::string& test_case) : test_case(test_case) {}

    void set_setup_failed()
    {
        fail_setup = "test case setup method threw an unknown exception";
    }

    void set_setup_failed(std::exception& e)
    {
        fail_setup = "test case setup method threw an exception: ";
        fail_setup += e.what();
    }

    void set_teardown_failed()
    {
        fail_teardown = "test case teardown method threw an unknown exception";
    }

    void set_teardown_failed(std::exception& e)
    {
        fail_teardown = "test case teardown method threw an exception: ";
        fail_teardown += e.what();
    }

    void add_test_method(TestMethodResult&& e)
    {
        methods.emplace_back(std::move(e));
    }

    bool is_success() const
    {
        if (!fail_setup.empty() || !fail_teardown.empty()) return false;
        for (const auto& m: methods)
            if (!m.is_success())
                return false;
        return true;
    }
};


/**
 * Abstract interface for the objects that supervise test execution.
 *
 * This can be used for printing progress, or to skip test methods or test
 * cases.
 */
struct TestController
{
    virtual ~TestController() {}

    /**
     * Called before running a test case.
     *
     * @returns true if the test case should be run, false if it should be skipped
     */
    virtual bool test_case_begin(const TestCase& test_case, const TestCaseResult& test_case_result) { return true; }

    /**
     * Called after running a test case.
     */
    virtual void test_case_end(const TestCase& test_case, const TestCaseResult& test_case_result) {}

    /**
     * Called before running a test method.
     *
     * @returns true if the test method should be run, false if it should be skipped
     */
    virtual bool test_method_begin(const TestMethod& test_method, const TestMethodResult& test_method_result) { return true; }

    /**
     * Called after running a test method.
     */
    virtual void test_method_end(const TestMethod& test_method, const TestMethodResult& test_method_result) {}
};

/**
 * Test controller that filters tests via a blacklist/whitelist system
 * containing glob patterns on testcase.testmethod names
 */
struct FilteringTestController : public TestController
{
    /// Any method not matching this glob expression will not be run
    std::string whitelist;

    /// Any method matching this glob expression will not be run
    std::string blacklist;

    bool test_method_should_run(const std::string& fullname) const;
};


/**
 * Simple default implementation of TestController.
 *
 * It does progress printing to stdout and basic glob-based test method
 * filtering.
 */
struct SimpleTestController : public FilteringTestController
{
    /// Output stream
    FILE* output = stdout;

    bool test_case_begin(const TestCase& test_case, const TestCaseResult& test_case_result) override;
    void test_case_end(const TestCase& test_case, const TestCaseResult& test_case_result) override;
    bool test_method_begin(const TestMethod& test_method, const TestMethodResult& test_method_result) override;
    void test_method_end(const TestMethod& test_method, const TestMethodResult& test_method_result) override;
};


/**
 * Verbose implementation of TestController.
 *
 * It does progress printing to stdout and basic glob-based test method
 * filtering.
 */
struct VerboseTestController : public FilteringTestController
{
    /// Output stream
    FILE* output = stdout;

    bool test_case_begin(const TestCase& test_case, const TestCaseResult& test_case_result) override;
    void test_case_end(const TestCase& test_case, const TestCaseResult& test_case_result) override;
    bool test_method_begin(const TestMethod& test_method, const TestMethodResult& test_method_result) override;
    void test_method_end(const TestMethod& test_method, const TestMethodResult& test_method_result) override;
};


/**
 * Test registry.
 *
 * It collects information about all known test cases and takes care of running
 * them.
 */
struct TestRegistry
{
    /// All known test cases
    std::vector<TestCase*> entries;

    /**
     * Register a new test case.
     *
     * No memory management is done: test_case needs to exist for the whole
     * lifetime of TestRegistry.
     */
    void register_test_case(TestCase& test_case);

    /**
     * Iterate on all test methods known by this registry.
     *
     * This method does not change the tests, but it cannot be const because it
     * calls register_tests_once on each TestCase.
     */
    void iterate_test_methods(std::function<void(const TestCase&, const TestMethod&)>);

    /**
     * Run all the registered tests using the given controller
     */
    std::vector<TestCaseResult> run_tests(TestController& controller);

    /// Get the singleton instance of TestRegistry
    static TestRegistry& get();
};


struct TestResultStats
{
    const std::vector<TestCaseResult>& results;
    unsigned methods_ok = 0;
    unsigned methods_failed = 0;
    unsigned methods_skipped = 0;
    unsigned test_cases_ok = 0;
    unsigned test_cases_failed = 0;
    bool success = false;

    TestResultStats(const std::vector<TestCaseResult>& results);

    void print_results(FILE* out);
    void print_stats(FILE* out);
};

}
}
}
#endif
