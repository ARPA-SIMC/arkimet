#include "testrunner.h"
#include "tests.h"
#include <fnmatch.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace utils {
namespace tests {

/*
 * TestMethodResult
 */

TestMethodResult::~TestMethodResult()
{
    delete error_stack;
}

void TestMethodResult::set_failed(TestFailed& e)
{
    error_message = e.what();
    error_stack = new TestStack(e.stack);
    if (error_message.empty())
        error_message = "test failed with an empty error message";
}


/*
 * SimpleTestController
 */

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


/*
 * TestRegistry
 */

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

void TestRegistry::iterate_test_methods(std::function<void(const TestCase&, const TestMethod&)> f)
{
    for (auto& e: entries)
    {
        e->register_tests_once();
        for (const auto& m: e->methods)
            f(*e, m);
    }
}

std::vector<TestCaseResult> TestRegistry::run_tests(TestController& controller)
{
    std::vector<TestCaseResult> res;
    for (auto& e: entries)
    {
        e->register_tests_once();
        // TODO: filter on e.name
        res.emplace_back(std::move(e->run_tests(controller)));
    }
    return res;
}


/*
 * TestResultStats
 */

TestResultStats::TestResultStats(const std::vector<TestCaseResult>& results)
    : results(results)
{
    for (const auto& tc_res: results)
    {
        if (!tc_res.fail_setup.empty())
            ++test_cases_failed;
        else {
            if (!tc_res.fail_teardown.empty())
                ++test_cases_failed;
            else
                ++test_cases_ok;

            for (const auto& tm_res: tc_res.methods)
            {
                if (tm_res.skipped)
                    ++methods_skipped;
                else if (tm_res.is_success())
                    ++methods_ok;
                else
                    ++methods_failed;
            }
        }
    }

    success = methods_ok && !test_cases_failed && !methods_failed;
}

void TestResultStats::print_results(FILE* out)
{
    for (const auto& tc_res: results)
    {
        if (!tc_res.fail_setup.empty())
            fprintf(stderr, "%s: %s\n", tc_res.test_case.c_str(), tc_res.fail_setup.c_str());
        else {
            if (!tc_res.fail_teardown.empty())
                fprintf(stderr, "%s: %s\n", tc_res.test_case.c_str(), tc_res.fail_teardown.c_str());

            for (const auto& tm_res: tc_res.methods)
            {
                if (tm_res.skipped || tm_res.is_success()) continue;
                fprintf(stderr, "\n");
                if (tm_res.exception_typeid.empty())
                    fprintf(stderr, "%s.%s: %s\n", tm_res.test_case.c_str(), tm_res.test_method.c_str(), tm_res.error_message.c_str());
                else
                    fprintf(stderr, "%s.%s:[%s] %s\n", tm_res.test_case.c_str(), tm_res.test_method.c_str(), tm_res.exception_typeid.c_str(), tm_res.error_message.c_str());
                for (const auto& frame : *tm_res.error_stack)
                    fprintf(stderr, "  %s", frame.format().c_str());
            }
        }
    }
}

void TestResultStats::print_stats(FILE* out)
{
    if (test_cases_failed)
        fprintf(stderr, "%u/%u test cases had issues initializing or cleaning up\n",
                test_cases_failed, test_cases_ok + test_cases_failed);

    if (methods_failed)
        fprintf(stderr, "%u/%u tests failed\n", methods_failed, methods_ok + methods_failed);

    if (methods_skipped)
        fprintf(stderr, "%u tests skipped\n", methods_skipped);

    fprintf(stderr, "%u tests succeeded\n", methods_ok);
}

}
}
}
