#include "testrunner.h"
#include "tests.h"
#include <fnmatch.h>
#include <map>
#include <algorithm>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace utils {
namespace tests {

/*
 * TestMethodResult
 */

void TestMethodResult::set_failed(TestFailed& e)
{
    error_message = e.what();
    error_stack = make_shared<TestStack>(e.stack);
    if (error_message.empty())
        error_message = "test failed with an empty error message";
}

void TestMethodResult::print_failure_details(FILE* out) const
{
    if (exception_typeid.empty())
        fprintf(out, "%s.%s: %s\n", test_case.c_str(), test_method.c_str(), error_message.c_str());
    else
        fprintf(out, "%s.%s:[%s] %s\n", test_case.c_str(), test_method.c_str(), exception_typeid.c_str(), error_message.c_str());
    for (const auto& frame : *error_stack)
        fprintf(out, "  %s", frame.format().c_str());
}


/*
 * TestCaseResult
 */

unsigned long long TestCaseResult::elapsed_ns() const
{
    unsigned long long res = 0;
    for (const auto tmr: methods)
        res += tmr.elapsed_ns;
    return res;
}


/*
 * FilteringTestController
 */

bool FilteringTestController::test_method_should_run(const std::string& fullname) const
{
    if (!whitelist.empty() && fnmatch(whitelist.c_str(), fullname.c_str(), 0) == FNM_NOMATCH)
        return false;

    if (!blacklist.empty() && fnmatch(blacklist.c_str(), fullname.c_str(), 0) != FNM_NOMATCH)
        return false;

    return true;
}


/*
 * SimpleTestController
 */

bool SimpleTestController::test_case_begin(const TestCase& test_case, const TestCaseResult& test_case_result)
{
    // Skip test case if all its methods should not run
    bool should_run = false;
    for (const auto& m : test_case.methods)
        should_run |= test_method_should_run(test_case.name + "." + m.name);
    if (!should_run) return false;

    fprintf(output, "%s: ", test_case.name.c_str());
    fflush(output);
    return true;
}

void SimpleTestController::test_case_end(const TestCase& test_case, const TestCaseResult& test_case_result)
{
    if (test_case_result.skipped)
        ;
    else if (test_case_result.is_success())
        fprintf(output, "\n");
    else
        fprintf(output, "\n");
    fflush(output);
}

bool SimpleTestController::test_method_begin(const TestMethod& test_method, const TestMethodResult& test_method_result)
{
    string name = test_method_result.test_case + "." + test_method.name;
    return test_method_should_run(name);
}

void SimpleTestController::test_method_end(const TestMethod& test_method, const TestMethodResult& test_method_result)
{
    if (test_method_result.skipped)
        putc('s', output);
    else if (test_method_result.is_success())
        putc('.', output);
    else
        putc('x', output);
    fflush(output);
}


/*
 * VerboseTestController
 */

static void format_elapsed(char* buf, size_t size, unsigned long long elapsed_ns)
{
    if (elapsed_ns < 1000)
        snprintf(buf, size, "%lluns", elapsed_ns);
    else if (elapsed_ns < 1000000)
        snprintf(buf, size, "%lluµs", elapsed_ns / 1000);
    else if (elapsed_ns < 1000000000)
        snprintf(buf, size, "%llums", elapsed_ns / 1000000);
    else
        snprintf(buf, size, "%.2fs", (double)(elapsed_ns / 1000000) / 1000.0);
}

bool VerboseTestController::test_case_begin(const TestCase& test_case, const TestCaseResult& test_case_result)
{
    // Skip test case if all its methods should not run
    bool should_run = false;
    for (const auto& m : test_case.methods)
        should_run |= test_method_should_run(test_case.name + "." + m.name);
    if (!should_run) return false;

    fprintf(output, "%s: setup\n", test_case.name.c_str());
    return true;
}

void VerboseTestController::test_case_end(const TestCase& test_case, const TestCaseResult& test_case_result)
{
    if (test_case_result.skipped)
        return;

    char elapsed[32];
    format_elapsed(elapsed, 32, test_case_result.elapsed_ns());
    if (test_case_result.is_success())
        fprintf(output, "%s: ✔ (%s)\n", test_case.name.c_str(), elapsed);
    else
        fprintf(output, "%s: ✘ (%s)\n", test_case.name.c_str(), elapsed);
}

bool VerboseTestController::test_method_begin(const TestMethod& test_method, const TestMethodResult& test_method_result)
{
    string name = test_method_result.test_case + "." + test_method.name;
    return test_method_should_run(name);
}

void VerboseTestController::test_method_end(const TestMethod& test_method, const TestMethodResult& test_method_result)
{
    char elapsed[32];
    format_elapsed(elapsed, 32, test_method_result.elapsed_ns);

    if (test_method_result.skipped)
    {
        if (test_method_result.skipped_reason.empty())
            fprintf(output, "%s.%s: skipped.\n", test_method_result.test_case.c_str(), test_method.name.c_str());
        else
            fprintf(output, "%s.%s: skipped: %s\n", test_method_result.test_case.c_str(), test_method.name.c_str(), test_method_result.skipped_reason.c_str());
    }
    else if (test_method_result.is_success())
        fprintf(output, "%s.%s: ✔ (%s)\n", test_method_result.test_case.c_str(), test_method.name.c_str(), elapsed);
    else
    {
        fprintf(output, "%s.%s: ✘ (%s)\n", test_method_result.test_case.c_str(), test_method.name.c_str(), elapsed);
        test_method_result.print_failure_details(output);
    }
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
            fprintf(out, "%s: %s\n", tc_res.test_case.c_str(), tc_res.fail_setup.c_str());
        else {
            if (!tc_res.fail_teardown.empty())
                fprintf(out, "%s: %s\n", tc_res.test_case.c_str(), tc_res.fail_teardown.c_str());

            for (const auto& tm_res: tc_res.methods)
            {
                if (tm_res.skipped || tm_res.is_success()) continue;
                fprintf(out, "\n");
                tm_res.print_failure_details(out);
            }
        }
    }
}

void TestResultStats::print_stats(FILE* out)
{
    //const long long unsigned slow_threshold = 10;
    const long long unsigned slow_threshold = 100000000;
    std::map<string, unsigned> skipped_by_reason;
    unsigned skipped_no_reason = 0;
    std::vector<const TestCaseResult*> slow_test_cases;
    std::vector<const TestMethodResult*> slow_test_methods;

    for (const auto& tc_res: results)
    {
        if (tc_res.elapsed_ns() > slow_threshold)
            slow_test_cases.push_back(&tc_res);

        for (const auto& tm_res: tc_res.methods)
        {
            if (tm_res.skipped)
            {
                if (tm_res.skipped_reason.empty())
                    ++skipped_no_reason;
                else
                    skipped_by_reason[tm_res.skipped_reason] += 1;
            }
            if (tm_res.elapsed_ns > slow_threshold)
                slow_test_methods.push_back(&tm_res);
        }
    }

    bool title_printed = false;
    auto maybe_print_title = [&]() {
        if (title_printed) return;
        fprintf(out, "\n * Test run statistics\n");
        title_printed = true;
    };

    if (!skipped_by_reason.empty())
    {
        maybe_print_title();
        fprintf(out, "\n    * Number of tests skipped, by reason:\n\n");
        std::vector<std::pair<unsigned, std::string>> sorted;
        for (const auto& i: skipped_by_reason)
            sorted.emplace_back(make_pair(i.second, i.first));
        std::sort(sorted.begin(), sorted.end());
        for (const auto& i: sorted)
            fprintf(out, "        %2d: %s\n", i.first, i.second.c_str());
        if (skipped_no_reason)
            fprintf(out, "        %2d: (no reason given)\n", skipped_no_reason);
    }

    if (!slow_test_cases.empty())
    {
        maybe_print_title();
        std::sort(slow_test_cases.begin(), slow_test_cases.end(), [](const TestCaseResult* a, const TestCaseResult* b) { return b->elapsed_ns() < a->elapsed_ns(); });
        size_t count = min((size_t)10, slow_test_cases.size());
        fprintf(out, "\n    * %zu slowest test cases:\n\n", count);
        for (size_t i = 0; i < count; ++i)
        {
            char elapsed[32];
            format_elapsed(elapsed, 32, slow_test_cases[i]->elapsed_ns());
            fprintf(out, "        %s: %s\n", slow_test_cases[i]->test_case.c_str(), elapsed);
        }
    }

    if (!slow_test_methods.empty())
    {
        maybe_print_title();
        std::sort(slow_test_methods.begin(), slow_test_methods.end(), [](const TestMethodResult* a, const TestMethodResult* b) { return b->elapsed_ns < a->elapsed_ns; });
        size_t count = min((size_t)10, slow_test_methods.size());
        fprintf(out, "\n    * %zu slowest test methods:\n\n", count);
        for (size_t i = 0; i < count; ++i)
        {
            char elapsed[32];
            format_elapsed(elapsed, 32, slow_test_methods[i]->elapsed_ns);
            fprintf(out, "        %s.%s: %s\n", slow_test_methods[i]->test_case.c_str(), slow_test_methods[i]->test_method.c_str(), elapsed);
        }
    }

    if (title_printed)
        fprintf(out, "\n");
}

void TestResultStats::print_summary(FILE* out)
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
