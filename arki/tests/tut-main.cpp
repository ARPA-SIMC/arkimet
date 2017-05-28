#include <arki/utils/tests.h>
#include <arki/types-init.h>
#include <arki/iotrace.h>
#include <arki/utils/lock.h>
#include <arki/emitter/json.h>
#include <signal.h>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <arki/nag.h>

using namespace std;

void signal_to_exception(int)
{
    throw std::runtime_error("killing signal catched");
}

struct ArkiRunner
{
    arki::utils::tests::SimpleTestController controller;
    unsigned methods_ok = 0;
    unsigned methods_failed = 0;
    unsigned methods_skipped = 0;
    unsigned test_cases_ok = 0;
    unsigned test_cases_failed = 0;
    bool run_all = true;
    bool success = true;

    bool setup()
    {
        if (const char* whitelist = getenv("TEST_WHITELIST"))
        {
            run_all = false;
            controller.whitelist = whitelist;
        }

        if (const char* blacklist = getenv("TEST_BLACKLIST"))
        {
            run_all = false;
            controller.blacklist = blacklist;
        }
        return false;
    }

    void doc()
    {
        using namespace arki::utils::tests;
        auto& tests = TestRegistry::get();
        arki::emitter::JSON out(cout);
        out.start_list();
        tests.iterate_test_methods([&](const TestCase& tc, const TestMethod& tm) {
            out.start_mapping();
            out.add("group", tc.name);
            out.add("method", tm.name);
            out.add("doc", tm.doc);
            out.end_mapping();
        });
        out.end_list();
    }

    void run()
    {
        auto& tests = arki::utils::tests::TestRegistry::get();
        auto all_results = tests.run_tests(controller);

        for (const auto& tc_res: all_results)
        {
            if (!tc_res.fail_setup.empty())
            {
                fprintf(stderr, "%s: %s\n", tc_res.test_case.c_str(), tc_res.fail_setup.c_str());
                ++test_cases_failed;
            } else {
                if (!tc_res.fail_teardown.empty())
                {
                    fprintf(stderr, "%s: %s\n", tc_res.test_case.c_str(), tc_res.fail_teardown.c_str());
                    ++test_cases_failed;
                }
                else
                    ++test_cases_ok;

                for (const auto& tm_res: tc_res.methods)
                {
                    if (tm_res.skipped)
                        ++methods_skipped;
                    else if (tm_res.is_success())
                        ++methods_ok;
                    else
                    {
                        fprintf(stderr, "\n");
                        if (tm_res.exception_typeid.empty())
                            fprintf(stderr, "%s.%s: %s\n", tm_res.test_case.c_str(), tm_res.test_method.c_str(), tm_res.error_message.c_str());
                        else
                            fprintf(stderr, "%s.%s:[%s] %s\n", tm_res.test_case.c_str(), tm_res.test_method.c_str(), tm_res.exception_typeid.c_str(), tm_res.error_message.c_str());
                        for (const auto& frame : tm_res.error_stack)
                            fprintf(stderr, "  %s", frame.format().c_str());
                        ++methods_failed;
                    }
                }
            }
        }

        if (test_cases_failed)
        {
            success = false;
            fprintf(stderr, "\n%u/%u test cases had issues initializing or cleaning up\n",
                    test_cases_failed, test_cases_ok + test_cases_failed);
        }

        if (methods_failed)
        {
            success = false;
            fprintf(stderr, "\n%u/%u tests failed\n", methods_failed, methods_ok + methods_failed);
        }
        else
            fprintf(stderr, "%u tests succeeded\n", methods_ok);
    }
};

int main(int argc, const char* argv[])
{
    arki::nag::init(false, false, true);
    arki::types::init_default_types();
    arki::iotrace::init();
    arki::utils::Lock::test_set_nowait_default(true);

    signal(SIGSEGV,signal_to_exception);
    signal(SIGILL,signal_to_exception);

    // Run arki::tests based tests
    ArkiRunner arki_runner;
    if (arki_runner.setup())
        return 0;

    if (argc > 1 && strcmp(argv[1], "--doc") == 0)
    {
        arki_runner.doc();
        return 0;
    }

    arki_runner.run();

    if (!arki_runner.success) return 1;
    return 0;
}
