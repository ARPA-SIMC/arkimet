#include <arki/wibble/tests/tut.h>
#include <arki/wibble/tests/tut_reporter.h>
#include <arki/utils/tests.h>
#include <arki/types-init.h>
#include <signal.h>
#include <cstring>
#include <cstdlib>
#include <arki/nag.h>

namespace tut {
  test_runner_singleton runner;
}

using namespace std;

void signal_to_exception(int)
{
    throw std::runtime_error("killing signal catched");
}

struct TutRunner
{
    tut::reporter visi;
    bool run_all = false;
    string wanted_group;
    int wanted_test = -1;
    bool success = true;

    // Setup the test runner. Returns true if program invocation has been
    // fully handled and execution should stop after this
    bool setup(int argc, const char* argv[])
    {
        if ( (argc == 2 && (! strcmp ("help", argv[1]))) || argc > 3 )
        {
            std::cout << argv[0] << " test runner." << std::endl;
            std::cout << "Usage: " << argv[0] << " [group] [test] (runs all tests)" << std::endl;
            std::cout << "Usage: " << argv[0] << " list (lists all test groups)" << std::endl;
            return true;
        }

        if (argc == 2 && std::string(argv[1]) == "list")
        {
            std::cout << "registered test groups:" << std::endl;
            tut::groupnames gl = tut::runner.get().list_groups();
            tut::groupnames::const_iterator i = gl.begin();
            tut::groupnames::const_iterator e = gl.end();
            while( i != e )
            {
                std::cout << "  " << *i << std::endl;
                ++i;
            }
            return true;
        }

        if (argc > 1)
            wanted_group = argv[1];
        else
            run_all = true;
        if (argc > 2)
            wanted_test = ::atoi(argv[2]);

        tut::runner.get().set_callback(&visi);
        return false;
    }

    void run()
    {
        try
        {
            if (wanted_group.empty())
                tut::runner.get().run_tests();
            else if (wanted_test == -1)
                tut::runner.get().run_tests(wanted_group);
            else
                tut::runner.get().run_test(wanted_group, wanted_test);
        }
        catch( const std::exception& ex )
        {
            std::cerr << "tut raised exception: " << ex.what() << std::endl;
        }

        success = !visi.failures_count && !visi.exceptions_count;
    }
};

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

int main(int argc,const char* argv[])
{
    arki::nag::init(false, false, true);
    arki::types::init_default_types();

    signal(SIGSEGV,signal_to_exception);
    signal(SIGILL,signal_to_exception);

    // Run tut-based tests
    TutRunner tut_runner;
    if (tut_runner.setup(argc, argv))
        return 0;

    // Run arki::tests based tests
    ArkiRunner arki_runner;
    if (arki_runner.setup())
        return 0;

    if (arki_runner.run_all)
        tut_runner.run();
    if (tut_runner.run_all)
        arki_runner.run();

    if (!arki_runner.success || !tut_runner.success) return 1;
    return 0;
}
