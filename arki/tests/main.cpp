#include "arki/utils/tests.h"
#include "arki/utils/testrunner.h"
#include "arki/utils/term.h"
#include "arki/core/file.h"
#include "arki/emitter/json.h"
#include "arki/runtime.h"
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
    arki::utils::term::Terminal output;
    arki::utils::tests::FilteringTestController* controller = nullptr;
    bool run_all = true;
    bool verbose = false;
    bool stats = false;

    ArkiRunner()
        : output(stdout)
    {
        if (getenv("TEST_VERBOSE"))
        {
            verbose = true;
            stats = true;
        }

        if (getenv("TEST_STATS"))
            stats = true;

        if (verbose)
            controller = new arki::utils::tests::VerboseTestController(output);
        else
            controller = new arki::utils::tests::SimpleTestController(output);
    }
    ~ArkiRunner()
    {
        delete controller;
    }

    bool setup()
    {
        if (const char* whitelist = getenv("TEST_WHITELIST"))
        {
            run_all = false;
            controller->whitelist = whitelist;
        }

        if (const char* blacklist = getenv("TEST_BLACKLIST"))
        {
            run_all = false;
            controller->blacklist = blacklist;
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

    bool run()
    {
        using namespace arki::utils::tests;
        auto& tests = arki::utils::tests::TestRegistry::get();
        auto all_results = tests.run_tests(*controller);
        TestResultStats rstats(all_results);
        rstats.print_results(output);
        if (stats)
            rstats.print_stats(output);
        rstats.print_summary(output);
        return rstats.success;
    }
};

int main(int argc, const char* argv[])
{
    arki::nag::init(false, false, true);
    arki::init();
    arki::core::lock::test_set_nowait_default(true);

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

    bool success = arki_runner.run();

    return success ? 0 : 1;
}
