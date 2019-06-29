#include "arki/tests/tests.h"
#include "arki/runtime/tests.h"
#include "arki/tests/daemon.h"
#include "arki-mergeconf.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
};

Tests test("arki_runtime_arkimergeconf");

void Tests::register_tests() {

add_method("http", [] {
    using runtime::tests::run_cmdline;

    Daemon server({"arki/dataset/http-redirect-daemon"});
    int port;
    sscanf(server.daemon_start_string.c_str(), "OK %d\n", &port);
    char url[512];
    snprintf(url, 512, "http://localhost:%d", port);

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiMergeconf>({ "arki-mergeconf", url });
        wassert(actual(res) == 0);
        wassert(actual_file(co.file_stderr.name()).empty());
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "[error]",
            "name = error",
            "path = http://foo.bar/foo/dataset/error",
            "type = remote",
            "",
            "[test200]",
            "name = test200",
            "path = http://foo.bar/foo/dataset/test200",
            "type = remote",
            "",
            "[test80]",
            "name = test80",
            "path = http://foo.bar/foo/dataset/test80",
            "type = remote",
        }));
    }
});

add_method("ignore_system_datasets", [] {
    using runtime::tests::run_cmdline;

    Daemon server({"arki/dataset/http-redirect-daemon"});
    int port;
    sscanf(server.daemon_start_string.c_str(), "OK %d\n", &port);
    char url[512];
    snprintf(url, 512, "http://localhost:%d", port);

    {
        runtime::tests::CatchOutput co;
        int res = run_cmdline<runtime::ArkiMergeconf>({ "arki-mergeconf", "--ignore-system-datasets", url });
        wassert(actual(res) == 0);
        wassert(actual_file(co.file_stderr.name()).contents_equal({}));
        wassert(actual_file(co.file_stdout.name()).contents_equal({
            "[test200]",
            "name = test200",
            "path = http://foo.bar/foo/dataset/test200",
            "type = remote",
            "",
            "[test80]",
            "name = test80",
            "path = http://foo.bar/foo/dataset/test80",
            "type = remote",
        }));
    }
});

}

}
