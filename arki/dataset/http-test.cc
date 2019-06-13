#include "arki/tests/tests.h"
#include "arki/tests/daemon.h"
#include "arki/configfile.h"
#include "http.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::dataset;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_http");

void Tests::register_tests() {

// Test allSameRemoteServer
add_method("allsameremoteserver", [] {
    {
        string conf =
            "[test200]\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/test200\n"
            "\n"
            "[test80]\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/test80\n"
            "\n"
            "[error]\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/error\n";
        ConfigFile cfg;
        cfg.parse(conf);

        wassert(actual(http::Reader::allSameRemoteServer(cfg)) == "http://foo.bar/foo");
    }

    {
        string conf =
            "[test200]\n"
            "type = remote\n"
            "path = http://bar.foo.bar/foo/dataset/test200\n"
            "\n"
            "[test80]\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/test80\n"
            "\n"
            "[error]\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/error\n";
        ConfigFile cfg;
        cfg.parse(conf);

        wassert(actual(http::Reader::allSameRemoteServer(cfg)) == "");
    }

    {
        string conf =
            "[test200]\n"
            "type = ondisk2\n"
            "path = http://foo.bar/foo/dataset/test200\n"
            "\n"
            "[test80]\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/test80\n"
            "\n"
            "[error]\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/error\n";
        ConfigFile cfg;
        cfg.parse(conf);

        wassert(actual(http::Reader::allSameRemoteServer(cfg)) == "");
    }
});

add_method("redirect", [] {
    tests::Daemon server({"arki/dataset/http-redirect-daemon"});
    int port;
    sscanf(server.daemon_start_string.c_str(), "OK %d\n", &port);
    char url[512];
    snprintf(url, 512, "http://localhost:%d", port);

    ConfigFile cfg;
    http::Reader::read_config(url, cfg);
});

}

}
