#include "arki/tests/tests.h"
#include "arki/tests/daemon.h"
#include "arki/dataset/session.h"
#include "arki/dataset/query.h"
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
        auto cfg = core::cfg::Sections::parse(conf);
        wassert(actual(http::Dataset::all_same_remote_server(cfg)) == "http://foo.bar/foo");
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
        auto cfg = core::cfg::Sections::parse(conf);
        wassert(actual(http::Dataset::all_same_remote_server(cfg)) == "");
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
        auto cfg = core::cfg::Sections::parse(conf);
        wassert(actual(http::Dataset::all_same_remote_server(cfg)) == "");
    }
});

add_method("redirect", [] {
    tests::Daemon server({"arki/dataset/http-test-daemon", "--action=redirect"});
    int port;
    sscanf(server.daemon_start_string.c_str(), "OK %d\n", &port);
    char url[512];
    snprintf(url, 512, "http://localhost:%d", port);

    auto cfg = http::Reader::load_cfg_sections(url);
    auto sec = cfg.section("test200");
    wassert(actual(sec->value("type")) == "remote");
    wassert(actual(sec->value("path")) == "http://foo.bar/foo/dataset/test200");
});

add_method("server_error", [] {
    tests::Daemon server({"arki/dataset/http-test-daemon", "--action=query500"});
    int port;
    sscanf(server.daemon_start_string.c_str(), "OK %d\n", &port);
    char url[512];
    snprintf(url, 512, "http://localhost:%d", port);

    auto cfg = http::Reader::load_cfg_sections(url);
    auto session = std::make_shared<dataset::Session>();
    auto sec = cfg.section("test200");
    wassert(actual(sec->value("type")) == "remote");
    auto ds = std::make_shared<http::Dataset>(session, *sec);
    auto reader = ds->create_reader();
    unsigned count = 0;
    wassert_throws(std::runtime_error, reader->query_data(dataset::DataQuery(), [&](std::shared_ptr<Metadata>) { ++count; return true; }));
    wassert(actual(count) == 0u);
});

}

}
