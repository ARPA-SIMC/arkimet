#include "arki/dataset/tests.h"
#include "arki/dataset/session.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset_session");

void write_test_config()
{
    sys::rmtree_ifexists("testds");
    sys::makedirs("testds");
    sys::write_file("testds/config", R"(
type = iseg
step = daily
filter = origin: GRIB1,200
index = origin, reftime
)");
}

void Tests::register_tests() {

add_method("read_config", [] {
    auto cfg = dataset::Session::read_config("inbound/test.grib1");
    wassert(actual(cfg->value("name")) == "inbound/test.grib1");

    cfg = dataset::Session::read_config("grib:inbound/test.grib1");
    wassert(actual(cfg->value("name")) == "inbound/test.grib1");

    write_test_config();
    cfg = dataset::Session::read_config("testds");
    wassert(actual(cfg->value("name")) == "testds");
});

add_method("read_configs", [] {
    auto cfg = dataset::Session::read_configs("inbound/test.grib1");
    auto sec = cfg->section("inbound/test.grib1");
    wassert_true(sec);
    wassert(actual(sec->value("name")) == "inbound/test.grib1");

    cfg = dataset::Session::read_configs("grib:inbound/test.grib1");
    sec = cfg->section("inbound/test.grib1");
    wassert_true(sec);
    wassert(actual(sec->value("name")) == "inbound/test.grib1");

    write_test_config();
    cfg = dataset::Session::read_configs("testds");
    sec = cfg->section("testds");
    wassert_true(sec);
    wassert(actual(sec->value("name")) == "testds");
});

add_method("add_local", [] {
    auto session = std::make_shared<dataset::Session>();
    auto cfg = dataset::Session::read_configs("inbound/test.grib1");
    auto sec = cfg->section("inbound/test.grib1");
    session->add_dataset(*sec);
    wassert_true(session->has_dataset("inbound/test.grib1"));

    auto ds = session->dataset("inbound/test.grib1");
    wassert_true(ds);
});

add_method("get_common_remote_server", [] {
    {
        std::string conf =
            "[test200]\n"
            "name = test200\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/test200\n"
            "server = http://foo.bar/foo/\n"
            "\n"
            "[test80]\n"
            "name = test80\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/test80\n"
            "server = http://foo.bar/foo/\n"
            "\n"
            "[error]\n"
            "name = error\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/error\n"
            "server = http://foo.bar/foo/\n";
        auto cfg = core::cfg::Sections::parse(conf);
        auto session = std::make_shared<dataset::Session>();
        for (const auto& si: *cfg)
            session->add_dataset(*si.second, false);

        wassert(actual(session->get_common_remote_server()) == "http://foo.bar/foo/");
    }

    {
        string conf =
            "[test200]\n"
            "name = test200\n"
            "type = remote\n"
            "path = http://bar.foo.bar/foo/dataset/test200\n"
            "server = http://bar.foo.bar/foo/\n"
            "\n"
            "[test80]\n"
            "name = test80\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/test80\n"
            "server = http://foo.bar/foo/\n"
            "\n"
            "[error]\n"
            "name = error\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/error\n"
            "server = http://foo.bar/foo/\n";
        auto cfg = core::cfg::Sections::parse(conf);
        auto session = std::make_shared<dataset::Session>();
        for (const auto& si: *cfg)
            session->add_dataset(*si.second, false);
        wassert(actual(session->get_common_remote_server()) == "");
    }

    {
        string conf =
            "[test200]\n"
            "name = test200\n"
            "type = ondisk2\n"
            "step = daily\n"
            "path = http://foo.bar/foo/dataset/test200\n"
            "server = http://foo.bar/foo/\n"
            "\n"
            "[test80]\n"
            "name = test80\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/test80\n"
            "server = http://foo.bar/foo/\n"
            "\n"
            "[error]\n"
            "name = error\n"
            "type = remote\n"
            "path = http://foo.bar/foo/dataset/error\n"
            "server = http://foo.bar/foo/\n";
        auto cfg = core::cfg::Sections::parse(conf);
        auto session = std::make_shared<dataset::Session>();
        for (const auto& si: *cfg)
            session->add_dataset(*si.second, false);
        wassert(actual(session->get_common_remote_server()) == "");
    }
});

}

}
