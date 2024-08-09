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
    std::filesystem::create_directories("testds");
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
    wassert(actual(cfg->value("path")) == std::filesystem::canonical("inbound/test.grib1"));

    cfg = dataset::Session::read_config("grib:inbound/test.grib1");
    wassert(actual(cfg->value("name")) == "inbound/test.grib1");
    wassert(actual(cfg->value("path")) == std::filesystem::canonical("inbound/test.grib1"));

    write_test_config();
    std::string expected_path = std::filesystem::canonical("testds");

    cfg = dataset::Session::read_config("testds");
    wassert(actual(cfg->value("name")) == "testds");
    wassert(actual(cfg->value("path")) == expected_path);

    cfg = dataset::Session::read_config("testds/config");
    wassert(actual(cfg->value("name")) == "testds");
    wassert(actual(cfg->value("path")) == expected_path);

    sys::write_file("testds/file.unknown", "test");
    wassert_throws(std::runtime_error, dataset::Session::read_config("testds/file.unknown"));
});

add_method("read_configs", [] {
    auto cfg = dataset::Session::read_configs("inbound/test.grib1");
    auto sec = cfg->section("inbound/test.grib1");
    wassert_true(sec);
    wassert(actual(sec->value("name")) == "inbound/test.grib1");
    wassert(actual(sec->value("path")) == std::filesystem::canonical("inbound/test.grib1"));

    cfg = dataset::Session::read_configs("grib:inbound/test.grib1");
    sec = cfg->section("inbound/test.grib1");
    wassert_true(sec);
    wassert(actual(sec->value("name")) == "inbound/test.grib1");
    wassert(actual(sec->value("path")) == std::filesystem::canonical("inbound/test.grib1"));

    write_test_config();
    cfg = dataset::Session::read_configs("testds");
    sec = cfg->section("testds");
    wassert_true(sec);
    wassert(actual(sec->value("name")) == "testds");
    wassert(actual(sec->value("path")) == std::filesystem::canonical("testds"));
});

add_method("read_config_remotedir", [] {
    // Reproduce #274
    sys::rmtree_ifexists("testds");
    std::filesystem::create_directories("testds");
    sys::write_file("testds/config", R"(
type = remote
path = http://example.org
step = daily
)");

    auto cfg = dataset::Session::read_config("testds");
    wassert(actual(cfg->value("name")) == "testds");
    wassert(actual(cfg->value("path")) == "http://example.org");

    auto configs = dataset::Session::read_configs("testds");
    cfg = configs->section("testds");
    wassert(actual(cfg->value("name")) == "testds");
    wassert(actual(cfg->value("path")) == "http://example.org");
});

}

}
