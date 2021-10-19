#include "arki/core/cfg.h"
#include "arki/tests/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/session.h"
#include "pool.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

static const char* sample_config = R"(
[test200]
type = iseg
format = grib
step = daily
filter = origin: GRIB1,200
index = origin, reftime
name = test200
path = test200

[test80]
type = iseg
format = grib
step = daily
filter = origin: GRIB1,80
index = origin, reftime
name = test80
path = test80

[error]
type = error
step = daily
name = error
path = error
)";

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_pool");

void Tests::register_tests() {

add_method("add_local", [] {
    auto session = std::make_shared<dataset::Session>();
    auto pool = std::make_shared<dataset::Pool>(session);
    auto cfg = dataset::Session::read_configs("inbound/test.grib1");
    auto sec = cfg->section("inbound/test.grib1");
    pool->add_dataset(*sec);
    wassert_true(pool->has_dataset("inbound/test.grib1"));

    auto ds = pool->dataset("inbound/test.grib1");
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
        auto pool = std::make_shared<dataset::Pool>(session);
        for (const auto& si: *cfg)
            pool->add_dataset(*si.second, false);

        wassert(actual(pool->get_common_remote_server()) == "http://foo.bar/foo/");
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
        auto pool = std::make_shared<dataset::Pool>(session);
        for (const auto& si: *cfg)
            pool->add_dataset(*si.second, false);
        wassert(actual(pool->get_common_remote_server()) == "");
    }

    {
        string conf =
            "[test200]\n"
            "name = test200\n"
            "type = iseg\n"
            "format = grib\n"
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
        auto pool = std::make_shared<dataset::Pool>(session);
        for (const auto& si: *cfg)
            pool->add_dataset(*si.second, false);
        wassert(actual(pool->get_common_remote_server()) == "");
    }
});

add_method("instantiate", [] {
    using namespace arki::dataset;
    // In-memory dataset configuration
    auto session = std::make_shared<dataset::Session>();
    auto pool = std::make_shared<dataset::Pool>(session);
    auto config = core::cfg::Sections::parse(sample_config, "(memory)");
    for (const auto& i: *config)
        pool->add_dataset(*i.second);

    DispatchPool dpool(pool);
    wassert(actual(dpool.get("error")).istrue());
    wassert(actual(dpool.get("test200")).istrue());
    wassert(actual(dpool.get("test80")).istrue());
    try {
        dpool.get("duplicates");
        throw TestFailed("looking up nonexisting dataset should throw runtime_error");
    } catch (runtime_error&) {
    }
});

}

}
