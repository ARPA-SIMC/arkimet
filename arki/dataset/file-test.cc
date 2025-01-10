#include "arki/core/tests.h"
#include "arki/core/cfg.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/dataset/file.h"
#include "arki/dataset/session.h"
#include "arki/query.h"
#include "arki/metadata/collection.h"
#include <memory>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using arki::core::Time;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_file");

void Tests::register_tests() {

add_method("grib", [] {
    auto cfg = dataset::file::read_config("inbound/test.grib1");
    wassert(actual(cfg->value("name")) == "inbound/test.grib1");
    wassert(actual(cfg->value("type")) == "file");
    wassert(actual(cfg->value("format")) == "grib");
});

add_method("grib_as_bufr", [] {
    auto cfg = dataset::file::read_config("bUFr", "inbound/test.grib1");
    wassert(actual(cfg->value("name")) == "inbound/test.grib1");
    wassert(actual(cfg->value("type")) == "file");
    wassert(actual(cfg->value("format")) == "bufr");
});

add_method("grib_strangename", [] {
    system("cp inbound/test.grib1 strangename");
    auto cfg = dataset::file::read_config("GRIB", "strangename");
    wassert(actual(cfg->value("name")) == "strangename");
    wassert(actual(cfg->value("type")) == "file");
    wassert(actual(cfg->value("format")) == "grib");

    // Scan it to be sure it can be read
    auto session = std::make_shared<dataset::Session>();
    metadata::Collection mdc(*session->dataset(*cfg), Matcher());
    wassert(actual(mdc.size()) == 3u);
});

add_method("metadata", [] {
    auto cfg = dataset::file::read_config("inbound/odim1.arkimet");
    wassert(actual(cfg->value("name")) == "inbound/odim1.arkimet");
    wassert(actual(cfg->value("type")) == "file");
    wassert(actual(cfg->value("format")) == "arkimet");

    // Scan it to be sure it can be read
    auto session = std::make_shared<dataset::Session>();
    metadata::Collection mdc(*session->dataset(*cfg), Matcher());
    wassert(actual(mdc.size()) == 1u);
});

add_method("yaml", [] {
    auto cfg = dataset::file::read_config("inbound/issue107.yaml");
    wassert(actual(cfg->value("name")) == "inbound/issue107.yaml");
    wassert(actual(cfg->value("type")) == "file");
    wassert(actual(cfg->value("format")) == "yaml");

    // Scan it to be sure it can be read
    auto session = std::make_shared<dataset::Session>();
    metadata::Collection mdc(*session->dataset(*cfg), Matcher());
    wassert(actual(mdc.size()) == 1u);
});

}

}
