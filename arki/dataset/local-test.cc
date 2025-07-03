#include "tests.h"
#include "local.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset_local");

void Tests::register_tests() {

add_method("read_config", [] {
    sys::rmtree_ifexists("testds");
    std::filesystem::create_directories("testds/.archive/last");
    sys::write_file("testds/config", R"(
description = test dataset
filter = origin:GRIB1,80,,22
index = reftime, timerange, level, product
replace = yes
step = daily
type = iseg
format = grib
unique = origin, reftime, timerange, level, product
archive age = 20
delete age = 50
postprocess = singlepoint, subarea, seriet, cat
    )");

    auto config = dataset::local::Reader::read_config("testds/.archive/last");
    wassert(actual(config->value("description")) == "test dataset");
    wassert(actual(config->value("filter")) == "origin:GRIB1,80,,22");
    wassert(actual(config->value("index")) == "reftime, timerange, level, product");
    wassert(actual(config->value("replace")) == "yes");
    wassert(actual(config->value("step")) == "daily");
    wassert(actual(config->value("type")) == "simple");
    wassert(actual(config->value("format")) == "grib");
    wassert(actual(config->value("unique")) == "origin, reftime, timerange, level, product");
    wassert(actual(config->value("postprocess")) == "singlepoint, subarea, seriet, cat");
    wassert(actual(config->value("name")) == "last");
    wassert(actual(config->value("path")) == std::filesystem::canonical("testds/.archive/last"));
    wassert_false(config->has("archive age"));
    wassert_false(config->has("delete age"));
});

}

}
