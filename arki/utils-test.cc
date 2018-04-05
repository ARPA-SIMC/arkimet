#include "config.h"
#include <arki/tests/tests.h>
#include <arki/utils.h>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils");

void Tests::register_tests() {

add_method("get_format", [] {
    wassert(actual(get_format("z.grib")) == "grib");
    wassert(actual(get_format("z.grib.tar")) == "tar");
});

}

}
