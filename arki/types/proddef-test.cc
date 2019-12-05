#include "tests.h"
#include "proddef.h"

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

class Tests : public TypeTestCase<types::Proddef>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_proddef");

void Tests::register_tests() {

add_generic_test("grib",
    {},
    "GRIB(uno=1,due=2,tre=-3,supercazzola=-1234567,pippo=pippo,pluto=\"12\",cippo=)",
    { "GRIB(dieci=10,undici=11,dodici=-12)" },
    "GRIB:cippo=, due=2, pippo=pippo, pluto=\"12\", supercazzola=-1234567, tre=-3, uno=1");

add_generic_test("grib_1",
    {},
    "GRIB(count=1,pippo=pippo)",
    { "GRIB(count=2,pippo=pippo)" });

}

}
