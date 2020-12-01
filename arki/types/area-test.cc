#include "tests.h"
#include "area.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::Area>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_area");

void Tests::register_tests() {

// Check GRIB
add_generic_test(
        "grib",
        {},
        "GRIB(uno=1,due=2,tre=-3,supercazzola=-1234567,pippo=pippo,pluto=\"12\",cippo=)",
        { "GRIB(dieci=10,undici=11,dodici=-12)", },
        "GRIB:cippo=, due=2, pippo=pippo, pluto=\"12\", supercazzola=-1234567, tre=-3, uno=1");

// Check two more samples
add_generic_test(
        "grib_1",
        {},
        "GRIB(count=1,pippo=pippo)",
        { "GRIB(count=2,pippo=pippo)", });

// Check ODIMH5
add_generic_test(
        "odimh5",
        {},
        "ODIMH5(uno=1,due=2,tre=-3,supercazzola=-1234567,pippo=pippo,pluto=\"12\",cippo=)",
        { "ODIMH5(dieci=10,undici=11,dodici=-12)", },
        "ODIMH5:cippo=, due=2, pippo=pippo, pluto=\"12\", supercazzola=-1234567, tre=-3, uno=1");

// Check VM2
add_generic_test(
        "vm2",
        {},
        "VM2(1)",
        { "VM2(2)", },
        "VM2,1:lat=4460016, lon=1207738, rep=locali");

// Test VM2 derived values
add_method("vm2_derived", [] {
    skip_unless_vm2();
    auto vb1 = types::ValueBag::parse("lon=1207738,lat=4460016,rep=locali");
    wassert(actual(types::area::VM2::get_VM2_derived_values(1) == vb1).istrue());
    auto vb2 = types::ValueBag::parse("lon=1207738,lat=4460016,rep=NONONO");
    wassert(actual(types::area::VM2::get_VM2_derived_values(1) != vb2).istrue());
});

}

}
