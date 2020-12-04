#include "tests.h"
#include "level.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::Level>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_level");

void Tests::register_tests() {

// Check GRIB1 without l1 and l2
add_generic_test("grib1_ltype",
        { "GRIB1(0, 0, 0)" },
        { "GRIB1(1)", "GRIB1(1, 0)", "GRIB1(1, 1, 1)" },
        { "GRIB1(2)", "GRIB1(2, 3, 4)", "GRIB2S(100, 100, 500)" },
        "GRIB1,1");

// Check GRIB1 with an 8 bit l1
add_generic_test("grib1_l1",
    { "GRIB1(1)", "GRIB1(100, 132)" },
    { "GRIB1(103, 132)", "GRIB1(103, 132, 0)", "GRIB1(103, 132, 1)"},
    { "GRIB1(103, 133)", "GRIB2S(100, 100, 500)" },
    "GRIB1,103,132");

// Check GRIB1 with a 16 bit l1
add_generic_test("grib1_l1large",
    { "GRIB1(1)", "GRIB1(100, 132)" },
    "GRIB1(103, 13200)",
    { "GRIB1(103, 13201)", "GRIB2S(100, 100, 500)" },
    "GRIB1,103,13200");

// Check GRIB1 with a layer
add_generic_test("grib1_l1_l2",
    { "GRIB1(1)", "GRIB1(100, 132)", "GRIB1(103, 13201)" },
    "GRIB1(104, 132, 231)",
    { "GRIB1(104, 133, 231)", "GRIB1(104, 132, 232)", "GRIB2S(100, 100, 500)" },
    "GRIB1,104,132,231");

add_method("grib1_details", [] {
    using namespace arki::types;
    unique_ptr<Level> o;
    unsigned ty, l1, l2;

    o = Level::createGRIB1(1);
    wassert(actual(o->style()) == Level::Style::GRIB1);
    o->get_GRIB1(ty, l1, l2);
    wassert(actual(ty) == 1u);
    wassert(actual(l1) == 0u);
    wassert(actual(l2) == 0u);
    wassert(actual(Level::GRIB1_type_vals(ty)) == 0u);

    o = Level::createGRIB1(103, 132);
    wassert(actual(o->style()) == Level::Style::GRIB1);
    o->get_GRIB1(ty, l1, l2);
    wassert(actual(ty) == 103u);
    wassert(actual(l1) == 132u);
    wassert(actual(l2) == 0u);
    wassert(actual(Level::GRIB1_type_vals(ty)) == 1u);

    o = Level::createGRIB1(103, 13200);
    wassert(actual(o->style()) == Level::Style::GRIB1);
    o->get_GRIB1(ty, l1, l2);
    wassert(actual(ty) == 103u);
    wassert(actual(l1) == 13200u);
    wassert(actual(l2) == 0u);
    wassert(actual(Level::GRIB1_type_vals(ty)) == 1u);

    o = Level::createGRIB1(104, 132, 231);
    wassert(actual(o->style()) == Level::Style::GRIB1);
    o->get_GRIB1(ty, l1, l2);
    wassert(actual(ty) == 104u);
    wassert(actual(l1) == 132u);
    wassert(actual(l2) == 231u);
    wassert(actual(Level::GRIB1_type_vals(ty)) == 2u);
});

// Check GRIB2S
add_generic_test("grib2s",
    { "GRIB1(1)", "GRIB2S( 99, 100, 500)", "GRIB2S(100,  99, 500)", "GRIB2S(100, 100, 499)", },
    "GRIB2S(100, 100, 500)",
    { "GRIB2S(101, 100, 500)", "GRIB2S(100, 101, 500)", "GRIB2S(100, 100, 501)", },
    "GRIB2S,100,100,500");

// Check GRIB2S with missing values
add_generic_test("grib2s_missing",
    { "GRIB1(1)",
    // FIXME: current implementation sorts missing higher than everything else;
    // it should perhaps be changed at some point
      "GRIB2S(1, -, -)", "GRIB2S(-, 1, -)", "GRIB2S(-, -, 1)", "GRIB2S(1, 1, 5)" },
    "GRIB2S(-, -, -)",
    {},
    "GRIB2S,-,-,-");

add_method("grib2s_details", [] {
    using namespace arki::types;
    unique_ptr<Level> o;

    o = Level::createGRIB2S(100, 100, 500);
    wassert(actual(o->style()) == Level::Style::GRIB2S);
    unsigned ty, sc, va;
    o->get_GRIB2S(ty, sc, va);
    wassert(actual(ty) == 100u);
    wassert(actual(sc) == 100u);
    wassert(actual(va) == 500u);

    o = Level::createGRIB2S(Level::GRIB2_MISSING_TYPE, Level::GRIB2_MISSING_SCALE, Level::GRIB2_MISSING_VALUE);
    wassert(actual(o->style()) == Level::Style::GRIB2S);
    o->get_GRIB2S(ty, sc, va);
    wassert(actual(ty) == Level::GRIB2_MISSING_TYPE);
    wassert(actual(sc) == Level::GRIB2_MISSING_SCALE);
    wassert(actual(va) == Level::GRIB2_MISSING_VALUE);
});

// Check GRIB2D
add_generic_test("grib2d",
    { "GRIB1(1)", "GRIB2S(1, -, -)" },
    "GRIB2D(100, 100, 500, 100, 100, 1000)",
    { "GRIB2D(101, 100, 500, 100, 100, 1000)",
      "GRIB2D(100, 101, 500, 100, 100, 1000)",
      "GRIB2D(100, 100, 501, 100, 100, 1000)",
      "GRIB2D(100, 100, 500, 101, 100, 1000)",
      "GRIB2D(100, 100, 500, 100, 101, 1000)",
      "GRIB2D(100, 100, 500, 100, 100, 1001)" },
    "GRIB2D,100,100,500,100,100,1000");

// Check GRIB2D with missing values
add_generic_test("grib2d_missing",
    {
      "GRIB1(1)",
      "GRIB1(100, 100)",
      "GRIB2S(1, -, -)",
      // FIXME: current implementation sorts missing higher than everything else;
      // it should perhaps be changed at some point
      "GRIB2D(1, -, -, -, -, -)",
      "GRIB2D(-, 1, -, -, -, -)",
      "GRIB2D(-, -, 1, -, -, -)",
      "GRIB2D(-, -, -, 1, -, -)",
      "GRIB2D(-, -, -, -, 1, -)",
      "GRIB2D(-, -, -, -, -, 1)",
      "GRIB2D(100, 100, 500, 100, 100, 1000)", },
    "GRIB2D(-, -, -, -, -, -)",
    {},
    "GRIB2D,-,-,-,-,-,-");

add_method("grib2d_details", [] {
    using namespace arki::types;
    unique_ptr<Level> o;
    unsigned ty1, sc1, va1, ty2, sc2, va2;

    o = Level::createGRIB2D(100, 100, 500, 100, 100, 1000);
    wassert(actual(o->style()) == Level::Style::GRIB2D);
    o->get_GRIB2D(ty1, sc1, va1, ty2, sc2, va2);
    wassert(actual(ty1) == 100u);
    wassert(actual(sc1) == 100u);
    wassert(actual(va1) == 500u);
    wassert(actual(ty2) == 100u);
    wassert(actual(sc2) == 100u);
    wassert(actual(va2) == 1000u);

    o = Level::createGRIB2D(
            Level::GRIB2_MISSING_TYPE, Level::GRIB2_MISSING_SCALE, Level::GRIB2_MISSING_VALUE,
            Level::GRIB2_MISSING_TYPE, Level::GRIB2_MISSING_SCALE, Level::GRIB2_MISSING_VALUE);
    wassert(actual(o->style()) == Level::Style::GRIB2D);
    o->get_GRIB2D(ty1, sc1, va1, ty2, sc2, va2);
    wassert(actual(ty1) == Level::GRIB2_MISSING_TYPE);
    wassert(actual(sc1) == Level::GRIB2_MISSING_SCALE);
    wassert(actual(va1) == Level::GRIB2_MISSING_VALUE);
    wassert(actual(ty2) == Level::GRIB2_MISSING_TYPE);
    wassert(actual(sc2) == Level::GRIB2_MISSING_SCALE);
    wassert(actual(va2) == Level::GRIB2_MISSING_VALUE);
});

// Check ODIMH5
add_generic_test("odimh5",
    {"GRIB1(1)", "GRIB1(100, 100)", "GRIB2S(1, -, -)", "GRIB2S(101, 100, 500, 100, 100, 1000)" },
    "ODIMH5(10.123, 20.123)",
    { "ODIMH5(10.124, 20.123)", "ODIMH5(10.123, 20.124)", },
    "ODIMH5,range 10.123 20.123");

add_method("odimh5_details", [] {
    using namespace arki::types;
    unique_ptr<Level> o = Level::createODIMH5(10.123, 20.123);
    wassert(actual(o->style()) == Level::Style::ODIMH5);
    double mi, ma;
    o->get_ODIMH5(mi, ma);
    wassert(actual(ma) == 20.123);
    wassert(actual(mi) == 10.123);
});

}

}
