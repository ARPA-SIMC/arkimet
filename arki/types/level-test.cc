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
    const level::GRIB1* v;

    o = Level::createGRIB1(1);
    wassert(actual(o->style()) == Level::GRIB1);
    v = dynamic_cast<level::GRIB1*>(o.get());
    wassert(actual(v->type()) == 1u);
    wassert(actual(v->l1()) == 0u);
    wassert(actual(v->l2()) == 0u);
    wassert(actual(v->valType()) == 0);

    o = Level::createGRIB1(103, 132);
    wassert(actual(o->style()) == Level::GRIB1);
    v = dynamic_cast<level::GRIB1*>(o.get());
    wassert(actual(v->type()) == 103);
    wassert(actual(v->l1()) == 132);
    wassert(actual(v->l2()) == 0);
    wassert(actual(v->valType()) == 1);

    o = Level::createGRIB1(103, 13200);
    wassert(actual(o->style()) == Level::GRIB1);
    v = dynamic_cast<level::GRIB1*>(o.get());
    wassert(actual(v->type()) == 103);
    wassert(actual(v->l1()) == 13200);
    wassert(actual(v->l2()) == 0);
    wassert(actual(v->valType()) == 1);

    o = Level::createGRIB1(104, 132, 231);
    wassert(actual(o->style()) == Level::GRIB1);
    v = dynamic_cast<level::GRIB1*>(o.get());
    wassert(actual(v->type()) == 104);
    wassert(actual(v->l1()) == 132);
    wassert(actual(v->l2()) == 231);
    wassert(actual(v->valType()) == 2);
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
    const level::GRIB2S* v;

    o = Level::createGRIB2S(100, 100, 500);
    wassert(actual(o->style()) == Level::GRIB2S);
    v = dynamic_cast<level::GRIB2S*>(o.get());
    wassert(actual(v->type()) == 100);
    wassert(actual(v->scale()) == 100);
    wassert(actual(v->value()) == 500);

    o = Level::createGRIB2S(level::GRIB2S::MISSING_TYPE, level::GRIB2S::MISSING_SCALE, level::GRIB2S::MISSING_VALUE);
    wassert(actual(o->style()) == Level::GRIB2S);
    v = dynamic_cast<level::GRIB2S*>(o.get());
    wassert(actual(v->type()) == level::GRIB2S::MISSING_TYPE);
    wassert(actual(v->scale()) == level::GRIB2S::MISSING_SCALE);
    wassert(actual(v->value()) == level::GRIB2S::MISSING_VALUE);
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
    const level::GRIB2D* v;

    o = Level::createGRIB2D(100, 100, 500, 100, 100, 1000);
    wassert(actual(o->style()) == Level::GRIB2D);
    v = dynamic_cast<level::GRIB2D*>(o.get());
    wassert(actual(v->type1()) == 100);
    wassert(actual(v->scale1()) == 100);
    wassert(actual(v->value1()) == 500);
    wassert(actual(v->type2()) == 100);
    wassert(actual(v->scale2()) == 100);
    wassert(actual(v->value2()) == 1000);

    o = Level::createGRIB2D(
            level::GRIB2S::MISSING_TYPE, level::GRIB2S::MISSING_SCALE, level::GRIB2S::MISSING_VALUE,
            level::GRIB2S::MISSING_TYPE, level::GRIB2S::MISSING_SCALE, level::GRIB2S::MISSING_VALUE);
    wassert(actual(o->style()) == Level::GRIB2D);
    v = dynamic_cast<level::GRIB2D*>(o.get());
    wassert(actual(v->type1()) == level::GRIB2S::MISSING_TYPE);
    wassert(actual(v->scale1()) == level::GRIB2S::MISSING_SCALE);
    wassert(actual(v->value1()) == level::GRIB2S::MISSING_VALUE);
    wassert(actual(v->type1()) == level::GRIB2S::MISSING_TYPE);
    wassert(actual(v->scale1()) == level::GRIB2S::MISSING_SCALE);
    wassert(actual(v->value1()) == level::GRIB2S::MISSING_VALUE);
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
    wassert(actual(o->style()) == Level::ODIMH5);
    const level::ODIMH5* v = dynamic_cast<level::ODIMH5*>(o.get());
    wassert(actual(v->max()) == 20.123);
    wassert(actual(v->min()) == 10.123);
});

// Test Lua functions
add_lua_test("lua", "GRIB1(104, 132, 231)", R"(
    function test(o)
      if o.style ~= 'GRIB1' then return 'style is '..o.style..' instead of GRIB1' end
      if o.type ~= 104 then return 'o.type first item is '..o.type..' instead of 104' end
      if o.l1 ~= 132 then return 'o.l1 second item is '..o.l1..' instead of 132' end
      if o.l2 ~= 231 then return 'o.l2 third item is '..o.l2..' instead of 231' end
      if tostring(o) ~= 'GRIB1(104, 132, 231)' then return 'tostring gave '..tostring(o)..' instead of GRIB1(104, 132, 231)' end
      o1 = arki_level.grib1(104, 132, 231)
      if o ~= o1 then return 'new level is '..tostring(o1)..' instead of '..tostring(o) end
    end
)");

}

}
