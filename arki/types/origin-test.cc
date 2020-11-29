#include "tests.h"
#include "origin.h"

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;

class Tests : public TypeTestCase<types::Origin>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_origin");

void Tests::register_tests() {

// Check GRIB1
add_generic_test(
        "grib1",
        { "GRIB1(1, 1, 1)", "GRIB1(1, 2, 2)", },
        "GRIB1(1, 2, 3)",
        { "GRIB1(1, 2, 4)", "GRIB1(2, 3, 4)", "GRIB2(1, 2, 3, 4, 5)", "BUFR(1, 2)", },
        "GRIB1,1,2,3");

add_method("grib1_details", [] {
    using namespace arki::types;
    std::unique_ptr<Origin> o = Origin::createGRIB1(1, 2, 3);
    wassert(actual(o->style()) == Origin::Style::GRIB1);
    unsigned c, s, p;
    o->get_GRIB1(c, s, p);
    wassert(actual(c) == 1u);
    wassert(actual(s) == 2u);
    wassert(actual(p) == 3u);
});

// Check GRIB2
add_generic_test(
    "grib2",
    { "GRIB1(1, 2, 3)", "GRIB2(1, 2, 3, 4, 4)", },
    "GRIB2(1, 2, 3, 4, 5)",
    { "GRIB2(1, 2, 3, 4, 6)", "GRIB2(2, 3, 4, 5, 6)", "BUFR(1, 2)", },
    "GRIB2,1,2,3,4,5");

add_method("grib2_details", [] {
    using namespace arki::types;
    unique_ptr<Origin> o = Origin::createGRIB2(1, 2, 3, 4, 5);
    wassert(actual(o->style()) == Origin::Style::GRIB2);
    unsigned ce, su, pt, bg, pi;
    o->get_GRIB2(ce, su, pt, bg, pi);
    wassert(actual(ce) == 1u);
    wassert(actual(su) == 2u);
    wassert(actual(pt) == 3u);
    wassert(actual(bg) == 4u);
    wassert(actual(pi) == 5u);
});

// Check BUFR
add_generic_test(
    "bufr",
    { "GRIB1(1, 2, 3)", "GRIB2(1, 2, 3, 4, 5)", "BUFR(0, 2)", "BUFR(1, 1)", },
    "BUFR(1, 2)",
    { "BUFR(1, 3)", "BUFR(2, 1)", },
    "BUFR,1,2");

add_method("bufr_details", [] {
    using namespace arki::types;
    unique_ptr<Origin> o = Origin::createBUFR(1, 2);
    wassert(actual(o->style()) == Origin::Style::BUFR);
    unsigned c, s;
    o->get_BUFR(c, s);
    wassert(actual(c) == 1u);
    wassert(actual(s) == 2u);
});

// Check ODIMH5
add_generic_test(
    "odimh5",
    { "GRIB1(1, 2, 3)", "GRIB2(1, 2, 3, 4, 5)", "BUFR(0, 2)", },
    "ODIMH5(1, 2, 3)",
    { "ODIMH5(1a, 2, 3)", "ODIMH5(1, 2a, 3)", "ODIMH5(1, 2, 3a)", "ODIMH5(1a, 2a, 3a)", },
    "ODIMH5,1,2,3");

// Check ODIMH5 with empty strings
add_generic_test(
    "odimh5_empty",
    { "GRIB1(1, 2, 3)", "GRIB2(1, 2, 3, 4, 5)", "BUFR(0, 2)", },
    "ODIMH5(, 2, 3)",
    { "ODIMH5(1a, 2, 3)", "ODIMH5(1, 2a, 3)", "ODIMH5(1, 2, 3a)", "ODIMH5(1a, 2a, 3a)", },
    "ODIMH5,,2,3");

add_method("odim_details", [] {
    using namespace arki::types;
    unique_ptr<Origin> o = Origin::createODIMH5("1", "2", "3");
    wassert(actual(o->style()) == Origin::Style::ODIMH5);
    std::string WMO, RAD, PLC;
    o->get_ODIMH5(WMO, RAD, PLC);
    wassert(actual(WMO) == "1");
    wassert(actual(RAD) == "2");
    wassert(actual(PLC) == "3");

    o = Origin::createODIMH5("", "2", "3");
    wassert(actual(o->style()) == Origin::Style::ODIMH5);
    o->get_ODIMH5(WMO, RAD, PLC);
    wassert(actual(WMO) == "");
    wassert(actual(RAD) == "2");
    wassert(actual(PLC) == "3");
});

}

}
