#include "config.h"
#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/matcher/parser.h"
#include "arki/metadata.h"
#include "arki/core/cfg.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <fcntl.h>

using namespace std;
using namespace arki::tests;
using namespace arki::core;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace {

struct Fixture : public arki::utils::tests::Fixture
{
    using arki::utils::tests::Fixture::Fixture;

    Metadata md;

    void test_setup()
    {
        md.clear();
        arki::tests::fill(md);
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_matcher");


void Tests::register_tests() {

// Test toString()
// Kind of pointless now, since it just returns the original unparsed string
add_method("tostring", [](Fixture& f) {
    matcher::Parser parser;
    wassert(actual(parser.parse("origin:GRIB1,1,,3 or BUFR,1").toString()) == "origin:GRIB1,1,,3 or BUFR,1");
    wassert(actual(parser.parse("reftime:>2015-06-01 09:00:00 % 24h").toStringExpanded()) == "reftime:>2015-06-01 09:00:00 % 24");
});

// Try OR matches
add_method("or", [](Fixture& f) {
    matcher::Parser parser;
    Matcher m;

    m = parser.parse("origin:GRIB1 OR BUFR");
    wassert_true(m(f.md));

    m = parser.parse("origin:BUFR or GRIB1");
    wassert_true(m(f.md));

    m = parser.parse("origin:BUFR or BUFR");
    wassert_false(m(f.md));
});

// Try matching an unset metadata (see #166)
add_method("unset", [](Fixture& f) {
    matcher::Parser parser;
    Metadata md;
    Matcher m;

    m = parser.parse("origin:GRIB1");
    wassert_false(m(md));

    m = parser.parse("product:BUFR");
    wassert_false(m(md));

    m = parser.parse("level:GRIB1");
    wassert_false(m(md));

    m = parser.parse("timerange:GRIB1");
    wassert_false(m(md));

    m = parser.parse("proddef:GRIB");
    wassert_false(m(md));

    m = parser.parse("area:GRIB:foo=5");
    wassert_false(m(md));

    m = parser.parse("quantity:VRAD");
    wassert_false(m(md));

    m = parser.parse("task:test");
    wassert_false(m(md));

    m = parser.parse("reftime:=2018");
    wassert_false(m(md));
});

add_method("regression", [](Fixture& f) {
    matcher::Parser parser;

    auto m1 = parser.parse("origin:GRIB1 OR BUFR\n    ");
    auto m2 = parser.parse("origin:GRIB1 OR BUFR;\n   \n;   \n  ;\n");
    wassert(actual(m1.toString()) == m2.toString());
});

}

}
