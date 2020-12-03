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

    std::shared_ptr<Metadata> md;

    void test_setup()
    {
        md = std::make_shared<Metadata>();
        arki::tests::fill(*md);
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
    wassert(actual(parser.parse("reftime:>2015-06-01 09:00:00 % 24h").toStringExpanded()) == "reftime:>=2015-06-01 09:00:01,@09:00:00%24h");
});

// Try OR matches
add_method("or", [](Fixture& f) {
    wassert(actual_matcher("origin:GRIB1 OR BUFR").matches(f.md));
    wassert(actual_matcher("origin:BUFR or GRIB1").matches(f.md));
    wassert(actual_matcher("origin:BUFR OR BUFR").not_matches(f.md));
});

// Try matching an unset metadata (see #166)
add_method("unset", [](Fixture& f) {
    auto md = std::make_shared<Metadata>();
    wassert(actual_matcher("origin:GRIB1").not_matches(md));
    wassert(actual_matcher("product:BUFR").not_matches(md));
    wassert(actual_matcher("level:GRIB1").not_matches(md));
    wassert(actual_matcher("timerange:GRIB1").not_matches(md));
    wassert(actual_matcher("proddef:GRIB").not_matches(md));
    wassert(actual_matcher("area:GRIB:foo=5").not_matches(md));
    wassert(actual_matcher("quantity:VRAD").not_matches(md));
    wassert(actual_matcher("task:test").not_matches(md));
    wassert(actual_matcher("reftime:=2018").not_matches(md));
});

add_method("regression", [](Fixture& f) {
    matcher::Parser parser;

    auto m1 = parser.parse("origin:GRIB1 OR BUFR\n    ");
    auto m2 = parser.parse("origin:GRIB1 OR BUFR;\n   \n;   \n  ;\n");
    wassert(actual(m1.toString()) == m2.toString());
});

}

}
