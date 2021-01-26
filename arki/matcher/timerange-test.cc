#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/matcher/parser.h"
#include "arki/metadata.h"
#include "arki/types/timerange.h"
#include "arki/matcher/timerange.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_matcher_timerange");

void Tests::register_tests() {

// Try matching Minute run
add_method("grib1", [] {
    Metadata md;
    arki::tests::fill(md);

    ensure_matches("timerange:GRIB1", md);
    ensure_matches("timerange:GRIB1,,", md);
    ensure_matches("timerange:GRIB1,2", md);
    ensure_matches("timerange:GRIB1,2,,", md);
    // Incomplete matches are now allowed
    ensure_matches("timerange:GRIB1,,22s,", md);
    ensure_matches("timerange:GRIB1,,,23s", md);
    ensure_matches("timerange:GRIB1,2,22s,", md);
    ensure_matches("timerange:GRIB1,2,,23s", md);
    ensure_matches("timerange:GRIB1,,22s,23s", md);
    ensure_matches("timerange:GRIB1,2,22s,23s", md);
    ensure_not_matches("timerange:GRIB1,2,23s,23s", md);
    ensure_not_matches("timerange:GRIB1,2,22s,22s", md);
    // ensure_not_matches("timerange:GRIB1,2,22,23");

    // Match timerange in years, which gave problems
    md.test_set(Timerange::createGRIB1(2, 4, 2, 3));
    ensure_matches("timerange:GRIB1,2,2y,3y", md);
});

// Try matching GRIB2 timerange
add_method("grib2", [] {
    Metadata md;
    arki::tests::fill(md);
    md.test_set(Timerange::createGRIB2(1, 2, 3, 4));

	ensure_matches("timerange:GRIB2", md);
	ensure_matches("timerange:GRIB2,,", md);
	ensure_matches("timerange:GRIB2,1", md);
	ensure_matches("timerange:GRIB2,1,,", md);
	ensure_matches("timerange:GRIB2,1,2,", md);
	ensure_matches("timerange:GRIB2,1,2,3", md);
	ensure_matches("timerange:GRIB2,1,2,3,4", md);
	// Incomplete matches are allowed
	ensure_matches("timerange:GRIB2,,2,", md);
	ensure_matches("timerange:GRIB2,,,3", md);
	ensure_matches("timerange:GRIB2,,,,4", md);
	ensure_not_matches("timerange:GRIB1", md);
	ensure_not_matches("timerange:GRIB2,2", md);
	ensure_not_matches("timerange:GRIB2,,3", md);
	ensure_not_matches("timerange:GRIB2,,,4", md);
	ensure_not_matches("timerange:GRIB2,,,,5", md);
});

// Try matching Timedef timerange
add_method("timedef", [] {
    Metadata md;
    arki::tests::fill(md);

    md.test_set(Timerange::decodeString("Timedef(6h,1,3h)"));
    ensure_matches("timerange:Timedef", md);
    ensure_matches("timerange:Timedef,,", md);
    ensure_matches("timerange:Timedef,6h", md);
    ensure_matches("timerange:Timedef,6h,,", md);
    ensure_matches("timerange:Timedef,6h,1,", md);
    ensure_matches("timerange:Timedef,6h,1,3h", md);
    // Incomplete matches are allowed
    ensure_matches("timerange:Timedef,,1,", md);
    ensure_matches("timerange:Timedef,,,3h", md);
    ensure_not_matches("timerange:GRIB1", md);
    ensure_not_matches("timerange:Timedef,5h", md);
    ensure_not_matches("timerange:Timedef,6h,2", md);
    ensure_not_matches("timerange:Timedef,6h,1,4h", md);
    ensure_not_matches("timerange:Timedef,,2", md);
    ensure_not_matches("timerange:Timedef,,2,4h", md);
    ensure_not_matches("timerange:Timedef,,,4h", md);
    ensure_not_matches("timerange:Timedef,-", md);
    ensure_not_matches("timerange:Timedef,6h,-", md);
    ensure_not_matches("timerange:Timedef,6h,2,-", md);

    md.test_set(Timerange::decodeString("Timedef(6h)"));
    ensure_matches("timerange:Timedef", md);
    ensure_matches("timerange:Timedef,,", md);
    ensure_matches("timerange:Timedef,6h", md);
    ensure_matches("timerange:Timedef,6h,,", md);
    ensure_matches("timerange:Timedef,6h,-", md);
    ensure_matches("timerange:Timedef,6h,-,-", md);
    ensure_not_matches("timerange:GRIB1", md);
    ensure_not_matches("timerange:Timedef,5h", md);
    ensure_not_matches("timerange:Timedef,6h,1", md);
    ensure_not_matches("timerange:Timedef,6h,1,4h", md);
    ensure_not_matches("timerange:Timedef,,2", md);
    ensure_not_matches("timerange:Timedef,,2,4h", md);
    ensure_not_matches("timerange:Timedef,,,4h", md);
    ensure_not_matches("timerange:Timedef,-", md);
    ensure_not_matches("timerange:Timedef,6h,2,-", md);

    md.test_set(Timerange::decodeString("Timedef(6h,2)"));
    ensure_matches("timerange:Timedef", md);
    ensure_matches("timerange:Timedef,,", md);
    ensure_matches("timerange:Timedef,6h", md);
    ensure_matches("timerange:Timedef,6h,,", md);
    ensure_matches("timerange:Timedef,6h,,-", md);
    ensure_matches("timerange:Timedef,6h,2,-", md);
    ensure_not_matches("timerange:GRIB1", md);
    ensure_not_matches("timerange:Timedef,5h", md);
    ensure_not_matches("timerange:Timedef,6h,1", md);
    ensure_not_matches("timerange:Timedef,6h,1,-", md);
    ensure_not_matches("timerange:Timedef,6h,2,4h", md);
    ensure_not_matches("timerange:Timedef,,2,4h", md);
    ensure_not_matches("timerange:Timedef,,,4h", md);
    ensure_not_matches("timerange:Timedef,-", md);
    ensure_not_matches("timerange:Timedef,6h,-", md);
});

// Try matching BUFR timerange
add_method("bufr", [] {
    Metadata md;
    arki::tests::fill(md);
    md.test_set(Timerange::createBUFR(2, 1));

	ensure_matches("timerange:BUFR", md);
	ensure_matches("timerange:BUFR,2h", md);
	ensure_matches("timerange:BUFR,120m", md);
	ensure_matches("timerange:BUFR,7200s", md);
	ensure_not_matches("timerange:GRIB1", md);
	ensure_not_matches("timerange:GRIB2", md);
	ensure_not_matches("timerange:BUFR,3h", md);
	ensure_not_matches("timerange:BUFR,2m", md);

    md.test_set(Timerange::createBUFR());

	ensure_matches("timerange:BUFR", md);
	ensure_matches("timerange:BUFR,0", md);
	ensure_matches("timerange:BUFR,0h", md);
	ensure_not_matches("timerange:GRIB1", md);
	ensure_not_matches("timerange:GRIB2", md);
	ensure_not_matches("timerange:BUFR,3h", md);
	ensure_not_matches("timerange:BUFR,2m", md);
});


// Try some cases that have been buggy
add_method("regression", [] {
    Metadata md;
    md.test_set(Timerange::createGRIB1(0, 254, 0, 0));
    ensure_matches("timerange:GRIB1,0,0h", md);
    ensure_matches("timerange:GRIB1,0,0h,0h", md);
    ensure_not_matches("timerange:GRIB1,0,12h", md);
    ensure_not_matches("timerange:GRIB1,0,12y", md);
    // This matches, because p2 is ignored with ptype=0
    ensure_matches("timerange:GRIB1,0,,12y", md);
});

// Test timedef matcher parsing
add_method("timedef_parse", [] {
    auto parse = [](const char* str) {
        matcher::Implementation* impl = matcher::MatchTimerange::parse(str);
        return std::unique_ptr<matcher::MatchTimerange>(dynamic_cast<matcher::MatchTimerange*>(impl));

    };
    {
        auto matcher = parse("Timedef,+72h,1,6h");
        const matcher::MatchTimerangeTimedef* m = dynamic_cast<const matcher::MatchTimerangeTimedef*>(matcher.get());
        wassert_true(m);

        wassert_true(m->step.present);
        wassert(actual(m->step.value) == 72 * 3600);
        wassert_true(m->step_is_seconds);

        wassert_true(m->proc_type.present);
        wassert(actual(m->proc_type.value) == 1);

        wassert_true(m->proc_duration.present);
        wassert(actual(m->proc_duration.value) == 6 * 3600);
        wassert_true(m->proc_duration_is_seconds);
    }

    {
        auto matcher = parse("Timedef,72h");
        const matcher::MatchTimerangeTimedef* m = dynamic_cast<const matcher::MatchTimerangeTimedef*>(matcher.get());
        wassert_true(m);

        wassert_true(m->step.present);
        wassert(actual(m->step.value) == 72 * 3600);
        wassert_true(m->step_is_seconds);

        wassert_false(m->proc_type.present);
        wassert_false(m->proc_duration.present);
    }

    {
        auto matcher = parse("Timedef,,-");
        const matcher::MatchTimerangeTimedef* m = dynamic_cast<const matcher::MatchTimerangeTimedef*>(matcher.get());
        wassert_true(m);

        wassert_false(m->step.present);

        wassert_true(m->proc_type.present);
        wassert(actual(m->proc_type.value) == -1);
    }
});

// Try matching timedef timerange on GRIB1
add_method("timedef_grib1", [] {
    Metadata md;
    arki::tests::fill(md);
    md.test_set(Timerange::createGRIB1(0, 0, 60, 0));
    ensure_matches("timerange:Timedef,1h", md);
    ensure_matches("timerange:Timedef,1h,254", md);
    ensure_matches("timerange:Timedef,60m,254,0", md);
    ensure_not_matches("timerange:Timedef,2h", md);
    ensure_not_matches("timerange:Timedef,1h,1,0", md);
    ensure_not_matches("timerange:Timedef,1h,254,1s", md);

    md.test_set(Timerange::createGRIB1(3, 1, 1, 3));
    ensure_matches("timerange:Timedef,+3h", md);
    ensure_matches("timerange:Timedef,+3h,0", md);
    ensure_matches("timerange:Timedef,+3h,0,2h", md);
    ensure_not_matches("timerange:Timedef,+2h", md);
    ensure_not_matches("timerange:Timedef,+3h,1,2h", md);
    ensure_not_matches("timerange:Timedef,+3h,0,1h", md);
});

// Try matching timedef timerange on GRIB2
add_method("timedef_grib2", [] {
    Metadata md;
    arki::tests::fill(md);
    md.test_set(Timerange::createGRIB2(3, 1, 1, 3));
    // timerange:GRIB2 metadata items do not carry enough information to feed Timedef matchers
    ensure_matches("timerange:Timedef", md);
    ensure_matches("timerange:Timedef,-", md);
    ensure_matches("timerange:Timedef,-,-", md);
    ensure_matches("timerange:Timedef,-,-,-", md);
    ensure_not_matches("timerange:Timedef,+3h", md);
    ensure_not_matches("timerange:Timedef,+3h,0", md);
    ensure_not_matches("timerange:Timedef,+3h,0,2h", md);
});

// Try matching timedef timerange on Timedef
add_method("timedef_timedef", [] {
    Metadata md;
    arki::tests::fill(md);
    md.test_set(Timerange::decodeString("Timedef(72h,1,6h)"));
    ensure_matches("timerange:Timedef,+72h", md);
    ensure_matches("timerange:Timedef,+72h,1", md);
    ensure_matches("timerange:Timedef,+72h,1,6h", md);
    ensure_matches("timerange:Timedef,+72h,1", md);
    ensure_matches("timerange:Timedef,+72h,,6h", md);
    ensure_matches("timerange:Timedef,,1", md);
    ensure_matches("timerange:Timedef,,1,6h", md);
    ensure_matches("timerange:Timedef,,,6h", md);
    ensure_not_matches("timerange:Timedef,73h", md);
    ensure_not_matches("timerange:Timedef,72h,-", md);
    ensure_not_matches("timerange:Timedef,72h,-", md);

    md.test_set(Timerange::decodeString("Timedef(72h)"));
    ensure_matches("timerange:Timedef,+72h", md);
    ensure_matches("timerange:Timedef,+72h,-", md);
    ensure_matches("timerange:Timedef,+72h,-,-", md);
    ensure_matches("timerange:Timedef,+72h,,-", md);
    ensure_matches("timerange:Timedef,,-", md);
    ensure_matches("timerange:Timedef,,-,-", md);
    ensure_matches("timerange:Timedef,,,-", md);
    ensure_not_matches("timerange:Timedef,73h", md);
    ensure_not_matches("timerange:Timedef,72h,1", md);

    md.test_set(Timerange::decodeString("Timedef(72h,1)"));
    ensure_matches("timerange:Timedef,+72h", md);
    ensure_matches("timerange:Timedef,+72h,1", md);
    ensure_matches("timerange:Timedef,+72h,1,-", md);
    ensure_matches("timerange:Timedef,+72h,,-", md);
    ensure_matches("timerange:Timedef,,1", md);
    ensure_matches("timerange:Timedef,,1,-", md);
    ensure_matches("timerange:Timedef,,,-", md);
    ensure_not_matches("timerange:Timedef,73h", md);
    ensure_not_matches("timerange:Timedef,72h,2", md);
    ensure_not_matches("timerange:Timedef,72h,1,3h", md);

    md.test_set(Timerange::decodeString("Timedef(72d,1,6h)"));
    ensure_matches("timerange:Timedef,+72d", md);
    ensure_matches("timerange:Timedef,+72d,1", md);
    ensure_matches("timerange:Timedef,+72d,1,6h", md);
    ensure_matches("timerange:Timedef,+72d,1", md);
    ensure_matches("timerange:Timedef,+72d,,6h", md);
    ensure_matches("timerange:Timedef,,1", md);
    ensure_matches("timerange:Timedef,,1,6h", md);
    ensure_matches("timerange:Timedef,,,6h", md);
    ensure_not_matches("timerange:Timedef,73d", md);
    ensure_not_matches("timerange:Timedef,72d,-", md);
    ensure_not_matches("timerange:Timedef,72d,-", md);
});

// Try matching timedef timerange on BUFR
add_method("timedef_bufr", [] {
    Metadata md;
    arki::tests::fill(md);
    md.test_set(Timerange::createBUFR(2, 1));
    ensure_matches("timerange:Timedef,+2h", md);
    ensure_matches("timerange:Timedef,+2h,-", md);
    ensure_matches("timerange:Timedef,+2h,-,-", md);
    ensure_not_matches("timerange:Timedef,+1h", md);
    ensure_not_matches("timerange:Timedef,-", md);
    ensure_not_matches("timerange:Timedef,2h,1", md);
});

// Try matching timedef timerange on known values
add_method("timedef_wellknown", [] {
    Metadata md;
    arki::tests::fill(md);
    md.test_set(Timerange::decodeString("Timedef(3h)"));
    ensure_matches("timerange:Timedef,+3h,-", md);
    ensure_not_matches("timerange:Timedef,+3h, 254", md);

    md.test_set(Timerange::decodeString("Timedef(3h)"));
    ensure_matches("timerange:Timedef,+3h,-", md);
    ensure_not_matches("timerange:Timedef,+3h, 254", md);

    md.test_set(Timerange::decodeString("Timedef(3h,254)"));
    ensure_matches("timerange:Timedef,+3h, 254", md);

    md.test_set(Timerange::decodeString("GRIB1(000, 003h)"));
    ensure_matches("timerange:Timedef,+3h, 254", md);

    md.test_set(Timerange::decodeString("Timedef(0s,254)"));
    ensure_matches("timerange:Timedef,0,254", md);

    md.test_set(Timerange::decodeString("GRIB1(000, 000h)"));
    ensure_matches("timerange:Timedef,0,254", md);

    /////
    md.test_set(Timerange::decodeString("GRIB1(004, 000h, 003h)"));
    ensure_not_matches("timerange:Timedef,3h,,1h", md);

    md.test_set(Timerange::decodeString("GRIB1(000, 000h)"));
    ensure_not_matches("timerange:Timedef,3h,,1h", md);

    md.test_set(Timerange::decodeString("GRIB1(004, 000h, 012h)"));
    ensure_not_matches("timerange:Timedef,,1,3h", md);

    md.test_set(Timerange::decodeString("GRIB1(013, 000h, 012h)"));
    ensure_not_matches("timerange:Timedef,,1,3h", md);

    md.test_set(Timerange::decodeString("GRIB1(000, 000h)"));
    ensure_not_matches("timerange:Timedef,,1,3h", md);

    md.test_set(Timerange::decodeString("GRIB1(013, 000h, 012h)"));
    ensure_matches("timerange:GRIB1,13,0", md);
});

// Test some serialisation scenarios
add_method("serialize", [] {
    matcher::Parser parser;
    Matcher m = parser.parse("timerange:Timedef,,1,3h");
    //Matcher m1 = Matcher::parse(m.toStringExpanded());
    //ensure_equals(m, m1);
    wassert(actual(m.toStringExpanded()) == "timerange:Timedef,,1,10800s");

    // Ensure that timerange expansion skips irrelevant arguments
    m = parser.parse("timerange:GRIB1,0,1h,2h");
    wassert(actual(m.toStringExpanded()) == "timerange:GRIB1,0,3600s");
    m = parser.parse("timerange:GRIB1,124,1h,2h");
    wassert(actual(m.toStringExpanded()) == "timerange:GRIB1,124,,7200s");
    m = parser.parse("timerange:GRIB1,6,1h,2h");
    wassert(actual(m.toStringExpanded()) == "timerange:GRIB1,6,3600s,7200s");
});

}

}
