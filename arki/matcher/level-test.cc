#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/types/level.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_matcher_level");

void Tests::register_tests() {

// Try matching GRIB1 level
add_method("grib1", [] {
    Metadata md;
    arki::tests::fill(md);

	ensure_matches("level:GRIB1", md);
	ensure_matches("level:GRIB1,,,", md);
	ensure_matches("level:GRIB1,110,,", md);
	ensure_matches("level:GRIB1,,12,", md);
	ensure_matches("level:GRIB1,,,13", md);
	ensure_matches("level:GRIB1,110,12,", md);
	ensure_matches("level:GRIB1,110,,13", md);
	ensure_matches("level:GRIB1,,12,13", md);
	ensure_matches("level:GRIB1,110,12,13", md);
	ensure_not_matches("level:GRIB1,12", md);
	ensure_not_matches("level:GRIB1,,13", md);
	ensure_not_matches("level:GRIB1,,,11", md);
});

// Try matching GRIB2S level
add_method("grib2s", [] {
    Metadata md;
    arki::tests::fill(md);
    md.set(level::GRIB2S::create(110, 12, 13));

	ensure_matches("level:GRIB2S", md);
	ensure_matches("level:GRIB2S,,,", md);
	ensure_matches("level:GRIB2S,110,,", md);
	ensure_matches("level:GRIB2S,,12,", md);
	ensure_matches("level:GRIB2S,,,13", md);
	ensure_matches("level:GRIB2S,110,12,", md);
	ensure_matches("level:GRIB2S,110,,13", md);
	ensure_matches("level:GRIB2S,,12,13", md);
	ensure_matches("level:GRIB2S,110,12,13", md);
	ensure_not_matches("level:GRIB2S,12", md);
	ensure_not_matches("level:GRIB2S,,13", md);
	ensure_not_matches("level:GRIB2S,,,11", md);
	//m = Matcher::parse("level:BUFR,11,12,13");
	//ensure(not m(md));

    md.set(level::GRIB2S::create(level::GRIB2S::MISSING_TYPE, level::GRIB2S::MISSING_SCALE, level::GRIB2S::MISSING_VALUE));

    ensure_matches("level:GRIB2S", md);
    ensure_matches("level:GRIB2S,,,", md);
    ensure_matches("level:GRIB2S,-,,", md);
    ensure_matches("level:GRIB2S,,-,", md);
    ensure_matches("level:GRIB2S,,,-", md);
    ensure_matches("level:GRIB2S,-,-,", md);
    ensure_matches("level:GRIB2S,-,,-", md);
    ensure_matches("level:GRIB2S,,-,-", md);
    ensure_matches("level:GRIB2S,-,-,-", md);
    ensure_not_matches("level:GRIB2S,1", md);
    ensure_not_matches("level:GRIB2S,,1", md);
    ensure_not_matches("level:GRIB2S,,,1", md);
    ensure_not_matches("level:GRIB2S,1,-", md);
    ensure_not_matches("level:GRIB2S,-,1", md);
    ensure_not_matches("level:GRIB2S,-,-,1", md);
});

// Try matching GRIB2D level
add_method("grib2d", [] {
    Metadata md;
    arki::tests::fill(md);
    md.set(level::GRIB2D::create(110, 12, 13, 114, 15, 16));

	ensure_matches("level:GRIB2D", md);
	ensure_matches("level:GRIB2D,,,", md);
	ensure_matches("level:GRIB2D,110,,", md);
	ensure_matches("level:GRIB2D,,12,", md);
	ensure_matches("level:GRIB2D,,,13", md);
	ensure_matches("level:GRIB2D,110,12,", md);
	ensure_matches("level:GRIB2D,110,,13", md);
	ensure_matches("level:GRIB2D,,12,13", md);
	ensure_matches("level:GRIB2D,110,12,13", md);
	ensure_matches("level:GRIB2D,110,12,13,114", md);
	ensure_matches("level:GRIB2D,110,12,13,114,15", md);
	ensure_matches("level:GRIB2D,110,12,13,114,15,16", md);
	ensure_matches("level:GRIB2D,,,,114,15,16", md);
	ensure_matches("level:GRIB2D,,,,114", md);
	ensure_not_matches("level:GRIB2D,12", md);
	ensure_not_matches("level:GRIB2D,,13", md);
	ensure_not_matches("level:GRIB2D,,,11", md);
	ensure_not_matches("level:GRIB2D,,,,115", md);
	//m = Matcher::parse("level:BUFR,11,12,13");
	//ensure(not m(md));
    md.set(level::GRIB2D::create(level::GRIB2S::MISSING_TYPE, level::GRIB2S::MISSING_SCALE, level::GRIB2S::MISSING_VALUE,
                                 level::GRIB2S::MISSING_TYPE, level::GRIB2S::MISSING_SCALE, level::GRIB2S::MISSING_VALUE));

    ensure_matches("level:GRIB2D", md);
    ensure_matches("level:GRIB2D,,,", md);
    ensure_matches("level:GRIB2D,-,,", md);
    ensure_matches("level:GRIB2D,,-,", md);
    ensure_matches("level:GRIB2D,,,-", md);
    ensure_matches("level:GRIB2D,-,-,", md);
    ensure_matches("level:GRIB2D,-,,-", md);
    ensure_matches("level:GRIB2D,,-,-", md);
    ensure_matches("level:GRIB2D,-,-,-", md);
    ensure_matches("level:GRIB2D,-,-,-,-", md);
    ensure_matches("level:GRIB2D,-,-,-,-,-", md);
    ensure_matches("level:GRIB2D,-,-,-,-,-", md);
    ensure_not_matches("level:GRIB2D,1", md);
    ensure_not_matches("level:GRIB2D,,1", md);
    ensure_not_matches("level:GRIB2D,,,1", md);
    ensure_not_matches("level:GRIB2D,1,-", md);
    ensure_not_matches("level:GRIB2D,-,1", md);
    ensure_not_matches("level:GRIB2D,-,-,1", md);
    ensure_not_matches("level:GRIB2D,-,-,1", md);
    ensure_not_matches("level:GRIB2D,-,-,-,1", md);
    ensure_not_matches("level:GRIB2D,-,-,-,-,1", md);
});

// Try matching ODIMH5 level
add_method("odimh5", [] {
    Metadata md;
    arki::tests::fill(md);
    md.set(level::ODIMH5::create(10.123, 20.123));

	ensure_matches("level:ODIMH5", md);
	ensure_matches("level:ODIMH5,", md);

	ensure_matches("level:ODIMH5,11 12 13 14 15", md);
	ensure_matches("level:ODIMH5,11 12 13 14 15 offset 0.5", md);
	ensure_matches("level:ODIMH5,10 offset 0.5", md);
	ensure_matches("level:ODIMH5,20.5 offset 0.5", md);

	ensure_matches("level:ODIMH5,range 0, 30", md);
	ensure_matches("level:ODIMH5,range 0, 15", md);
	ensure_matches("level:ODIMH5,range 15, 30", md);
	ensure_matches("level:ODIMH5,range 12, 17", md);

	ensure_not_matches("level:ODIMH5,30", md);
	ensure_not_matches("level:ODIMH5,30 offset 5", md);
	ensure_not_matches("level:ODIMH5,0 offset 5", md);
	ensure_not_matches("level:ODIMH5,range 0 5", md);
	ensure_not_matches("level:ODIMH5,range 21 30", md);

	ensure_not_matches("level:GRIB2D,12", md);
	ensure_not_matches("level:GRIB2D,,13", md);
	ensure_not_matches("level:GRIB2D,,,11", md);
	ensure_not_matches("level:GRIB2D,,,,115", md);
});

}

}
