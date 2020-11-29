#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/types/origin.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_matcher_origin");

void Tests::register_tests() {

// Try matching GRIB origin
add_method("grib1", [] {
    Metadata md;
    arki::tests::fill(md);

	ensure_matches("origin:GRIB1", md);
	ensure_matches("origin:GRIB1,,,", md);
	ensure_matches("origin:GRIB1,1,,", md);
	ensure_matches("origin:GRIB1,,2,", md);
	ensure_matches("origin:GRIB1,,,3", md);
	ensure_matches("origin:GRIB1,1,2,", md);
	ensure_matches("origin:GRIB1,1,,3", md);
	ensure_matches("origin:GRIB1,,2,3", md);
	ensure_matches("origin:GRIB1,1,2,3", md);
	ensure_not_matches("origin:GRIB1,2", md);
	ensure_not_matches("origin:GRIB1,,3", md);
	ensure_not_matches("origin:GRIB1,,,1", md);
	ensure_not_matches("origin:BUFR,1,2,3", md);
});

// Try matching BUFR origin
add_method("bufr", [] {
    Metadata md;
    arki::tests::fill(md);
    md.set(Origin::createBUFR(1, 2));

	ensure_matches("origin:BUFR", md);
	ensure_matches("origin:BUFR,,", md);
	ensure_matches("origin:BUFR,1,", md);
	ensure_matches("origin:BUFR,,2", md);
	ensure_matches("origin:BUFR,1,2", md);
	ensure_not_matches("origin:BUFR,2", md);
	ensure_not_matches("origin:BUFR,,3", md);
	ensure_not_matches("origin:BUFR,,1", md);
	ensure_not_matches("origin:BUFR,2,3", md);
});

// Try matching ODIMH5 origin
add_method("odimh5", [] {
    Metadata md;
    arki::tests::fill(md);
    md.set(Origin::createODIMH5("wmo", "rad", "plc"));

	ensure_matches("origin:ODIMH5", md);
	ensure_matches("origin:ODIMH5,,,", md);
	ensure_matches("origin:ODIMH5,wmo,,", md);
	ensure_matches("origin:ODIMH5,,rad,", md);
	ensure_matches("origin:ODIMH5,,,plc", md);
	ensure_matches("origin:ODIMH5,wmo,rad,", md);
	ensure_matches("origin:ODIMH5,wmo,,plc", md);
	ensure_matches("origin:ODIMH5,wmo,rad,plc", md);

	ensure_not_matches("origin:ODIMH5,x,,", md);
	ensure_not_matches("origin:ODIMH5,,x,", md);
	ensure_not_matches("origin:ODIMH5,,,x", md);
	ensure_not_matches("origin:ODIMH5,wmo,x,", md);
	ensure_not_matches("origin:ODIMH5,wmo,,x", md);
	ensure_not_matches("origin:ODIMH5,wmo,x,x", md);
});

}

}
