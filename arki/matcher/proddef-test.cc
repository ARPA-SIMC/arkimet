#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/types/proddef.h"
#include "arki/types/values.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_matcher_proddef");

void Tests::register_tests() {

// Try matching Proddef
add_method("grib", [] {
    Metadata md;
    arki::tests::fill(md);

    // 0x1ffffff = 33554431
    ValueBag testProddef2 = ValueBag::parse("foo=15, bar=15000, baz=-1200, moo=33554431, antani=0, blinda=-1, supercazzola=-7654321");

	ensure_matches("proddef:GRIB:foo=5", md);
	ensure_matches("proddef:GRIB:bar=5000", md);
	ensure_matches("proddef:GRIB:foo=5,bar=5000", md);
	ensure_matches("proddef:GRIB:baz=-200", md);
	ensure_matches("proddef:GRIB:blinda=0", md);
	ensure_matches("proddef:GRIB:pippo=pippo", md);
	ensure_matches("proddef:GRIB:pippo=\"pippo\"", md);
	ensure_matches("proddef:GRIB:pluto=\"12\"", md);
	ensure_not_matches("proddef:GRIB:pluto=12", md);
	// Check that we match the first item
	ensure_matches("proddef:GRIB:aaa=0", md);
	// Check that we match the last item
	ensure_matches("proddef:GRIB:zzz=1", md);
	ensure_not_matches("proddef:GRIB:foo=50", md);
	ensure_not_matches("proddef:GRIB:foo=-5", md);
	ensure_not_matches("proddef:GRIB:foo=5,miao=0", md);
	// Check matching a missing item at the beginning of the list
	ensure_not_matches("proddef:GRIB:a=1", md);
	// Check matching a missing item at the end of the list
	ensure_not_matches("proddef:GRIB:zzzz=1", md);

    md.test_set(Proddef::createGRIB(testProddef2));

    ensure_not_matches("proddef:GRIB:foo=5", md);
    ensure_matches("proddef:GRIB:foo=15", md);
});

}

}
