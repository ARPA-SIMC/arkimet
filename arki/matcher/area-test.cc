#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/matcher/parser.h"
#include "arki/metadata.h"
#include "arki/types/area.h"
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
} test("arki_matcher_area");

void Tests::register_tests() {

add_method("grib", [] {
    Metadata md;
    arki::tests::fill(md);

    // 0x1ffffff = 33554431
    ValueBag testArea2 = ValueBag::parse("foo=15, bar=15000, baz=-1200, moo=33554431, antani=0, blinda=-1, supercazzola=-7654321");

	Matcher m;

	ensure_matches("area:GRIB:foo=5", md);
	ensure_matches("area:GRIB:bar=5000", md);
	ensure_matches("area:GRIB:foo=5,bar=5000", md);
	ensure_matches("area:GRIB:baz=-200", md);
	ensure_matches("area:GRIB:blinda=0", md);
	ensure_matches("area:GRIB:pippo=pippo", md);
	ensure_matches("area:GRIB:pippo=\"pippo\"", md);
	ensure_matches("area:GRIB:pluto=\"12\"", md);
	ensure_not_matches("area:GRIB:pluto=12", md);
	// Check that we match the first item
	ensure_matches("area:GRIB:aaa=0", md);
	// Check that we match the last item
	ensure_matches("area:GRIB:zzz=1", md);
	ensure_not_matches("area:GRIB:foo=50", md);
	ensure_not_matches("area:GRIB:foo=-5", md);
	ensure_not_matches("area:GRIB:foo=5,miao=0", md);
	// Check matching a missing item at the beginning of the list
	ensure_not_matches("area:GRIB:a=1", md);
	// Check matching a missing item at the end of the list
	ensure_not_matches("area:GRIB:zzzz=1", md);

    md.test_set<area::GRIB>(testArea2);

	ensure_not_matches("area:GRIB:foo=5", md);
	ensure_matches("area:GRIB:foo=15", md);
});

// Try matching with "bbox equals"
add_method("bbox_equals", [] {
    skip_unless_geos();
    Metadata md;
    arki::tests::fill(md);
    md.test_set(types::Area::decodeString("GRIB(Ni=441, Nj=181, latfirst=45000000, latlast=43000000, lonfirst=10000000, lonlast=12000000, type=0)"));
    ensure_matches("area: bbox equals POLYGON((10 43, 12 43, 12 45, 10 45, 10 43))", md);
    ensure_not_matches("area: bbox equals POINT EMPTY", md);
    ensure_not_matches("area: bbox equals POINT(11 44)", md);
    ensure_not_matches("area: bbox equals POLYGON((12 43, 10 44, 12 45, 12 43))", md);
});

// Try matching with "bbox covers"
add_method("bbox_covers", [] {
    skip_unless_geos();
    Metadata md;
    arki::tests::fill(md);
    md.test_set(types::Area::decodeString("GRIB(Ni=441, Nj=181, latfirst=45000000, latlast=43000000, lonfirst=10000000, lonlast=12000000, type=0)"));
	ensure_matches("area: bbox covers POINT(11 44)", md);
	ensure_matches("area: bbox covers POINT(12 43)", md);
	ensure_matches("area: bbox covers LINESTRING(10 43, 10 45, 12 45, 12 43, 10 43)", md);
	ensure_matches("area: bbox covers POLYGON((10 43, 10 45, 12 45, 12 43, 10 43))", md);
	ensure_matches("area: bbox covers POLYGON((10.5 43.5, 10.5 44.5, 11.5 44.5, 11.5 43.5, 10.5 43.5))", md);
	ensure_matches("area: bbox covers POLYGON((12 43, 10 44, 12 45, 12 43))", md);
	//ensure_not_matches("area: bbox covers INVALID()", md);
	ensure_not_matches("area: bbox covers POINT(11 40)", md);
	ensure_not_matches("area: bbox covers POINT(13 44)", md);
	ensure_not_matches("area: bbox covers POINT( 5 40)", md);
	ensure_not_matches("area: bbox covers LINESTRING(10 43, 10 45, 13 45, 13 43, 10 43)", md);
	ensure_not_matches("area: bbox covers POLYGON((10 43, 10 45, 13 45, 13 43, 10 43))", md);
	ensure_not_matches("area: bbox covers POLYGON((12 42, 10 44, 12 45, 12 42))", md);

    // Known cases that gave trouble
    md.test_set(types::Area::decodeString("GRIB(Ni=441, Nj=181, latfirst=75000000, latlast=30000000, lonfirst=-45000000, lonlast=65000000, type=0)"));
    ensure_matches("area:bbox covers POLYGON((30 60, -20 60, -20 30, 30 30, 30 60))", md);
});

// Try matching with "bbox intersects"
add_method("bbox_intersects", [] {
    skip_unless_geos();
    Metadata md;
    arki::tests::fill(md);
    md.test_set(types::Area::decodeString("GRIB(Ni=441, Nj=181, latfirst=45000000, latlast=43000000, lonfirst=10000000, lonlast=12000000, type=0)"));
	ensure_matches("area: bbox intersects POINT(11 44)", md);
	ensure_matches("area: bbox intersects POINT(12 43)", md);
	ensure_matches("area: bbox intersects LINESTRING(10 43, 10 45, 12 45, 12 43, 10 43)", md);
	ensure_matches("area: bbox intersects POLYGON((10 43, 10 45, 12 45, 12 43, 10 43))", md);
	ensure_matches("area: bbox intersects POLYGON((10.5 43.5, 10.5 44.5, 11.5 44.5, 11.5 43.5, 10.5 43.5))", md);
	ensure_matches("area: bbox intersects POLYGON((12 43, 10 44, 12 45, 12 43))", md);
	ensure_matches("area: bbox intersects POLYGON((10 43, 10 45, 13 45, 13 43, 10 43))", md);
	ensure_matches("area: bbox intersects POLYGON((12 42, 10 44, 12 45, 12 42))", md);
	ensure_matches("area: bbox intersects POLYGON((9 40, 10 43, 10 40, 9 40))", md);  // Touches one vertex
	ensure_not_matches("area: bbox intersects POINT(11 40)", md); // Outside
	ensure_not_matches("area: bbox intersects POINT(13 44)", md); // Outside
	ensure_not_matches("area: bbox intersects POINT( 5 40)", md); // Outside
	ensure_not_matches("area: bbox intersects LINESTRING(9 40, 10 42, 10 40, 9 40)", md); // Disjoint
	ensure_not_matches("area: bbox intersects POLYGON((9 40, 10 42, 10 40, 9 40))", md);  // Disjoint
});

// Try matching with "bbox coveredby"
add_method("bbox_coveredby", [] {
    skip_unless_geos();
    Metadata md;
    arki::tests::fill(md);
    md.test_set(types::Area::decodeString("GRIB(Ni=441, Nj=181, latfirst=45000000, latlast=43000000, lonfirst=10000000, lonlast=12000000, type=0)"));
	ensure_matches("area: bbox coveredby POLYGON((10 43, 10 45, 12 45, 12 43, 10 43))", md); // Same shape
	ensure_matches("area: bbox coveredby POLYGON((10 43, 10 45, 13 45, 13 43, 10 43))", md); // Trapezoid containing area
	ensure_not_matches("area: bbox coveredby POINT(11 44)", md); // Intersection exists but area is 0
	ensure_not_matches("area: bbox coveredby POINT(12 43)", md); // Intersection exists but area is 0
	ensure_not_matches("area: bbox coveredby POINT(11 40)", md); // Outside
	ensure_not_matches("area: bbox coveredby POINT(13 44)", md); // Outside
	ensure_not_matches("area: bbox coveredby POINT( 5 40)", md); // Outside
	ensure_not_matches("area: bbox coveredby POLYGON((12 42, 10 44, 12 45, 12 42))", md);
	ensure_not_matches("area: bbox coveredby POLYGON((12 43, 10 44, 12 45, 12 43))", md); // Triangle contained inside, touching borders (?)
	ensure_not_matches("area: bbox coveredby POLYGON((10.5 43.5, 10.5 44.5, 11.5 44.5, 11.5 43.5, 10.5 43.5))", md); // Fully inside
	ensure_not_matches("area: bbox coveredby LINESTRING(10 43, 10 45, 12 45, 12 43, 10 43)", md); // Same shape, but intersection area is 0
	ensure_not_matches("area: bbox coveredby POLYGON((9 40, 10 43, 10 40, 9 40))", md);  // Touches one vertex
	ensure_not_matches("area: bbox coveredby LINESTRING(9 40, 10 42, 10 40, 9 40)", md); // Disjoint
	ensure_not_matches("area: bbox coveredby POLYGON((9 40, 10 42, 10 40, 9 40))", md);  // Disjoint

    md.test_set(types::Area::decodeString("GRIB(blo=0, lat=4502770, lon=966670, sta=101)"));
    ensure_matches("area:bbox coveredby POLYGON((9 45, 10 45, 10 46, 9 46, 9 45))", md); // Point contained
});


// Try matching Area with ODIMH5
add_method("odimh5", [] {
    using namespace arki::types::values;

    // 0x1ffffff = 33554431
    ValueBag testArea2 = ValueBag::parse("foo=15, bar=15000, baz=-1200, moo=33554431, antani=0, blinda=-1, supercazzola=-7654321");

	//Matcher m;

    Metadata md;
    md.test_set(types::Area::decodeString("ODIMH5(foo=5,bar=5000,baz=-200,blinda=0,pippo=\"pippo\",pluto=\"12\",aaa=0,zzz=1)"));

	ensure_matches("area:ODIMH5:foo=5", md);
	ensure_matches("area:ODIMH5:bar=5000", md);
	ensure_matches("area:ODIMH5:foo=5,bar=5000", md);
	ensure_matches("area:ODIMH5:baz=-200", md);
	ensure_matches("area:ODIMH5:blinda=0", md);
	ensure_matches("area:ODIMH5:pippo=pippo", md);
	ensure_matches("area:ODIMH5:pippo=\"pippo\"", md);
	ensure_matches("area:ODIMH5:pluto=\"12\"", md);
	ensure_not_matches("area:ODIMH5:pluto=12", md);
	// Check that we match the first item
	ensure_matches("area:ODIMH5:aaa=0", md);
	// Check that we match the last item
	ensure_matches("area:ODIMH5:zzz=1", md);

	ensure_not_matches("area:ODIMH5:foo=50", md);
	ensure_not_matches("area:ODIMH5:foo=-5", md);
	ensure_not_matches("area:ODIMH5:foo=5,miao=0", md);
	// Check matching a missing item at the beginning of the list
	ensure_not_matches("area:ODIMH5:a=1", md);
	// Check matching a missing item at the end of the list
	ensure_not_matches("area:ODIMH5:zzzz=1", md);

    md.test_set<area::ODIMH5>(testArea2);

    ensure_not_matches("area:ODIMH5:foo=5", md);
    ensure_matches("area:ODIMH5:foo=15", md);
});

// Try matching Area with ODIMH5
add_method("odimh5_octagon", [] {
    //Vediamo se la formula per calcolare un ottagono con centro e raggio del radar funziona
    Metadata md1;
    md1.test_set(types::Area::decodeString("ODIMH5(lon=11623600,lat=44456700,radius=100000)"));

	//il valori devono corrispondere
	ensure_matches("area:ODIMH5:lon=11623600", md1);
	ensure_matches("area:ODIMH5:lat=44456700", md1);
	ensure_matches("area:ODIMH5:radius=100000", md1);
});

// Try matching Area with ODIMH5
add_method("bbox_odimh5", [] {
    skip_unless_geos();

    //Vediamo se la formula per calcolare un ottagono con centro e raggio del radar funziona
    Metadata md1;
    md1.test_set(types::Area::decodeString("ODIMH5(lon=11623600,lat=44456700,radius=100000)"));

	//il centro deve starci per forza
	ensure_matches("area: bbox covers POINT(11.6236 44.4567)", md1);   

	//i punti estremi del blobo non ci devono stare
	ensure_not_matches("area: bbox covers POINT(0     0)", md1);	   
	ensure_not_matches("area: bbox covers POINT(90    0)", md1);	   
	ensure_not_matches("area: bbox covers POINT(-90   0)", md1);	   
	ensure_not_matches("area: bbox covers POINT(0   180)", md1);	   
	ensure_not_matches("area: bbox covers POINT(0  -180)", md1);	   
});

// Try matching Area with VM2
add_method("vm2", [] {
    skip_unless_vm2();
    matcher::Parser parser;
    Metadata md;
    md.test_set<area::VM2>(1);

	ensure_matches("area:VM2", md);
	ensure_matches("area:VM2,", md);
	ensure_matches("area:VM2,1", md);
	ensure_not_matches("area:VM2,2", md);
	ensure_not_matches("area:GRIB:lon=0,lat=0", md);

    auto e1 = wassert_throws(std::invalid_argument, parser.parse("area:VM2,ciccio=riccio"));
    wassert(actual(e1.what()).contains("is not a number"));

    ensure_not_matches("area: VM2:ciccio=riccio", md);
    ensure_not_matches("area: VM2,1:ciccio=riccio", md);
    ensure_matches("area: VM2,1:lon=1207738", md);
});

}

}
