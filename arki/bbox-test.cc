#include "bbox.h"
#include "arki/types/tests.h"
#include "arki/utils/geos.h"
#include "arki/types/area.h"
#include "arki/tests/legacy.h"
#include <cmath>
#include <memory>

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_bbox");

void Tests::register_tests() {

// Empty or unsupported area should give 0
add_method("1", [] {
    BBox bbox;
    ValueBag vb;
    unique_ptr<Area> area(Area::createGRIB(vb));
    unique_ptr<arki::utils::geos::Geometry> g(bbox(*area));

    ensure(g.get() == 0);
});

// Test normal latlon areas
add_method("2", [] {
    skip_unless_geos();

	BBox bbox;
	ValueBag vb;
	vb.set("Ni", Value::create_integer(97));
	vb.set("Nj", Value::create_integer(73));
	vb.set("latfirst", Value::create_integer(40000000));
	vb.set("latlast", Value::create_integer(46000000));
	vb.set("lonfirst", Value::create_integer(12000000));
	vb.set("lonlast", Value::create_integer(20000000));
	vb.set("type", Value::create_integer(0));

    unique_ptr<Area> area(Area::createGRIB(vb));
    auto g = bbox(*area);

	ensure(g.get() != 0);
	ensure_equals(g->getNumPoints(), 5u);
	ensure_equals(g->getGeometryType(), "Polygon");
	//ensure_equals(g->getNumGeometries(), 1u);
	//ensure(g->isRectangle());
	ensure_equals(g->getDimension(), 2);

    auto cs = g->getCoordinates();
    wassert(actual(cs->getAt(0).x) == 12.00);
    wassert(actual(cs->getAt(0).y) == 40.00);
    wassert(actual(cs->getAt(1).x) == 12.00);
    wassert(actual(cs->getAt(1).y) == 46.00);
    wassert(actual(cs->getAt(2).x) == 20.00);
    wassert(actual(cs->getAt(2).y) == 46.00);
    wassert(actual(cs->getAt(3).x) == 20.00);
    wassert(actual(cs->getAt(3).y) == 40.00);
    wassert(actual(cs->getAt(4).x) == 12.00);
    wassert(actual(cs->getAt(4).y) == 40.00);
});

// Test UTM areas
add_method("3", [] {
    skip_unless_geos();

	BBox bbox;
	ValueBag vb;
	vb.set("Ni", Value::create_integer(90));
	vb.set("Nj", Value::create_integer(52));
	vb.set("latfirst", Value::create_integer(4852500));
	vb.set("latlast", Value::create_integer(5107500));
	vb.set("lonfirst", Value::create_integer(402500));
	vb.set("lonlast", Value::create_integer(847500));
	vb.set("type", Value::create_integer(0));
	vb.set("utm", Value::create_integer(1));

    unique_ptr<Area> area(Area::createGRIB(vb));
    auto g = bbox(*area);

	ensure(g.get() != 0);
	ensure_equals(g->getNumPoints(), 5u);
	ensure_equals(g->getGeometryType(), "Polygon");
	//ensure_equals(g->getNumGeometries(), 1u);
	//ensure(g->isRectangle());
	ensure_equals(g->getDimension(), 2);

    auto cs(g->getCoordinates());
    wassert(actual(cs->getAt(0).x).almost_equal( 3.3241, 4)); // 7.7876   These are the values computed
    wassert(actual(cs->getAt(0).y).almost_equal(43.6864, 4)); // 43.8211  with the old BB algorithm
    wassert(actual(cs->getAt(1).x).almost_equal( 8.8445, 4)); // 7.7876   that however only produced
    wassert(actual(cs->getAt(1).y).almost_equal(43.8274, 4)); // 46.0347  rectangles.
    wassert(actual(cs->getAt(2).x).almost_equal( 8.8382, 4)); // 13.4906
    wassert(actual(cs->getAt(2).y).almost_equal(46.1229, 4)); // 46.0347
    wassert(actual(cs->getAt(3).x).almost_equal( 3.0946, 4)); // 13.4906
    wassert(actual(cs->getAt(3).y).almost_equal(45.9702, 4)); // 43.8211
    wassert(actual(cs->getAt(4).x).almost_equal( 3.3241, 4)); // 7.7876
    wassert(actual(cs->getAt(4).y).almost_equal(43.6864, 4)); // 43.8211

    //ARKI_GEOS_NS::Polygon* p = (ARKI_GEOS_NS::Polygon*)g.get();
});

// Test rotated latlon areas
add_method("4", [] {
    skip_unless_geos();

	BBox bbox;
	ValueBag vb;
	vb.set("Ni", Value::create_integer(447));
	vb.set("Nj", Value::create_integer(532));
	vb.set("latfirst", Value::create_integer(-21925000));
	vb.set("latlast", Value::create_integer(-8650000));
	vb.set("lonfirst", Value::create_integer(-3500000));
	vb.set("lonlast", Value::create_integer(7650000));
	vb.set("latp", Value::create_integer(-32500000));
	vb.set("lonp", Value::create_integer(10000000));
	vb.set("type", Value::create_integer(10));
	vb.set("rot", Value::create_integer(0));

    unique_ptr<Area> area(Area::createGRIB(vb));
    auto g = bbox(*area);

	ensure(g.get() != 0);
	ensure_equals(g->getNumPoints(), 28u);
	ensure_equals(g->getGeometryType(), "Polygon");
	//ensure_equals(g->getNumGeometries(), 1u);
	//ensure(g->isRectangle());
	ensure_equals(g->getDimension(), 2);

    auto cs = g->getCoordinates();
    wassert(actual(cs->getAt( 0).x).almost_equal( 6.0124, 4)); wassert(actual(cs->getAt( 0).y).almost_equal(35.4723, 4));
    wassert(actual(cs->getAt( 1).x).almost_equal( 8.1280, 4)); wassert(actual(cs->getAt( 1).y).almost_equal(35.5524, 4));
    wassert(actual(cs->getAt( 2).x).almost_equal(10.2471, 4)); wassert(actual(cs->getAt( 2).y).almost_equal(35.5746, 4));
    wassert(actual(cs->getAt( 3).x).almost_equal(12.3657, 4)); wassert(actual(cs->getAt( 3).y).almost_equal(35.5389, 4));
    wassert(actual(cs->getAt( 4).x).almost_equal(14.4800, 4)); wassert(actual(cs->getAt( 4).y).almost_equal(35.4453, 4));
    wassert(actual(cs->getAt( 5).x).almost_equal(16.5860, 4)); wassert(actual(cs->getAt( 5).y).almost_equal(35.2942, 4));
    wassert(actual(cs->getAt( 6).x).almost_equal(18.6800, 4)); wassert(actual(cs->getAt( 6).y).almost_equal(35.0860, 4));
    wassert(actual(cs->getAt( 7).x).almost_equal(18.6800, 4)); wassert(actual(cs->getAt( 7).y).almost_equal(35.0860, 4));
    wassert(actual(cs->getAt( 8).x).almost_equal(19.0614, 4)); wassert(actual(cs->getAt( 8).y).almost_equal(37.2769, 4));
    wassert(actual(cs->getAt( 9).x).almost_equal(19.4657, 4)); wassert(actual(cs->getAt( 9).y).almost_equal(39.4666, 4));
    wassert(actual(cs->getAt(10).x).almost_equal(19.8963, 4)); wassert(actual(cs->getAt(10).y).almost_equal(41.6548, 4));
    wassert(actual(cs->getAt(11).x).almost_equal(20.3571, 4)); wassert(actual(cs->getAt(11).y).almost_equal(43.8413, 4));
    wassert(actual(cs->getAt(12).x).almost_equal(20.8530, 4)); wassert(actual(cs->getAt(12).y).almost_equal(46.0258, 4));
    wassert(actual(cs->getAt(13).x).almost_equal(21.3897, 4)); wassert(actual(cs->getAt(13).y).almost_equal(48.2079, 4));
    wassert(actual(cs->getAt(14).x).almost_equal(21.3897, 4)); wassert(actual(cs->getAt(14).y).almost_equal(48.2079, 4));
    wassert(actual(cs->getAt(15).x).almost_equal(18.6560, 4)); wassert(actual(cs->getAt(15).y).almost_equal(48.4808, 4));
    wassert(actual(cs->getAt(16).x).almost_equal(15.8951, 4)); wassert(actual(cs->getAt(16).y).almost_equal(48.6793, 4));
    wassert(actual(cs->getAt(17).x).almost_equal(13.1154, 4)); wassert(actual(cs->getAt(17).y).almost_equal(48.8024, 4));
    wassert(actual(cs->getAt(18).x).almost_equal(10.3255, 4)); wassert(actual(cs->getAt(18).y).almost_equal(48.8495, 4));
    wassert(actual(cs->getAt(19).x).almost_equal( 7.5346, 4)); wassert(actual(cs->getAt(19).y).almost_equal(48.8202, 4));
    wassert(actual(cs->getAt(20).x).almost_equal( 4.7517, 4)); wassert(actual(cs->getAt(20).y).almost_equal(48.7148, 4));
    wassert(actual(cs->getAt(21).x).almost_equal( 4.7517, 4)); wassert(actual(cs->getAt(21).y).almost_equal(48.7148, 4));
    wassert(actual(cs->getAt(22).x).almost_equal( 5.0025, 4)); wassert(actual(cs->getAt(22).y).almost_equal(46.5087, 4));
    wassert(actual(cs->getAt(23).x).almost_equal( 5.2337, 4)); wassert(actual(cs->getAt(23).y).almost_equal(44.3022, 4));
    wassert(actual(cs->getAt(24).x).almost_equal( 5.4482, 4)); wassert(actual(cs->getAt(24).y).almost_equal(42.0952, 4));
    wassert(actual(cs->getAt(25).x).almost_equal( 5.6482, 4)); wassert(actual(cs->getAt(25).y).almost_equal(39.8879, 4));
    wassert(actual(cs->getAt(26).x).almost_equal( 5.8357, 4)); wassert(actual(cs->getAt(26).y).almost_equal(37.6802, 4));
    wassert(actual(cs->getAt(27).x).almost_equal( 6.0124, 4)); wassert(actual(cs->getAt(27).y).almost_equal(35.4723, 4));
    //ARKI_GEOS_NS::Polygon* p = (ARKI_GEOS_NS::Polygon*)g.get();
});

// Test that latlon areas are reported with the right amount of significant digits
add_method("5", [] {
    skip_unless_geos();

	BBox bbox;
	ValueBag vb;
	vb.set("Ni", Value::create_integer(135));
	vb.set("Nj", Value::create_integer(98));
	vb.set("latfirst", Value::create_integer(49200000));
	vb.set("latlast", Value::create_integer(34650000));
	vb.set("lonfirst", Value::create_integer(2400000));
	vb.set("lonlast", Value::create_integer(22500000));
	vb.set("type", Value::create_integer(0));

    unique_ptr<Area> area(Area::createGRIB(vb));
    auto g = bbox(*area);

	ensure(g.get() != 0);
	ensure_equals(g->getNumPoints(), 5u);
	ensure_equals(g->getGeometryType(), "Polygon");
	//ensure_equals(g->getNumGeometries(), 1u);
	//ensure(g->isRectangle());
	ensure_equals(g->getDimension(), 2);

    auto cs = g->getCoordinates();
    wassert(actual(cs->getAt(0).x) ==  2.40000);
    wassert(actual(cs->getAt(0).y) == 49.20000);
    wassert(actual(cs->getAt(1).x) ==  2.40000);
    wassert(actual(cs->getAt(1).y) == 34.65000);
    wassert(actual(cs->getAt(2).x) == 22.50000);
    wassert(actual(cs->getAt(2).y) == 34.65000);
    wassert(actual(cs->getAt(3).x) == 22.50000);
    wassert(actual(cs->getAt(3).y) == 49.20000);
    wassert(actual(cs->getAt(4).x) ==  2.40000);
    wassert(actual(cs->getAt(4).y) == 49.20000);
});

// Simplified BUFR mobile station areas
add_method("6", [] {
    skip_unless_geos();

    BBox bbox;
    ValueBag vb;
    vb.set("type", Value::create_string("mob"));
    vb.set("x", Value::create_integer(11));
    vb.set("y", Value::create_integer(45));

    unique_ptr<Area> area(Area::createGRIB(vb));
    auto g(bbox(*area));

    ensure(g.get() != 0);
    ensure_equals(g->getNumPoints(), 5u);
    ensure_equals(g->getGeometryType(), "Polygon");
    //ensure_equals(g->getNumGeometries(), 1u);
    //ensure(g->isRectangle());
    ensure_equals(g->getDimension(), 2);

    auto cs(g->getCoordinates());
    wassert(actual(cs->getAt(0).x) == 11.0);
    wassert(actual(cs->getAt(0).y) == 45.0);
    wassert(actual(cs->getAt(1).x) == 11.0);
    wassert(actual(cs->getAt(1).y) == 46.0);
    wassert(actual(cs->getAt(2).x) == 12.0);
    wassert(actual(cs->getAt(2).y) == 46.0);
    wassert(actual(cs->getAt(3).x) == 12.0);
    wassert(actual(cs->getAt(3).y) == 45.0);
    wassert(actual(cs->getAt(4).x) == 11.0);
    wassert(actual(cs->getAt(4).y) == 45.0);
});

// Experimental UTM areas
add_method("7", [] {
    skip_unless_geos();

    BBox bbox;
    unique_ptr<Area> area = Area::decodeString("GRIB(fe=0, fn=0, latfirst=4852500, latlast=5107500, lonfirst=402500, lonlast=847500, tn=32768, utm=1, zone=32)");

    auto g(bbox(*area));

    ensure(g.get() != 0);
    ensure_equals(g->getNumPoints(), 5u);
    ensure_equals(g->getGeometryType(), "Polygon");
    //ensure_equals(g->getNumGeometries(), 1u);
    //ensure(g->isRectangle());
    ensure_equals(g->getDimension(), 2);

    auto cs(g->getCoordinates());
    wassert(actual(cs->getAt(0).x).almost_equal(13.996, 3));
    wassert(actual(cs->getAt(0).y).almost_equal(43.718, 3));
    wassert(actual(cs->getAt(1).x).almost_equal(19.453, 3));
    wassert(actual(cs->getAt(1).y).almost_equal(43.347, 3));
    wassert(actual(cs->getAt(2).x).almost_equal(19.869, 3));
    wassert(actual(cs->getAt(2).y).almost_equal(45.603, 3));
    wassert(actual(cs->getAt(3).x).almost_equal(14.199, 3));
    wassert(actual(cs->getAt(3).y).almost_equal(46.005, 3));
    wassert(actual(cs->getAt(4).x).almost_equal(13.996, 3));
    wassert(actual(cs->getAt(4).y).almost_equal(43.718, 3));
});

}

}
