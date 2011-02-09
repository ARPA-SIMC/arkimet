/*
 * Copyright (C) 2009--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/tests/test-utils.h>
#include <arki/bbox.h>
#include <arki/utils/geosdef.h>
#include <arki/types/area.h>

#include <sstream>
#include <iostream>
#include <memory>

namespace tut {
using namespace std;
using namespace arki;

struct arki_bbox_shar {
};
TESTGRP(arki_bbox);

// Empty or unsupported area should give 0
template<> template<>
void to::test<1>()
{
	BBox bbox;
	ValueBag vb;
	Item<types::Area> area(types::area::GRIB::create(vb));
	auto_ptr<ARKI_GEOS_GEOMETRY> g(bbox(area));

	ensure(g.get() == 0);
}

// Test normal latlon areas
template<> template<>
void to::test<2>()
{
#ifdef HAVE_GEOS
	BBox bbox;
	ValueBag vb;
	vb.set("Ni", Value::createInteger(97));
	vb.set("Nj", Value::createInteger(73));
	vb.set("latfirst", Value::createInteger(40000000));
	vb.set("latlast", Value::createInteger(46000000));
	vb.set("lonfirst", Value::createInteger(12000000));
	vb.set("lonlast", Value::createInteger(20000000));
	vb.set("type", Value::createInteger(0));

	Item<types::Area> area(types::area::GRIB::create(vb));
	auto_ptr<ARKI_GEOS_GEOMETRY> g(bbox(area));
	//cerr <<" AREA " << area << endl;

	ensure(g.get() != 0);
	ensure_equals(g->getNumPoints(), 5u);
	ensure_equals(g->getGeometryType(), "Polygon");
	//ensure_equals(g->getNumGeometries(), 1u);
	//ensure(g->isRectangle());
	ensure_equals(g->getDimension(), 2);

	auto_ptr<ARKI_GEOS_NS::CoordinateSequence> cs(g->getCoordinates());
	ensure_equals(cs->getAt(0).x, 12.00);
	ensure_equals(cs->getAt(0).y, 40.00);
	ensure_equals(cs->getAt(1).x, 12.00);
	ensure_equals(cs->getAt(1).y, 46.00);
	ensure_equals(cs->getAt(2).x, 20.00);
	ensure_equals(cs->getAt(2).y, 46.00);
	ensure_equals(cs->getAt(3).x, 20.00);
	ensure_equals(cs->getAt(3).y, 40.00);
	ensure_equals(cs->getAt(4).x, 12.00);
	ensure_equals(cs->getAt(4).y, 40.00);

	//ARKI_GEOS_NS::Polygon* p = (ARKI_GEOS_NS::Polygon*)g.get();
#endif
}

// Test UTM areas
template<> template<>
void to::test<3>()
{
#ifdef HAVE_GEOS
	BBox bbox;
	ValueBag vb;
	vb.set("Ni", Value::createInteger(90));
	vb.set("Nj", Value::createInteger(52));
	vb.set("latfirst", Value::createInteger(4852500));
	vb.set("latlast", Value::createInteger(5107500));
	vb.set("lonfirst", Value::createInteger(402500));
	vb.set("lonlast", Value::createInteger(847500));
	vb.set("type", Value::createInteger(0));
	vb.set("utm", Value::createInteger(1));

	Item<types::Area> area(types::area::GRIB::create(vb));
	auto_ptr<ARKI_GEOS_GEOMETRY> g(bbox(area));
	//cerr <<" AREA " << area << endl;

	ensure(g.get() != 0);
	ensure_equals(g->getNumPoints(), 5u);
	ensure_equals(g->getGeometryType(), "Polygon");
	//ensure_equals(g->getNumGeometries(), 1u);
	//ensure(g->isRectangle());
	ensure_equals(g->getDimension(), 2);

	auto_ptr<ARKI_GEOS_NS::CoordinateSequence> cs(g->getCoordinates());
	ensure_similar(cs->getAt(0).x,  3.3241, 0.0001); // 7.7876   These are the values computed
	ensure_similar(cs->getAt(0).y, 43.6864, 0.0001); // 43.8211  with the old BB algorithm
	ensure_similar(cs->getAt(1).x,  8.8445, 0.0001); // 7.7876   that however only produced
	ensure_similar(cs->getAt(1).y, 43.8274, 0.0001); // 46.0347  rectangles.
	ensure_similar(cs->getAt(2).x,  8.8382, 0.0001); // 13.4906
	ensure_similar(cs->getAt(2).y, 46.1229, 0.0001); // 46.0347
	ensure_similar(cs->getAt(3).x,  3.0946, 0.0001); // 13.4906
	ensure_similar(cs->getAt(3).y, 45.9702, 0.0001); // 43.8211
	ensure_similar(cs->getAt(4).x,  3.3241, 0.0001); // 7.7876
	ensure_similar(cs->getAt(4).y, 43.6864, 0.0001); // 43.8211

	//ARKI_GEOS_NS::Polygon* p = (ARKI_GEOS_NS::Polygon*)g.get();
#endif
}

// Test rotated latlon areas
template<> template<>
void to::test<4>()
{
#ifdef HAVE_GEOS
	BBox bbox;
	ValueBag vb;
	vb.set("Ni", Value::createInteger(447));
	vb.set("Nj", Value::createInteger(532));
	vb.set("latfirst", Value::createInteger(-21925000));
	vb.set("latlast", Value::createInteger(-8650000));
	vb.set("lonfirst", Value::createInteger(-3500000));
	vb.set("lonlast", Value::createInteger(7650000));
	vb.set("latp", Value::createInteger(-32500000));
	vb.set("lonp", Value::createInteger(10000000));
	vb.set("type", Value::createInteger(10));
	vb.set("rot", Value::createInteger(0));

	Item<types::Area> area(types::area::GRIB::create(vb));
	auto_ptr<ARKI_GEOS_GEOMETRY> g(bbox(area));
	//cerr <<" AREA " << area << endl;

	ensure(g.get() != 0);
	ensure_equals(g->getNumPoints(), 28u);
	ensure_equals(g->getGeometryType(), "Polygon");
	//ensure_equals(g->getNumGeometries(), 1u);
	//ensure(g->isRectangle());
	ensure_equals(g->getDimension(), 2);

	auto_ptr<ARKI_GEOS_NS::CoordinateSequence> cs(g->getCoordinates());
	ensure_similar(cs->getAt( 0).x,  6.0124, 0.0001); ensure_similar(cs->getAt( 0).y, 35.4723, 0.0001);
	ensure_similar(cs->getAt( 1).x,  8.1280, 0.0001); ensure_similar(cs->getAt( 1).y, 35.5524, 0.0001);
	ensure_similar(cs->getAt( 2).x, 10.2471, 0.0001); ensure_similar(cs->getAt( 2).y, 35.5746, 0.0001);
	ensure_similar(cs->getAt( 3).x, 12.3657, 0.0001); ensure_similar(cs->getAt( 3).y, 35.5389, 0.0001);
	ensure_similar(cs->getAt( 4).x, 14.4800, 0.0001); ensure_similar(cs->getAt( 4).y, 35.4453, 0.0001);
	ensure_similar(cs->getAt( 5).x, 16.5860, 0.0001); ensure_similar(cs->getAt( 5).y, 35.2942, 0.0001);
	ensure_similar(cs->getAt( 6).x, 18.6800, 0.0001); ensure_similar(cs->getAt( 6).y, 35.0860, 0.0001);
	ensure_similar(cs->getAt( 7).x, 18.6800, 0.0001); ensure_similar(cs->getAt( 7).y, 35.0860, 0.0001);
	ensure_similar(cs->getAt( 8).x, 19.0614, 0.0001); ensure_similar(cs->getAt( 8).y, 37.2769, 0.0001);
	ensure_similar(cs->getAt( 9).x, 19.4657, 0.0001); ensure_similar(cs->getAt( 9).y, 39.4666, 0.0001);
	ensure_similar(cs->getAt(10).x, 19.8963, 0.0001); ensure_similar(cs->getAt(10).y, 41.6548, 0.0001);
	ensure_similar(cs->getAt(11).x, 20.3571, 0.0001); ensure_similar(cs->getAt(11).y, 43.8413, 0.0001);
	ensure_similar(cs->getAt(12).x, 20.8530, 0.0001); ensure_similar(cs->getAt(12).y, 46.0258, 0.0001);
	ensure_similar(cs->getAt(13).x, 21.3897, 0.0001); ensure_similar(cs->getAt(13).y, 48.2079, 0.0001);
	ensure_similar(cs->getAt(14).x, 21.3897, 0.0001); ensure_similar(cs->getAt(14).y, 48.2079, 0.0001);
	ensure_similar(cs->getAt(15).x, 18.6560, 0.0001); ensure_similar(cs->getAt(15).y, 48.4808, 0.0001);
	ensure_similar(cs->getAt(16).x, 15.8951, 0.0001); ensure_similar(cs->getAt(16).y, 48.6793, 0.0001);
	ensure_similar(cs->getAt(17).x, 13.1154, 0.0001); ensure_similar(cs->getAt(17).y, 48.8024, 0.0001);
	ensure_similar(cs->getAt(18).x, 10.3255, 0.0001); ensure_similar(cs->getAt(18).y, 48.8495, 0.0001);
	ensure_similar(cs->getAt(19).x,  7.5346, 0.0001); ensure_similar(cs->getAt(19).y, 48.8202, 0.0001);
	ensure_similar(cs->getAt(20).x,  4.7517, 0.0001); ensure_similar(cs->getAt(20).y, 48.7148, 0.0001);
	ensure_similar(cs->getAt(21).x,  4.7517, 0.0001); ensure_similar(cs->getAt(21).y, 48.7148, 0.0001);
	ensure_similar(cs->getAt(22).x,  5.0025, 0.0001); ensure_similar(cs->getAt(22).y, 46.5087, 0.0001);
	ensure_similar(cs->getAt(23).x,  5.2337, 0.0001); ensure_similar(cs->getAt(23).y, 44.3022, 0.0001);
	ensure_similar(cs->getAt(24).x,  5.4482, 0.0001); ensure_similar(cs->getAt(24).y, 42.0952, 0.0001);
	ensure_similar(cs->getAt(25).x,  5.6482, 0.0001); ensure_similar(cs->getAt(25).y, 39.8879, 0.0001);
	ensure_similar(cs->getAt(26).x,  5.8357, 0.0001); ensure_similar(cs->getAt(26).y, 37.6802, 0.0001);
	ensure_similar(cs->getAt(27).x,  6.0124, 0.0001); ensure_similar(cs->getAt(27).y, 35.4723, 0.0001);
	//ARKI_GEOS_NS::Polygon* p = (ARKI_GEOS_NS::Polygon*)g.get();
#endif
}

// Test that latlon areas are reported with the right amount of significant digits
template<> template<>
void to::test<5>()
{
#ifdef HAVE_GEOS
	BBox bbox;
	ValueBag vb;
	vb.set("Ni", Value::createInteger(135));
	vb.set("Nj", Value::createInteger(98));
	vb.set("latfirst", Value::createInteger(49200000));
	vb.set("latlast", Value::createInteger(34650000));
	vb.set("lonfirst", Value::createInteger(2400000));
	vb.set("lonlast", Value::createInteger(22500000));
	vb.set("type", Value::createInteger(0));

	Item<types::Area> area(types::area::GRIB::create(vb));
	auto_ptr<ARKI_GEOS_GEOMETRY> g(bbox(area));
	//cerr <<" AREA " << area << endl;

	ensure(g.get() != 0);
	ensure_equals(g->getNumPoints(), 5u);
	ensure_equals(g->getGeometryType(), "Polygon");
	//ensure_equals(g->getNumGeometries(), 1u);
	//ensure(g->isRectangle());
	ensure_equals(g->getDimension(), 2);

	auto_ptr<ARKI_GEOS_NS::CoordinateSequence> cs(g->getCoordinates());
	ensure_equals(cs->getAt(0).x,  2.40000);
	ensure_equals(cs->getAt(0).y, 49.20000);
	ensure_equals(cs->getAt(1).x,  2.40000);
	ensure_equals(cs->getAt(1).y, 34.65000);
	ensure_equals(cs->getAt(2).x, 22.50000);
	ensure_equals(cs->getAt(2).y, 34.65000);
	ensure_equals(cs->getAt(3).x, 22.50000);
	ensure_equals(cs->getAt(3).y, 49.20000);
	ensure_equals(cs->getAt(4).x,  2.40000);
	ensure_equals(cs->getAt(4).y, 49.20000);

	//ARKI_GEOS_NS::Polygon* p = (ARKI_GEOS_NS::Polygon*)g.get();
#endif
}

// Simplified BUFR mobile station areas
template<> template<>
void to::test<6>()
{
#ifdef HAVE_GEOS
    BBox bbox;
    ValueBag vb;
    vb.set("type", Value::createString("mob"));
    vb.set("x", Value::createInteger(11));
    vb.set("y", Value::createInteger(45));

    Item<types::Area> area(types::area::GRIB::create(vb));
    auto_ptr<ARKI_GEOS_GEOMETRY> g(bbox(area));
    //cerr <<" AREA " << area << endl;

    ensure(g.get() != 0);
    ensure_equals(g->getNumPoints(), 5u);
    ensure_equals(g->getGeometryType(), "Polygon");
    //ensure_equals(g->getNumGeometries(), 1u);
    //ensure(g->isRectangle());
    ensure_equals(g->getDimension(), 2);

    auto_ptr<ARKI_GEOS_NS::CoordinateSequence> cs(g->getCoordinates());
    ensure_equals(cs->getAt(0).x, 11.0);
    ensure_equals(cs->getAt(0).y, 45.0);
    ensure_equals(cs->getAt(1).x, 11.0);
    ensure_equals(cs->getAt(1).y, 46.0);
    ensure_equals(cs->getAt(2).x, 12.0);
    ensure_equals(cs->getAt(2).y, 46.0);
    ensure_equals(cs->getAt(3).x, 12.0);
    ensure_equals(cs->getAt(3).y, 45.0);
    ensure_equals(cs->getAt(4).x, 11.0);
    ensure_equals(cs->getAt(4).y, 45.0);
#endif
}


}

// vim:set ts=4 sw=4:
