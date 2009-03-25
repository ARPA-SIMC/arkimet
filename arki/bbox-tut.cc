/*
 * Copyright (C) 2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
	BBox bbox;
	ValueBag vb;
	vb.set("Ni", Value::createInteger(97));
	vb.set("Nj", Value::createInteger(73));
	vb.set("latfirst", Value::createInteger(40000));
	vb.set("latlast", Value::createInteger(46000));
	vb.set("lonfirst", Value::createInteger(12000));
	vb.set("lonlast", Value::createInteger(20000));
	vb.set("type", Value::createInteger(0));

	Item<types::Area> area(types::area::GRIB::create(vb));
	auto_ptr<ARKI_GEOS_GEOMETRY> g(bbox(area));
	//cerr <<" AREA " << area << endl;

	ensure(g.get() != 0);
	ensure_equals(g->getNumPoints(), 5u);
	ensure_equals(g->getGeometryType(), "Polygon");
	ensure_equals(g->getNumGeometries(), 1u);
	//ensure(g->isRectangle());
	ensure_equals(g->getDimension(), 2);

	auto_ptr<ARKI_GEOS_NS::CoordinateSequence> cs(g->getCoordinates());
	ensure_equals((*cs)[0].x, 12.00);
	ensure_equals((*cs)[0].y, 40.00);
	ensure_equals((*cs)[1].x, 12.00);
	ensure_equals((*cs)[1].y, 46.00);
	ensure_equals((*cs)[2].x, 20.00);
	ensure_equals((*cs)[2].y, 46.00);
	ensure_equals((*cs)[3].x, 20.00);
	ensure_equals((*cs)[3].y, 40.00);
	ensure_equals((*cs)[4].x, 12.00);
	ensure_equals((*cs)[4].y, 40.00);

	//ARKI_GEOS_NS::Polygon* p = (ARKI_GEOS_NS::Polygon*)g.get();
}

// Test UTM areas
template<> template<>
void to::test<3>()
{
	BBox bbox;
	ValueBag vb;
	vb.set("Ni", Value::createInteger(90));
	vb.set("Nj", Value::createInteger(52));
	vb.set("latfirst", Value::createInteger(4852500));
	vb.set("latlast", Value::createInteger(5107500));
	vb.set("lonfirst", Value::createInteger(402500));
	vb.set("lonlast", Value::createInteger(847500));
	vb.set("type", Value::createInteger(0));

	Item<types::Area> area(types::area::GRIB::create(vb));
	auto_ptr<ARKI_GEOS_GEOMETRY> g(bbox(area));
	//cerr <<" AREA " << area << endl;

	ensure(g.get() != 0);
	ensure_equals(g->getNumPoints(), 5u);
	ensure_equals(g->getGeometryType(), "Polygon");
	ensure_equals(g->getNumGeometries(), 1u);
	//ensure(g->isRectangle());
	ensure_equals(g->getDimension(), 2);

	auto_ptr<ARKI_GEOS_NS::CoordinateSequence> cs(g->getCoordinates());
	ensure_similar((*cs)[0].x,  3.3241, 0.0001); // 7.7876   These are the values computed
	ensure_similar((*cs)[0].y, 43.6864, 0.0001); // 43.8211  with the old BB algorithm
	ensure_similar((*cs)[1].x,  8.8445, 0.0001); // 7.7876   that however only produced
	ensure_similar((*cs)[1].y, 43.8274, 0.0001); // 46.0347  rectangles.
	ensure_similar((*cs)[2].x,  8.8382, 0.0001); // 13.4906
	ensure_similar((*cs)[2].y, 46.1229, 0.0001); // 46.0347
	ensure_similar((*cs)[3].x,  3.0946, 0.0001); // 13.4906
	ensure_similar((*cs)[3].y, 45.9702, 0.0001); // 43.8211
	ensure_similar((*cs)[4].x,  3.3241, 0.0001); // 7.7876
	ensure_similar((*cs)[4].y, 43.6864, 0.0001); // 43.8211

	//ARKI_GEOS_NS::Polygon* p = (ARKI_GEOS_NS::Polygon*)g.get();
}


}

// vim:set ts=4 sw=4:
