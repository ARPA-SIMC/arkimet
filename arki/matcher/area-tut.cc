/*
 * Copyright (C) 2007--2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/matcher/test-utils.h>
#include <arki/matcher.h>
#include <arki/metadata.h>
#include <arki/types/area.h>
#include <arki/configfile.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;

struct arki_matcher_area_shar {
	Metadata md;

	arki_matcher_area_shar()
	{
		md.create();
		arki::tests::fill(md);
	}
};
TESTGRP(arki_matcher_area);

// Try matching Area
template<> template<>
void to::test<1>()
{
	ValueBag testArea2;
	testArea2.set("foo", Value::createInteger(15));
	testArea2.set("bar", Value::createInteger(5000));
	testArea2.set("baz", Value::createInteger(-1200));
	testArea2.set("moo", Value::createInteger(0x1ffffff));
	testArea2.set("antani", Value::createInteger(0));
	testArea2.set("blinda", Value::createInteger(-1));
	testArea2.set("supercazzola", Value::createInteger(-7654321));

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

	md.set(area::GRIB::create(testArea2));

	ensure_not_matches("area:GRIB:foo=5", md);
	ensure_matches("area:GRIB:foo=15", md);
}

// Try matching with "bbox equals"
template<> template<>
void to::test<2>()
{
#ifdef HAVE_GEOS
	//md.set(types::Area::decodeString("GRIB(Ni=1, Nj=1, latfirst=44000, latlast=44000, lonfirst=11000, lonlast=11000, type=0)"));
	//ensure_matches("area: bbox is POINT(44 11)", md);
	//ensure_not_matches("bbox:is INVALID()", md);
	//ensure_not_matches("bbox:is BOX(43, 45, 10, 12)", md);
	//ensure_not_matches("bbox:is HULL(43 12, 44 10, 45 12, 43 12)", md);

	md.set(types::Area::decodeString("GRIB(Ni=441, Nj=181, latfirst=45000000, latlast=43000000, lonfirst=10000000, lonlast=12000000, type=0)"));
	ensure_matches("area: bbox equals POLYGON((10 43, 12 43, 12 45, 10 45, 10 43))", md);
	ensure_not_matches("area: bbox equals POINT EMPTY", md);
	ensure_not_matches("area: bbox equals POINT(11 44)", md);
	ensure_not_matches("area: bbox equals POLYGON((12 43, 10 44, 12 45, 12 43))", md);

	//std::vector< std::pair<float, float> > points;
	//points.push_back(make_pair(43.0, 12.0));
	//points.push_back(make_pair(44.0, 10.0));
	//points.push_back(make_pair(45.0, 12.0));
	//points.push_back(make_pair(43.0, 12.0));
	//md.set(bbox::HULL::create(points));
	//ensure_matches("bbox:is HULL(43 12, 44 10, 45 12, 43 12)", md);
	//ensure_not_matches("bbox:is INVALID()", md);
	//ensure_not_matches("bbox:is POINT(44, 11)", md);
	//ensure_not_matches("bbox:is BOX(43, 45, 10, 12)", md);
#endif
}

// Try matching with "bbox covers"
template<> template<>
void to::test<3>()
{
#ifdef HAVE_GEOS
	//md.set(bbox::INVALID::create());
	//ensure_not_matches("bbox:contains INVALID()", md);
	//ensure_not_matches("bbox:contains POINT(44, 11)", md);
	//ensure_not_matches("bbox:contains BOX(43, 45, 10, 12)", md);
	//ensure_not_matches("bbox:contains HULL(43 12, 44 10, 45 12, 43 12)", md);

	//md.set(bbox::POINT::create(44, 11));
	//ensure_matches("bbox:contains POINT(44, 11)", md);
	//ensure_not_matches("bbox:contains INVALID()", md);
	//ensure_not_matches("bbox:contains BOX(43, 45, 10, 12)", md);
	//ensure_not_matches("bbox:contains HULL(43 12, 44 10, 45 12, 43 12)", md);

	md.set(types::Area::decodeString("GRIB(Ni=441, Nj=181, latfirst=45000000, latlast=43000000, lonfirst=10000000, lonlast=12000000, type=0)"));
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
	md.set(types::Area::decodeString("GRIB(Ni=441, Nj=181, latfirst=75000000, latlast=30000000, lonfirst=-45000000, lonlast=65000000, type=0)"));
	ensure_matches("area:bbox covers POLYGON((30 60, -20 60, -20 30, 30 30, 30 60))", md);

	//std::vector< std::pair<float, float> > points;
	//points.push_back(make_pair(43.0, 12.0));
	//points.push_back(make_pair(44.0, 12.0));
	//points.push_back(make_pair(45.0, 13.0));
	//points.push_back(make_pair(42.0, 13.0));
	//points.push_back(make_pair(43.0, 12.0));
	//md.set(bbox::HULL::create(points));
	//ensure_matches("bbox:contains POINT(43.5, 12.5)", md);
	//ensure_matches("bbox:contains POINT(43, 12)", md);
	//ensure_matches("bbox:contains BOX(43, 44, 12, 13)", md);
	//ensure_matches("bbox:contains BOX(43.2, 43.8, 12.2, 12.8)", md);
	//ensure_matches("bbox:contains HULL(43.5 12, 44 13, 43 13, 43.5 12)", md);
	//ensure_matches("bbox:contains HULL(43 12, 44 12, 45 13, 42 13, 43 12)", md);
	//ensure_matches("bbox:contains HULL(43 12, 44 12, 44 13, 43 13, 43 12)", md);
	//ensure_not_matches("bbox:contains INVALID()", md);
	//ensure_not_matches("bbox:contains POINT(44, 11)", md);
	//ensure_not_matches("bbox:contains BOX(42, 45, 12, 13)", md);
#endif
}

// Try matching with "bbox intersects"
template<> template<>
void to::test<4>()
{
#ifdef HAVE_GEOS
	md.set(types::Area::decodeString("GRIB(Ni=441, Nj=181, latfirst=45000000, latlast=43000000, lonfirst=10000000, lonlast=12000000, type=0)"));
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
#endif
}

// Try matching with "bbox coveredby"
template<> template<>
void to::test<5>()
{
#ifdef HAVE_GEOS
	md.set(types::Area::decodeString("GRIB(Ni=441, Nj=181, latfirst=45000000, latlast=43000000, lonfirst=10000000, lonlast=12000000, type=0)"));
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

	md.set(types::Area::decodeString("GRIB(blo=0, lat=4502770, lon=966670, sta=101)"));
	ensure_matches("area:bbox coveredby POLYGON((9 45, 10 45, 10 46, 9 46, 9 45))", md); // Point contained
#endif
}


// Try matching Area with ODIMH5
template<> template<>
void to::test<6>()
{
	ValueBag testArea2;
	testArea2.set("foo", Value::createInteger(15));
	testArea2.set("bar", Value::createInteger(15000));
	testArea2.set("baz", Value::createInteger(-1200));
	testArea2.set("moo", Value::createInteger(0x1ffffff));
	testArea2.set("antani", Value::createInteger(0));
	testArea2.set("blinda", Value::createInteger(-1));
	testArea2.set("supercazzola", Value::createInteger(-7654321));

	//Matcher m;

	Metadata md;
	md.create();
	md.set(types::Area::decodeString("ODIMH5(foo=5,bar=5000,baz=-200,blinda=0,pippo=\"pippo\",pluto=\"12\",aaa=0,zzz=1)"));

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

	md.set(area::ODIMH5::create(testArea2));

	ensure_not_matches("area:ODIMH5:foo=5", md);
	ensure_matches("area:ODIMH5:foo=15", md);
}

// Try matching Area with ODIMH5
template<> template<>
void to::test<7>()
{
	//Vediamo se la formula per calcolare un ottagono con centro e raggio del radar funziona
	Metadata md1;
	md1.create();
	md1.set(types::Area::decodeString("ODIMH5(lon=11623600,lat=44456700,radius=100000)"));

	//il valori devono corrispondere
	ensure_matches("area:ODIMH5:lon=11623600", md1);
	ensure_matches("area:ODIMH5:lat=44456700", md1);
	ensure_matches("area:ODIMH5:radius=100000", md1);
}

// Try matching Area with ODIMH5
template<> template<>
void to::test<8>()
{
#ifdef HAVE_GEOS	
	//Vediamo se la formula per calcolare un ottagono con centro e raggio del radar funziona
	Metadata md1;
	md1.create();
	md1.set(types::Area::decodeString("ODIMH5(lon=11623600,lat=44456700,radius=100000)"));

	//il centro deve starci per forza
	ensure_matches("area: bbox covers POINT(11.6236 44.4567)", md1);   

	//i punti estremi del blobo non ci devono stare
	ensure_not_matches("area: bbox covers POINT(0     0)", md1);	   
	ensure_not_matches("area: bbox covers POINT(90    0)", md1);	   
	ensure_not_matches("area: bbox covers POINT(-90   0)", md1);	   
	ensure_not_matches("area: bbox covers POINT(0   180)", md1);	   
	ensure_not_matches("area: bbox covers POINT(0  -180)", md1);	   
#endif
}

// Try matching Area with ODIMH5
template<> template<>
void to::test<9>()
{
#ifdef HAVE_GEOS	
	//Vediamo se la formula per calcolare un ottagono con centro e raggio del radar funziona
	Metadata md1;
	md1.create();
	md1.set(types::Area::decodeString("ODIMH5(lon=11623600,lat=44456700,radius=100000)"));

	//aggiungere controlli altri punti appena dentro 
	//TODO xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
	//TODO xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
	//TODO xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

	//aggiungere controlli altri punti appena fuori
	//TODO xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
	//TODO xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

#endif

}
// Try matching Area with VM2
template<> template<>
void to::test<10>()
{
	Metadata md;
	md.create();
	md.set(area::VM2::create(1));

	ensure_matches("area:VM2", md);
	ensure_matches("area:VM2,", md);
	ensure_matches("area:VM2,1", md);
	ensure_not_matches("area:VM2,2", md);
	ensure_not_matches("area:GRIB:lon=0,lat=0", md);

	try {
		ensure_matches("area:VM2,ciccio=riccio", md);
		ensure(false);
	} catch (wibble::exception::Consistency& e) {
		ensure(string(e.what()).find("is not a number") != string::npos);
	}
    ensure_not_matches("area: VM2:ciccio=riccio", md);
    ensure_not_matches("area: VM2,1:ciccio=riccio", md);
    ensure_matches("area: VM2,1:lon=1207738", md);
}


}

// vim:set ts=4 sw=4:
