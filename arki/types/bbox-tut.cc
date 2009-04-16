/*
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types/test-utils.h>
#include <arki/types/bbox.h>

#include <sstream>
#include <iostream>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_types_bbox_shar {
};
TESTGRP(arki_types_bbox);

// Check INVALID
template<> template<>
void to::test<1>()
{
	Item<BBox> o = bbox::INVALID::create();
	ensure_equals(o->style(), BBox::INVALID);
	const bbox::INVALID* v = o->upcast<bbox::INVALID>();
	ensure(v);

	ensure_equals(o, Item<BBox>(bbox::INVALID::create()));

	ensure(o != bbox::POINT::create(2, 3));
	ensure(o != bbox::BOX::create(2, 3, 4, 5));
	ensure(o != bbox::HULL::create(vector< pair<float, float> >()));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_BBOX);
}

// Check POINT
template<> template<>
void to::test<2>()
{
	Item<BBox> o = bbox::POINT::create(1.1, 2.1);
	ensure_equals(o->style(), BBox::POINT);
	const bbox::POINT* v = o->upcast<bbox::POINT>();
	ensure_equals(v->lat, 1.1f);
	ensure_equals(v->lon, 2.1f);

	ensure_equals(o, Item<BBox>(bbox::POINT::create(1.1, 2.1)));

	ensure(o != bbox::INVALID::create());
	ensure(o != bbox::POINT::create(1.1, 2.2));
	ensure(o != bbox::BOX::create(1.1, 2.1, 3.1, 4.2));
	vector< pair<float, float> > points;
	points.push_back(make_pair(1.1, 2.1));
	ensure(o != bbox::HULL::create(points));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_BBOX);
}

// Check BOX
template<> template<>
void to::test<3>()
{
	Item<BBox> o = bbox::BOX::create(1.1, 2.1, 3.1, 4.1);
	ensure_equals(o->style(), BBox::BOX);
	const bbox::BOX* v = o->upcast<bbox::BOX>();
	ensure_equals(v->latmin, 1.1f);
	ensure_equals(v->latmax, 2.1f);
	ensure_equals(v->lonmin, 3.1f);
	ensure_equals(v->lonmax, 4.1f);

	ensure_equals(o, Item<BBox>(bbox::BOX::create(1.1, 2.1, 3.1, 4.1)));

	ensure(o != bbox::INVALID::create());
	ensure(o != bbox::POINT::create(1.1, 2.1));
	ensure(o != bbox::BOX::create(1.1, 2.1, 3.1, 4.2));
	vector< pair<float, float> > points;
	points.push_back(make_pair(1.1f, 3.1f));
	points.push_back(make_pair(2.1f, 3.1f));
	points.push_back(make_pair(2.1f, 4.1f));
	points.push_back(make_pair(1.1f, 4.1f));
	points.push_back(make_pair(1.1f, 3.1f));
	ensure(o != bbox::HULL::create(points));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_BBOX);
}

// Check HULL
template<> template<>
void to::test<4>()
{
	vector< pair<float, float> > points;
	points.push_back(make_pair(1.1f, 3.1f));
	points.push_back(make_pair(2.1f, 3.1f));
	points.push_back(make_pair(2.1f, 4.1f));
	points.push_back(make_pair(1.1f, 4.1f));
	points.push_back(make_pair(1.1f, 3.1f));
	Item<BBox> o = bbox::HULL::create(points);
	ensure_equals(o->style(), BBox::HULL);
	const bbox::HULL* v = o->upcast<bbox::HULL>();
	ensure_equals(v->points.size(), 5u);
	ensure_equals(v->points[0].first, 1.1f);
	ensure_equals(v->points[2].first, 2.1f);
	ensure_equals(v->points[2].second, 4.1f);
	ensure_equals(v->points[4].second, 3.1f);

	ensure_equals(o, Item<BBox>(bbox::HULL::create(points)));

	ensure(o != bbox::INVALID::create());
	ensure(o != bbox::POINT::create(1.1f, 2.1f));
	ensure(o != bbox::BOX::create(1.1f, 2.1f, 3.1f, 4.2f));
	vector< pair<float, float> > points1;
	points1.push_back(make_pair(2.1f, 3.1f));
	points1.push_back(make_pair(2.1f, 4.1f));
	points1.push_back(make_pair(1.1f, 4.1f));
	points1.push_back(make_pair(1.1f, 3.1f));
	points1.push_back(make_pair(2.1f, 3.1f));
	ensure(o != bbox::HULL::create(points1));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_BBOX);
}

#ifdef HAVE_LUA
// Test Lua functions
template<> template<>
void to::test<5>()
{
	Item<BBox> o = bbox::POINT::create(1.1, 2.1);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style() ~= 'POINT' then return 'style is '..o.style()..' instead of POINT' end \n"
		"  a, b = o.point() \n"
		"  if math.abs(a - 1.1) > 0.001 then return 'o.point() first item is '..a..' instead of 1.1' end \n"
		"  if math.abs(b - 2.1) > 0.001 then return 'o.point() second item is '..b..' instead of 2.1' end \n"
		"  if o.invalid() ~= nil then return 'o.invalid() gave '..o.invalid()..' instead of nil' end \n"
		"  if o.box() ~= nil then return 'o.box() gave '..o.box()..' instead of nil' end \n"
		"  if o.hull() ~= nil then return 'o.hull() gave '..o.hull()..' instead of nil' end \n"
		"  if tostring(o) ~= 'POINT(001.1000, 002.1000)' then return 'tostring gave '..tostring(o)..' instead of POINT(001.1000, 002.1000)' end \n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
}
#endif

}

// vim:set ts=4 sw=4:
