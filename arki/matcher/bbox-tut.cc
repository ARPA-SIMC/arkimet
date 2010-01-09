/*
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/matcher/test-utils.h>
#include <arki/matcher.h>
#include <arki/metadata.h>
#include <arki/types/bbox.h>
#include <arki/configfile.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;

struct arki_matcher_bbox_shar {
	Metadata md;

	arki_matcher_bbox_shar()
	{
		md.create();
		arki::tests::fill(md);
	}
};
TESTGRP(arki_matcher_bbox);

// Try matching with "is"
template<> template<>
void to::test<1>()
{
	md.set(bbox::INVALID::create());

	ensure_matches("bbox:is INVALID()", md);
	ensure_matches("bbox:IS INVALID()", md);
	ensure_not_matches("bbox:is POINT(44, 11)", md);
	ensure_not_matches("bbox:is BOX(43, 45, 10, 12)", md);
	ensure_not_matches("bbox:is HULL(43 12, 44 10, 45 12)", md);

	md.set(bbox::POINT::create(44, 11));
	ensure_matches("bbox:is POINT(44, 11)", md);
	ensure_not_matches("bbox:is INVALID()", md);
	ensure_not_matches("bbox:is BOX(43, 45, 10, 12)", md);
	ensure_not_matches("bbox:is HULL(43 12, 44 10, 45 12)", md);

	md.set(bbox::BOX::create(43, 45, 10, 12));
	ensure_matches("bbox:is BOX(43, 45, 10, 12)", md);
	ensure_not_matches("bbox:is INVALID()", md);
	ensure_not_matches("bbox:is POINT(44, 11)", md);
	ensure_not_matches("bbox:is HULL(43 12, 44 10, 45 12)", md);

	std::vector< std::pair<float, float> > points;
	points.push_back(make_pair(43.0, 12.0));
	points.push_back(make_pair(44.0, 10.0));
	points.push_back(make_pair(45.0, 12.0));
	md.set(bbox::HULL::create(points));
	ensure_matches("bbox:is HULL(43 12, 44 10, 45 12)", md);
	ensure_not_matches("bbox:is INVALID()", md);
	ensure_not_matches("bbox:is POINT(44, 11)", md);
	ensure_not_matches("bbox:is BOX(43, 45, 10, 12)", md);
}

}

// vim:set ts=4 sw=4:
