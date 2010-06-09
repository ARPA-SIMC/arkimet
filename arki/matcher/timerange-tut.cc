/*
 * Copyright (C) 2007,2008  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/matcher/test-utils.h>
#include <arki/matcher.h>
#include <arki/metadata.h>
#include <arki/types/timerange.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_matcher_timerange_shar {
	Metadata md;

	arki_matcher_timerange_shar()
	{
		md.create();
		arki::tests::fill(md);
	}
};
TESTGRP(arki_matcher_timerange);

// Try matching GRIB1 timerange
template<> template<>
void to::test<1>()
{
	ensure_matches("timerange:GRIB1", md);
	ensure_matches("timerange:GRIB1,,", md);
	ensure_matches("timerange:GRIB1,2", md);
	ensure_matches("timerange:GRIB1,2,,", md);
	// Incomplete matches are not allowed
	ensure_not_matches("timerange:GRIB1,,22s,", md);
	ensure_not_matches("timerange:GRIB1,,,23s", md);
	// ensure_not_matches("timerange:GRIB1,2,22s,", md);
	ensure_not_matches("timerange:GRIB1,2,,23s", md);
	ensure_not_matches("timerange:GRIB1,,22s,23s", md);
	ensure_matches("timerange:GRIB1,2,22s,23s", md);
	ensure_not_matches("timerange:GRIB1,2,23s,23s", md);
	ensure_not_matches("timerange:GRIB1,2,22s,22s", md);
	// ensure_not_matches("timerange:GRIB1,2,22,23");

	// Match timerange in years, which gave problems
	md.set(timerange::GRIB1::create(2, 4, 2, 3));
	ensure_matches("timerange:GRIB1,2,2y,3y", md);
}

// Try matching GRIB2 timerange
template<> template<>
void to::test<2>()
{
	md.set(timerange::GRIB2::create(1, 2, 3, 4));

	ensure_matches("timerange:GRIB2", md);
	ensure_matches("timerange:GRIB2,,", md);
	ensure_matches("timerange:GRIB2,1", md);
	ensure_matches("timerange:GRIB2,1,,", md);
	ensure_matches("timerange:GRIB2,1,2,", md);
	ensure_matches("timerange:GRIB2,1,2,3", md);
	ensure_matches("timerange:GRIB2,1,2,3,4", md);
	// Incomplete matches are allowed
	ensure_matches("timerange:GRIB2,,2,", md);
	ensure_matches("timerange:GRIB2,,,3", md);
	ensure_matches("timerange:GRIB2,,,,4", md);
	ensure_not_matches("timerange:GRIB1", md);
	ensure_not_matches("timerange:GRIB2,2", md);
	ensure_not_matches("timerange:GRIB2,,3", md);
	ensure_not_matches("timerange:GRIB2,,,4", md);
	ensure_not_matches("timerange:GRIB2,,,,5", md);
}


// Try matching BUFR timerange
template<> template<>
void to::test<3>()
{
	md.set(timerange::BUFR::create(2, 1));

	ensure_matches("timerange:BUFR", md);
	ensure_matches("timerange:BUFR,2h", md);
	ensure_matches("timerange:BUFR,120m", md);
	ensure_matches("timerange:BUFR,7200s", md);
	ensure_not_matches("timerange:GRIB1", md);
	ensure_not_matches("timerange:GRIB2", md);
	ensure_not_matches("timerange:BUFR,3h", md);
	ensure_not_matches("timerange:BUFR,2m", md);
}


// Try some cases that have been buggy
template<> template<>
void to::test<4>()
{
	Metadata md;

	md.set(timerange::GRIB1::create(0, 254, 0, 0));
	ensure_matches("timerange:GRIB1,0,0h", md);
	ensure_matches("timerange:GRIB1,0,0h,0h", md);
	ensure_not_matches("timerange:GRIB1,0,12h", md);
	ensure_not_matches("timerange:GRIB1,0,12y", md);
	ensure_not_matches("timerange:GRIB1,0,,12y", md);
}

}

// vim:set ts=4 sw=4:
