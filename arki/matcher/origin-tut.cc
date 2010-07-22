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
#include <arki/types/origin.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_matcher_origin_shar {
	Metadata md;

	arki_matcher_origin_shar()
	{
		md.create();
		arki::tests::fill(md);
	}
};
TESTGRP(arki_matcher_origin);

// Try matching GRIB origin
template<> template<>
void to::test<1>()
{
	Matcher m;

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
}

// Try matching BUFR origin
template<> template<>
void to::test<2>()
{
	Matcher m;
	md.set(origin::BUFR::create(1, 2));

	ensure_matches("origin:BUFR", md);
	ensure_matches("origin:BUFR,,", md);
	ensure_matches("origin:BUFR,1,", md);
	ensure_matches("origin:BUFR,,2", md);
	ensure_matches("origin:BUFR,1,2", md);
	ensure_not_matches("origin:BUFR,2", md);
	ensure_not_matches("origin:BUFR,,3", md);
	ensure_not_matches("origin:BUFR,,1", md);
	ensure_not_matches("origin:BUFR,2,3", md);

	ensure(m(md));
}

}

// vim:set ts=4 sw=4:
