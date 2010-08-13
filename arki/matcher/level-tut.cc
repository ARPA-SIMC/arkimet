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
#include <arki/types/level.h>
#include <arki/configfile.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_matcher_level_shar {
	Metadata md;

	arki_matcher_level_shar()
	{
		md.create();
		arki::tests::fill(md);
	}
};
TESTGRP(arki_matcher_level);

// Try matching GRIB1 level
template<> template<>
void to::test<1>()
{
	Matcher m;

	ensure_matches("level:GRIB1", md);
	ensure_matches("level:GRIB1,,,", md);
	ensure_matches("level:GRIB1,110,,", md);
	ensure_matches("level:GRIB1,,12,", md);
	ensure_matches("level:GRIB1,,,13", md);
	ensure_matches("level:GRIB1,110,12,", md);
	ensure_matches("level:GRIB1,110,,13", md);
	ensure_matches("level:GRIB1,,12,13", md);
	ensure_matches("level:GRIB1,110,12,13", md);
	ensure_not_matches("level:GRIB1,12", md);
	ensure_not_matches("level:GRIB1,,13", md);
	ensure_not_matches("level:GRIB1,,,11", md);
	//m = Matcher::parse("level:BUFR,11,12,13");
	//ensure(not m(md));
}

// Try matching GRIB2S level
template<> template<>
void to::test<2>()
{
	Matcher m;
	md.set(level::GRIB2S::create(110, 12, 13));

	ensure_matches("level:GRIB2S", md);
	ensure_matches("level:GRIB2S,,,", md);
	ensure_matches("level:GRIB2S,110,,", md);
	ensure_matches("level:GRIB2S,,12,", md);
	ensure_matches("level:GRIB2S,,,13", md);
	ensure_matches("level:GRIB2S,110,12,", md);
	ensure_matches("level:GRIB2S,110,,13", md);
	ensure_matches("level:GRIB2S,,12,13", md);
	ensure_matches("level:GRIB2S,110,12,13", md);
	ensure_not_matches("level:GRIB2S,12", md);
	ensure_not_matches("level:GRIB2S,,13", md);
	ensure_not_matches("level:GRIB2S,,,11", md);
	//m = Matcher::parse("level:BUFR,11,12,13");
	//ensure(not m(md));
}

// Try matching GRIB2D level
template<> template<>
void to::test<3>()
{
	Matcher m;
	md.set(level::GRIB2D::create(110, 12, 13, 114, 15, 16));

	ensure_matches("level:GRIB2D", md);
	ensure_matches("level:GRIB2D,,,", md);
	ensure_matches("level:GRIB2D,110,,", md);
	ensure_matches("level:GRIB2D,,12,", md);
	ensure_matches("level:GRIB2D,,,13", md);
	ensure_matches("level:GRIB2D,110,12,", md);
	ensure_matches("level:GRIB2D,110,,13", md);
	ensure_matches("level:GRIB2D,,12,13", md);
	ensure_matches("level:GRIB2D,110,12,13", md);
	ensure_matches("level:GRIB2D,110,12,13,114", md);
	ensure_matches("level:GRIB2D,110,12,13,114,15", md);
	ensure_matches("level:GRIB2D,110,12,13,114,15,16", md);
	ensure_matches("level:GRIB2D,,,,114,15,16", md);
	ensure_matches("level:GRIB2D,,,,114", md);
	ensure_not_matches("level:GRIB2D,12", md);
	ensure_not_matches("level:GRIB2D,,13", md);
	ensure_not_matches("level:GRIB2D,,,11", md);
	ensure_not_matches("level:GRIB2D,,,,115", md);
	//m = Matcher::parse("level:BUFR,11,12,13");
	//ensure(not m(md));
}

// Try matching ODIMH5 level
template<> template<>
void to::test<4>()
{
	Matcher m;
	md.set(level::ODIMH5::create(10.123, 20.123));

	ensure_matches("level:ODIMH5", md);
	ensure_matches("level:ODIMH5,", md);

	ensure_matches("level:ODIMH5,11 12 13 14 15", md);
	ensure_matches("level:ODIMH5,11 12 13 14 15 offset 0.5", md);
	ensure_matches("level:ODIMH5,10 offset 0.5", md);
	ensure_matches("level:ODIMH5,20.5 offset 0.5", md);

	ensure_matches("level:ODIMH5,range 0, 30", md);
	ensure_matches("level:ODIMH5,range 0, 15", md);
	ensure_matches("level:ODIMH5,range 15, 30", md);
	ensure_matches("level:ODIMH5,range 12, 17", md);

	ensure_not_matches("level:ODIMH5,30", md);
	ensure_not_matches("level:ODIMH5,30 offset 5", md);
	ensure_not_matches("level:ODIMH5,0 offset 5", md);
	ensure_not_matches("level:ODIMH5,range 0 5", md);
	ensure_not_matches("level:ODIMH5,range 21 30", md);

	ensure_not_matches("level:GRIB2D,12", md);
	ensure_not_matches("level:GRIB2D,,13", md);
	ensure_not_matches("level:GRIB2D,,,11", md);
	ensure_not_matches("level:GRIB2D,,,,115", md);

}

}

// vim:set ts=4 sw=4:
