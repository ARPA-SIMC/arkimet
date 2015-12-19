#include "config.h"

#include <arki/matcher/tests.h>
#include <arki/matcher.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;

struct arki_matcher_origin_shar
{
    Metadata md;

    arki_matcher_origin_shar()
    {
        arki::tests::fill(md);
    }
};
TESTGRP(arki_matcher_origin);

// Try matching GRIB origin
def_test(1)
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
def_test(2)
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

// Try matching ODIMH5 origin
def_test(3)
{
	Matcher m;
	md.set(origin::ODIMH5::create("wmo", "rad", "plc"));

	ensure_matches("origin:ODIMH5", md);
	ensure_matches("origin:ODIMH5,,,", md);
	ensure_matches("origin:ODIMH5,wmo,,", md);
	ensure_matches("origin:ODIMH5,,rad,", md);
	ensure_matches("origin:ODIMH5,,,plc", md);
	ensure_matches("origin:ODIMH5,wmo,rad,", md);
	ensure_matches("origin:ODIMH5,wmo,,plc", md);
	ensure_matches("origin:ODIMH5,wmo,rad,plc", md);
}

// Try matching ODIMH5 origin
def_test(4)
{
	Matcher m;
	md.set(origin::ODIMH5::create("wmo", "rad", "plc"));

	ensure_not_matches("origin:ODIMH5,x,,", md);
	ensure_not_matches("origin:ODIMH5,,x,", md);
	ensure_not_matches("origin:ODIMH5,,,x", md);
	ensure_not_matches("origin:ODIMH5,wmo,x,", md);
	ensure_not_matches("origin:ODIMH5,wmo,,x", md);
	ensure_not_matches("origin:ODIMH5,wmo,x,x", md);
}

}
