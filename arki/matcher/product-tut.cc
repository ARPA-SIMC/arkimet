#include "config.h"
#include "arki/matcher/tests.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/types/product.h"
#include "arki/matcher/product.h"
#include "arki/wibble/exception.h"
#include <sstream>
#include <iostream>
#include <memory>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;

struct arki_matcher_product_shar
{
    Metadata md;

    arki_matcher_product_shar()
    {
        arki::tests::fill(md);
    }
};
TESTGRP(arki_matcher_product);

// Try matching GRIB product
def_test(1)
{
	Matcher m;

	ensure_matches("product:GRIB1", md);
	ensure_matches("product:GRIB1,,,", md);
	ensure_matches("product:GRIB1,1,,", md);
	ensure_matches("product:GRIB1,,2,", md);
	ensure_matches("product:GRIB1,,,3", md);
	ensure_matches("product:GRIB1,1,2,", md);
	ensure_matches("product:GRIB1,1,,3", md);
	ensure_matches("product:GRIB1,,2,3", md);
	ensure_matches("product:GRIB1,1,2,3", md);
	ensure_not_matches("product:GRIB1,2", md);
	ensure_not_matches("product:GRIB1,,3", md);
	ensure_not_matches("product:GRIB1,,,1", md);
	ensure_not_matches("product:BUFR,1,2,3", md);

	// If we have more than one product, we match any of them
	md.set(product::GRIB1::create(2, 3, 4));
	ensure_matches("product:GRIB1,2", md);
}

// Try matching BUFR product
def_test(2)
{
	ValueBag vb;
	vb.set("name", Value::createString("antani"));
	Matcher m;
	md.set(product::BUFR::create(1, 2, 3, vb));

	ensure_matches("product:BUFR", md);
	ensure_matches("product:BUFR,1", md);
	ensure_matches("product:BUFR,1,2", md);
	ensure_matches("product:BUFR,1,2,3", md);
	ensure_matches("product:BUFR,1,2,3:name=antani", md);
	ensure_matches("product:BUFR,1:name=antani", md);
	ensure_matches("product:BUFR:name=antani", md);
	ensure_not_matches("product:BUFR,1,2,3:name=blinda", md);
	ensure_not_matches("product:BUFR,1,2,3:name=antan", md);
	ensure_not_matches("product:BUFR,1,2,3:name=antani1", md);
	ensure_not_matches("product:BUFR,1,2,3:enam=antani", md);
	ensure_not_matches("product:GRIB1,1,2,3", md);
    try {
        ensure_matches("product:BUFR,name=antani", md);
        ensure(false);
    } catch (std::exception& e) {
        ensure(string(e.what()).find("is not a number") != string::npos);
    }
    try {
        ensure_matches("product:BUFR:0,,2", md);
        ensure(false);
    } catch (std::exception& e) {
        ensure(string(e.what()).find("key=value") != string::npos);
    }
}
// Try matching VM2 product
def_test(3)
{
	md.set(product::VM2::create(1));

	ensure_matches("product:VM2", md);
	ensure_matches("product:VM2,", md);
	ensure_matches("product:VM2,1", md);
	ensure_not_matches("product:GRIB1,1,2,3", md);
    ensure_not_matches("product:VM2,2", md);
    try {
        ensure_matches("product:VM2,ciccio=riccio", md);
        ensure(false);
    } catch (std::exception& e) {
        ensure(string(e.what()).find("is not a number") != string::npos);
    }
    ensure_not_matches("product: VM2:ciccio=riccio", md);
    ensure_not_matches("product: VM2,1:ciccio=riccio", md);
    ensure_matches("product: VM2,1:bcode=B20013", md);
}

}

// vim:set ts=4 sw=4:
