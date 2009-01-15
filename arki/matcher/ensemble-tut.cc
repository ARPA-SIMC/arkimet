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
#include <arki/types/ensemble.h>
#include <arki/configfile.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_matcher_ensemble_shar {
	Metadata md;

	arki_matcher_ensemble_shar()
	{
		md.create();
		arki::tests::fill(md);
	}
};
TESTGRP(arki_matcher_ensemble);

// Try matching Ensemble
template<> template<>
void to::test<1>()
{
	ValueBag testEnsemble2;
	testEnsemble2.set("foo", Value::createInteger(15));
	testEnsemble2.set("bar", Value::createInteger(15000));
	testEnsemble2.set("baz", Value::createInteger(-1200));
	testEnsemble2.set("moo", Value::createInteger(0x1ffffff));
	testEnsemble2.set("antani", Value::createInteger(0));
	testEnsemble2.set("blinda", Value::createInteger(-1));
	testEnsemble2.set("supercazzola", Value::createInteger(-7654321));

	Matcher m;

	ensure_matches("ensemble:GRIB:foo=5", md);
	ensure_matches("ensemble:GRIB:bar=5000", md);
	ensure_matches("ensemble:GRIB:foo=5,bar=5000", md);
	ensure_matches("ensemble:GRIB:baz=-200", md);
	ensure_matches("ensemble:GRIB:blinda=0", md);
	ensure_matches("ensemble:GRIB:pippo=pippo", md);
	ensure_matches("ensemble:GRIB:pippo=\"pippo\"", md);
	ensure_matches("ensemble:GRIB:pluto=\"12\"", md);
	ensure_not_matches("ensemble:GRIB:pluto=12", md);
	// Check that we match the first item
	ensure_matches("ensemble:GRIB:aaa=0", md);
	// Check that we match the last item
	ensure_matches("ensemble:GRIB:zzz=1", md);
	ensure_not_matches("ensemble:GRIB:foo=50", md);
	ensure_not_matches("ensemble:GRIB:foo=-5", md);
	ensure_not_matches("ensemble:GRIB:foo=5,miao=0", md);
	// Check matching a missing item at the beginning of the list
	ensure_not_matches("ensemble:GRIB:a=1", md);
	// Check matching a missing item at the end of the list
	ensure_not_matches("ensemble:GRIB:zzzz=1", md);

	md.set(ensemble::GRIB::create(testEnsemble2));

	ensure_not_matches("ensemble:GRIB:foo=5", md);
	ensure_matches("ensemble:GRIB:foo=15", md);
}

}

// vim:set ts=4 sw=4:
