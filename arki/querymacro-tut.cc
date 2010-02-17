/*
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/targetfile.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/area.h>
#include <arki/types/ensemble.h>
#include <arki/types/run.h>
#include <arki/types/reftime.h>

#include <sstream>
#include <iostream>
#include <memory>

namespace tut {
using namespace std;
using namespace arki;

struct arki_targetfile_shar {
	Targetfile tf;
	Metadata md;

	arki_targetfile_shar()
		: tf(
			"targetfile['echo'] = function(format)\n"
			"  return function(md)\n"
			"    return format\n"
			"  end\n"
			"end\n"
		)
	{
		tf.loadRCFiles();
		using namespace arki::types;
		ValueBag testValues;
		testValues.set("antani", Value::createInteger(-1));
		testValues.set("blinda", Value::createInteger(0));

		md.set(origin::GRIB1::create(1, 2, 3));
		md.set(product::GRIB1::create(1, 2, 3));
		md.set(level::GRIB1::create(110, 12, 13));
		md.set(timerange::GRIB1::create(0, 0, 0, 0));
		md.set(area::GRIB::create(testValues));
		md.set(ensemble::GRIB::create(testValues));
		md.notes.push_back(types::Note::create("test note"));
		md.set(run::Minute::create(12));
		md.set(reftime::Position::create(types::Time::create(2007, 1, 2, 3, 4, 5)));
	}
};
TESTGRP(arki_targetfile);

// Empty or unsupported area should give 0
template<> template<>
void to::test<1>()
{
	Targetfile::Func f = tf.get("echo:foo");
	ensure_equals(f(md), "foo");
}

// Test MARS expansion
template<> template<>
void to::test<2>()
{
	Targetfile::Func f = tf.get("mars:foo[DATE][TIME]+[STEP].grib");
	ensure_equals(f(md), "foo200701020304+00.grib");
}

}

// vim:set ts=4 sw=4:
