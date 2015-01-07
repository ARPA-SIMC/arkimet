/*
 * Copyright (C) 2009--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "tests.h"
#include <arki/matcher.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/proddef.h>
#include <arki/types/assigneddataset.h>
#include <arki/types/run.h>
#include <arki/types/task.h>
#include <arki/types/quantity.h>

using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

namespace arki {
namespace tests {

void fill(Metadata& md)
{
	ValueBag testValues;
	testValues.set("aaa", Value::createInteger(0));
	testValues.set("foo", Value::createInteger(5));
	testValues.set("bar", Value::createInteger(5000));
	testValues.set("baz", Value::createInteger(-200));
	testValues.set("moo", Value::createInteger(0x5ffffff));
	testValues.set("antani", Value::createInteger(-1));
	testValues.set("blinda", Value::createInteger(0));
	testValues.set("supercazzola", Value::createInteger(-1234567));
	testValues.set("pippo", Value::createString("pippo"));
	testValues.set("pluto", Value::createString("12"));
	testValues.set("zzz", Value::createInteger(1));

    md.set(Origin::createGRIB1(1, 2, 3));
    md.set(Product::createGRIB1(1, 2, 3));
    md.set(Level::createGRIB1(110, 12, 13));
    md.set(Timerange::createGRIB1(2, 254u, 22, 23));
    md.set(Area::createGRIB(testValues));
    md.set(Proddef::createGRIB(testValues));
    md.add_note("test note");
    md.set(AssignedDataset::create("dsname", "dsid"));
    md.set(Run::createMinute(12));
    md.set(Reftime::createPosition(Time(2007, 1, 2, 3, 4, 5)));

	/* metadati specifici di odimh5 */
	md.set(Task::create("task1"));
	md.set(Quantity::create("a,b,c"));
}

void impl_ensure_matches(const wibble::tests::Location& loc, const std::string& expr, const Metadata& md, bool shouldMatch)
{
	Matcher m = Matcher::parse(expr);

	// Check that it should match as expected
	if (shouldMatch)
		inner_ensure(m(md));
	else
		inner_ensure(not m(md));

	// Check stringification and reparsing
	Matcher m1 = Matcher::parse(m.toString());

	//fprintf(stderr, "%s -> %s -> %s\n", expr.c_str(), m.toString().c_str(), m1.toString().c_str());

	inner_ensure_equals(m1.toString(), m.toString());
	inner_ensure_equals(m1(md), m(md));

    // Retry with an expanded stringification
    Matcher m2 = Matcher::parse(m.toStringExpanded());
    inner_ensure_equals(m2.toStringExpanded(), m.toStringExpanded());
    inner_ensure_equals(m2(md), m(md));
}

}
}
// vim:set ts=4 sw=4:
