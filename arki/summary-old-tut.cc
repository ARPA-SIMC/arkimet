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

#include <arki/tests/test-utils.h>
#include <arki/summary-old.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/reftime.h>
#include <arki/types/source.h>
#include <arki/types/run.h>
#include <arki/matcher.h>

#include <sstream>
#include <iostream>

#include <config.h>

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_oldsummary_shar {
	Metadata md1;
	Metadata md2;
	OldSummary s;

	arki_oldsummary_shar()
	{
		md1.create();
		md1.set(origin::GRIB1::create(1, 2, 3));
		md1.set(product::GRIB1::create(1, 2, 3));
		md1.set(reftime::Position::create(types::Time::create(2007, 1, 2, 3, 4, 5)));
		md1.source = source::Inline::create("grib1", 10);

		md2.create();
		md2.set(origin::GRIB1::create(3, 4, 5));
		md2.set(product::GRIB1::create(2, 3, 4));
		md2.set(reftime::Position::create(types::Time::create(2006, 5, 4, 3, 2, 1)));
		md2.source = source::Inline::create("grib1", 20);

		s.add(md1);
		s.add(md2);
	}
};
TESTGRP(arki_oldsummary);

// Test that it contains the right things
template<> template<>
void to::test<1>()
{
	// Check that it contains 2 metadata
	ensure_equals(s.count(), 2u);
	ensure_equals(s.size(), 30u);
}

// Test assignment and comparison
template<> template<>
void to::test<2>()
{
	OldSummary s1 = s;
	ensure_equals(s1, s);
	ensure_equals(s1.count(), 2u);
	ensure_equals(s1.size(), 30u);
}

// Test matching
template<> template<>
void to::test<3>()
{
#if 0
	ensure(Matcher::parse("origin:GRIB1,1")(s));
	ensure(!Matcher::parse("origin:GRIB1,2")(s));
#endif
}

// Test filtering
template<> template<>
void to::test<4>()
{
#if 0
	OldSummary s1 = s.filter(Matcher::parse("origin:GRIB1,1"));
	ensure_equals(s1.count(), 1u);
	ensure_equals(s1.size(), 10u);

	OldSummary s2;
	s.filter(Matcher::parse("origin:GRIB1,1"), s2);
	ensure_equals(s2.count(), 1u);
	ensure_equals(s2.size(), 10u);

	ensure_equals(s1, s2);
#endif
}

// Test serialisation to binary
template<> template<>
void to::test<5>()
{
	string encoded = s.encode();
	stringstream stream(encoded, ios_base::in);
	OldSummary s1;
	ensure(s1.read(stream, "(test memory buffer)"));
	ensure_equals(s1, s);
}

// Test serialisation to Yaml
template<> template<>
void to::test<6>()
{
	stringstream stream1;
	s.writeYaml(stream1);
	stringstream stream2(stream1.str(), ios_base::in);
	OldSummary s2;
	s2.readYaml(stream2, "(test memory buffer)");
	ensure_equals(s2, s);
}

// Test merging summaries
template<> template<>
void to::test<7>()
{
	OldSummary s1;
	s1.add(s);
	ensure_equals(s1, s);
}

// Test serialisation of empty summary
template<> template<>
void to::test<8>()
{
	s = OldSummary();
	string encoded = s.encode();
	stringstream stream(encoded, ios_base::in);
	OldSummary s1;
	ensure(s1.read(stream, "(test memory buffer)"));
	ensure_equals(s1, s);
}

// Test a case of metadata wrongly considered the same
template<> template<>
void to::test<9>()
{
	Metadata tmd1;
	Metadata tmd2;
	OldSummary ts;

	tmd1.create();
	tmd1.set(origin::GRIB1::create(1, 2, 3));
	tmd1.set(product::GRIB1::create(1, 2, 3));
	tmd1.set(reftime::Position::create(types::Time::create(2007, 1, 2, 3, 4, 5)));
	tmd1.source = source::Inline::create("grib1", 10);

	tmd2.create();
	tmd2.set(origin::GRIB1::create(1, 2, 3));
	tmd2.set(product::GRIB1::create(1, 2, 3));
	tmd2.set(reftime::Position::create(types::Time::create(2007, 1, 2, 3, 4, 5)));
	tmd2.set(run::Minute::create(12, 0));
	tmd2.source = source::Inline::create("grib1", 15);

	ts.add(tmd1);
	ts.add(tmd2);

	ensure_equals(ts.count(), 2u);
	ensure_equals(ts.size(), 25u);

	size_t count = 0;
	for (OldSummary::const_iterator i = ts.begin(); i != ts.end(); ++i, ++count)
		;

	ensure_equals(count, 2u);
}

#ifdef HAVE_LUA
// Test Lua functions
template<> template<>
void to::test<10>()
{
	s.add(md2);

	tests::Lua test(
		"function test(s) \n"
		"  if s.count() ~= 3 then return 'count is '..s.count()..' instead of 3' end \n"
		"  if s.size() ~= 50 then return 'size is '..s.size()..' instead of 50' end \n"
		"  i = 0 \n"
		"  items = {} \n"
		"  for item, stats in s.iter() do \n"
		"    for name, val in item.iter() do \n"
		"      o = name..':'..tostring(val) \n"
		"      count = items[o] or 0 \n"
		"      items[o] = count + stats.count() \n"
		"    end \n"
		"    i = i + 1 \n"
		"  end \n"
		"  if i ~= 2 then return 'iterated '..i..' times instead of 2' end \n"
		"  c = items['origin:GRIB1(001, 002, 003)'] \n"
		"  if c ~= 1 then return 'origin1 c is '..tostring(c)..' instead of 1' end \n"
		"  c = items['origin:GRIB1(003, 004, 005)'] \n"
		"  if c ~= 2 then return 'origin2 c is '..c..' instead of 2' end \n"
		"  c = items['product:GRIB1(001, 002, 003)'] \n"
		"  if c ~= 1 then return 'product1 c is '..c..' instead of 1' end \n"
		"  c = items['product:GRIB1(002, 003, 004)'] \n"
		"  if c ~= 2 then return 'product2 c is '..c..' instead of 2' end \n"
		"  return nil\n"
		"end \n"
	);

	test.pusharg(s);
	ensure_equals(test.run(), "");
}
#endif

}

// vim:set ts=4 sw=4:
