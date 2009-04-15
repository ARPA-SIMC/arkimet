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
#include <arki/summary.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/source.h>
#include <arki/types/run.h>
#include <arki/matcher.h>
#include <arki/utils.h>

#include <sstream>
#include <iostream>

#include <config.h>

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

#ifdef HAVE_GRIBAPI
#include <arki/scan/grib.h>
#endif

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_summary_shar {
	Metadata md1;
	Metadata md2;
	Summary s;

	arki_summary_shar()
	{
		md1.create();
		md1.set(origin::GRIB1::create(1, 2, 3));
		md1.set(product::GRIB1::create(1, 2, 3));
		md1.set(timerange::GRIB1::create(1, timerange::GRIB1::SECOND, 0, 0));
		md1.set(reftime::Position::create(types::Time::create(2007, 1, 2, 3, 4, 5)));
		md1.source = source::Inline::create("grib1", 10);

		md2.create();
		md2.set(origin::GRIB1::create(3, 4, 5));
		md2.set(product::GRIB1::create(2, 3, 4));
		md2.set(timerange::GRIB1::create(1, timerange::GRIB1::SECOND, 0, 0));
		md2.set(reftime::Position::create(types::Time::create(2006, 5, 4, 3, 2, 1)));
		md2.source = source::Inline::create("grib1", 20);

		s.add(md1);
		s.add(md2);
	}
};
TESTGRP(arki_summary);

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
	Summary s1 = s;
	ensure_equals(s1, s);
	ensure_equals(s1.count(), 2u);
	ensure_equals(s1.size(), 30u);
}

// Test matching
template<> template<>
void to::test<3>()
{
	Summary s1;
	ensure(Matcher::parse("origin:GRIB1,1")(s));
	s1 = s.filter(Matcher::parse("origin:GRIB1,1")); ensure_equals(s1.count(), 1u);
	ensure(!Matcher::parse("origin:GRIB1,2")(s));
	s1 = s.filter(Matcher::parse("origin:GRIB1,2")); ensure_equals(s1.count(), 0u);
}

// Test matching runs
template<> template<>
void to::test<4>()
{
	md1.set(run::Minute::create(0, 0));
	md2.set(run::Minute::create(12, 0));
	s.clear();
	s.add(md1);
	s.add(md2);
	Summary s1;

	ensure(Matcher::parse("run:MINUTE,0")(s));
	s1 = s.filter(Matcher::parse("run:MINUTE,0")); ensure_equals(s1.count(), 1u);
	ensure(Matcher::parse("run:MINUTE,12")(s));
	s1 = s.filter(Matcher::parse("run:MINUTE,12")); ensure_equals(s1.count(), 1u);
}

// Test filtering
template<> template<>
void to::test<5>()
{
	Summary s1 = s.filter(Matcher::parse("origin:GRIB1,1"));
	ensure_equals(s1.count(), 1u);
	ensure_equals(s1.size(), 10u);

	Summary s2;
	s.filter(Matcher::parse("origin:GRIB1,1"), s2);
	ensure_equals(s2.count(), 1u);
	ensure_equals(s2.size(), 10u);

	ensure_equals(s1, s2);
}

// Test serialisation to binary
template<> template<>
void to::test<6>()
{
	string encoded = s.encode();
	stringstream stream(encoded, ios_base::in);
	Summary s1;
	ensure(s1.read(stream, "(test memory buffer)"));
	ensure_equals(s1, s);
}

// Test serialisation to Yaml
template<> template<>
void to::test<7>()
{
	stringstream stream1;
	s.writeYaml(stream1);
	stringstream stream2(stream1.str(), ios_base::in);
	Summary s2;
	s2.readYaml(stream2, "(test memory buffer)");
	ensure_equals(s2, s);
}

// Test merging summaries
template<> template<>
void to::test<8>()
{
	Summary s1;
	s1.add(s);
	ensure_equals(s1, s);
}

// Test serialisation of empty summary
template<> template<>
void to::test<9>()
{
	s = Summary();
	string encoded = s.encode();
	stringstream stream(encoded, ios_base::in);
	Summary s1;
	ensure(s1.read(stream, "(test memory buffer)"));
	ensure_equals(s1, s);
}

// Test a case of metadata wrongly considered the same
template<> template<>
void to::test<10>()
{
	Metadata tmd1;
	Metadata tmd2;
	Summary ts;

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

#warning this check is disabled
#if 0
	size_t count = 0;
	for (Summary::const_iterator i = ts.begin(); i != ts.end(); ++i, ++count)
		;
	ensure_equals(count, 2u);
#endif
}

// Test Lua functions
template<> template<>
void to::test<11>()
{
#ifdef HAVE_LUA
	s.add(md2);

	tests::Lua test(
		"function test(s) \n"
		"  if s.count() ~= 3 then return 'count is '..s.count()..' instead of 3' end \n"
		"  if s.size() ~= 50 then return 'size is '..s.size()..' instead of 50' end \n"
		"  i = 0 \n"
		"  items = {} \n"
		"  for idx, entry in ipairs(s.data()) do \n"
		"    item, stats = unpack(entry) \n"
		"    for name, val in pairs(item) do \n"
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
#endif
}

// Summarise the test gribs
template<> template<>
void to::test<12>()
{
#ifdef HAVE_GRIBAPI
	Summary s1;
	Metadata md;

	scan::Grib scanner;
	scanner.open("inbound/test.grib1");
	ensure(scanner.next(md));
	s1.add(md);
	ensure(scanner.next(md));
	//s1.add(md);
	ensure(scanner.next(md));
	//s1.add(md);
	ensure(!scanner.next(md));

	// Serialisation to binary
	string encoded = s1.encode();
	stringstream stream(encoded, ios_base::in);
	Summary s2;
	ensure(s2.read(stream, "(test memory buffer)"));
	ensure_equals(s1, s2);

	// Serialisation to Yaml
	stringstream stream1;
	s1.writeYaml(stream1);
	stringstream stream2(stream1.str(), ios_base::in);
	Summary s3;
	s3.readYaml(stream2, "(test memory buffer)");
	ensure_equals(s3, s1);
#endif
}

// Test adding a metadata plus stats
template<> template<>
void to::test<13>()
{
	Metadata md3;
	md3.create();
	md3.set(origin::GRIB1::create(5, 6, 7));
	md3.set(product::GRIB1::create(4, 5, 6));
	md3.set(timerange::GRIB1::create(1, timerange::GRIB1::SECOND, 0, 0));
	md3.set(reftime::Position::create(types::Time::create(2006, 5, 4, 3, 2, 1)));

	arki::Item<summary::Stats> st(new summary::Stats);
	st->count = 5;
	st->size = 123456;
	st->reftimeMerger.mergeTime(Time::create(2008, 7, 6, 5, 4, 3), Time::create(2008, 9, 8, 7, 6, 5));

	s.add(md3, st);

	// Check that it contains 2 metadata
	ensure_equals(s.count(), 7u);
	ensure_equals(s.size(), 123486u);
}

}

// vim:set ts=4 sw=4:
