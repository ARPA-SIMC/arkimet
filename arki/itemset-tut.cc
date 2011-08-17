/*
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/tests/test-utils.h>
#include <arki/itemset.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/proddef.h>
#include <arki/types/assigneddataset.h>

#include <sstream>
#include <iostream>
#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace std {
static inline std::ostream& operator<<(std::ostream& o, const arki::ItemSet& m)
{
        o << "(";
        for (arki::ItemSet::const_iterator i = m.begin(); i != m.end(); ++i)
                if (i == m.begin())
                        o << i->second;
                else
                        o << ", " << i->second;
        o << ")";
	return o;
}
}

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_itemset_shar {
	ItemSet md;
	ValueBag testValues;

	arki_itemset_shar()
	{
		testValues.set("foo", Value::createInteger(5));
		testValues.set("bar", Value::createInteger(5000));
		testValues.set("baz", Value::createInteger(-200));
		testValues.set("moo", Value::createInteger(0x5ffffff));
		testValues.set("antani", Value::createInteger(-1));
		testValues.set("blinda", Value::createInteger(0));
		testValues.set("supercazzola", Value::createInteger(-1234567));
		testValues.set("pippo", Value::createString("pippo"));
		testValues.set("pluto", Value::createString("12"));
		testValues.set("cippo", Value::createString(""));
	}

	void fill(ItemSet& md)
	{
		md.set(origin::GRIB1::create(1, 2, 3));
		md.set(product::GRIB1::create(1, 2, 3));
		md.set(level::GRIB1::create(114, 12, 34));
		md.set(timerange::GRIB1::create(1, 1, 2, 3));
		md.set(area::GRIB::create(testValues));
		md.set(proddef::GRIB::create(testValues));
		md.set(AssignedDataset::create("dsname", "dsid"));
		// Test POSITION reference times
		md.set(reftime::Position::create(types::Time::create(2006, 5, 4, 3, 2, 1)));
	}
};
TESTGRP(arki_itemset);

#define ensure_stores(T, x, y, z) impl_ensure_stores<T>(wibble::tests::Location(__FILE__, __LINE__, #x ", " #y ", " #z), (x), (y), (z))
template<typename T>
static inline void impl_ensure_stores(const wibble::tests::Location& loc, ItemSet& md, const Item<>& val, const Item<>& val1)
{
	// Make sure that val and val1 have the same serialisation code
	types::Code code = val->serialisationCode();
	ensure_equals(val1->serialisationCode(), code);

	// Start empty
	//inner_ensure_equals(md.size(), 0u);

	// Add one and check that it works
	md.set(val);
	//inner_ensure_equals(md.size(), 1u);
	inner_ensure_equals(md.get(code), val);

	// Test template get
	Item<T> i = md.get<T>();
	inner_ensure(i.defined());
	inner_ensure_equals(i, val);

	// Add the same again and check that things are still fine
	md.set(val);
	//inner_ensure_equals(md.size(), 1u);
	inner_ensure_equals(md.get(code), val);

	// Test template get
	i = md.get<T>();
	inner_ensure(i.defined());
	inner_ensure_equals(i, val);

	ItemSet md1;
	md1.clear();
	md1.set(val);
	inner_ensure_equals(md, md1);

	// Add a different one and see that we get it
	md.set(val1);
	//inner_ensure_equals(md.size(), 1u);
	inner_ensure_equals(md.get(code), val1);

	ItemSet md2;
	md2.clear();
	md2.set(val1);
	inner_ensure_equals(md, md2);
}

/*
 * It looks like most of these tests are just testing if std::set and
 * std::vector work.
 *
 * I thought about removing them, but then I relised that they're in fact
 * testing if all our metadata items work properly inside stl containers, so I
 * decided to keep them.
 */

// Test GRIB1 origins
template<> template<>
void to::test<1>()
{
	Item<> val(origin::GRIB1::create(1, 2, 3));
	Item<> val1(origin::GRIB1::create(2, 3, 4));
	ensure_stores(types::Origin, md, val, val1);
}

// Test BUFR origins
template<> template<>
void to::test<3>()
{
	// Add one and check that it works
	Item<> val(origin::BUFR::create(1, 2));
	Item<> val1(origin::BUFR::create(2, 3));
	ensure_stores(types::Origin, md, val, val1);
}

// Test Position reference times
template<> template<>
void to::test<4>()
{
	Item<> val(reftime::Position::create(types::Time::create(2006, 5, 4, 3, 2, 1)));
	Item<> val1(reftime::Position::create(types::Time::create(2008, 7, 6, 5, 4, 3)));
	ensure_stores(types::Reftime, md, val, val1);
#if 0
	// Test PERIOD reference times
	md.reftime.set(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3));
	ensure_equals(md.reftime.style, Reftime::PERIOD);

	Time tbegin;
	Time tend;
	md.reftime.get(tbegin, tend);
	ensure_equals(tbegin, Time(2007, 6, 5, 4, 3, 2));
	ensure_equals(tend, Time(2008, 7, 6, 5, 4, 3));

// Work-around for a bug in old gcc versions
#if __GNUC__ && __GNUC__ < 4
#else
	// Test open-ended PERIOD reference times
	md.reftime.set(tbegin, Time());
	ensure_equals(md.reftime.style, Reftime::PERIOD);

	tbegin = Time();
	tend = Time();
	md.reftime.get(tbegin, tend);

	ensure_equals(tbegin, Time(2007, 6, 5, 4, 3, 2));
	ensure_equals(tend, Time());
#endif
#endif
}

// Test GRIB product info
template<> template<>
void to::test<5>()
{
	Item<> val(product::GRIB1::create(1, 2, 3));
	Item<> val1(product::GRIB1::create(2, 3, 4));
	ensure_stores(types::Product, md, val, val1);
}

// Test BUFR product info
template<> template<>
void to::test<6>()
{
	ValueBag vb;
	vb.set("name", Value::createString("antani"));
	ValueBag vb1;
	vb1.set("name", Value::createString("blinda"));
	Item<> val(product::BUFR::create(1, 2, 3, vb));
	Item<> val1(product::BUFR::create(1, 2, 3, vb1));
	ensure_stores(types::Product, md, val, val1);
}

// Test levels
template<> template<>
void to::test<7>()
{
	Item<> val(level::GRIB1::create(114, 260));
	Item<> val1(level::GRIB1::create(120, 280));
	ensure_stores(types::Level, md, val, val1);
}

// Test time ranges
template<> template<>
void to::test<8>()
{
	Item<> val(timerange::GRIB1::create(1, 1, 2, 3));
	Item<> val1(timerange::GRIB1::create(2, 2, 3, 4));
	ensure_stores(types::Timerange, md, val, val1);
}

// Test areas
template<> template<>
void to::test<9>()
{
	ValueBag test1;
	test1.set("foo", Value::createInteger(5));
	test1.set("bar", Value::createInteger(5000));
	test1.set("baz", Value::createInteger(-200));
	test1.set("moo", Value::createInteger(0x5ffffff));
	test1.set("pippo", Value::createString("pippo"));
	test1.set("pluto", Value::createString("12"));
	test1.set("cippo", Value::createString(""));

	ValueBag test2;
	test2.set("antani", Value::createInteger(-1));
	test2.set("blinda", Value::createInteger(0));
	test2.set("supercazzola", Value::createInteger(-1234567));

	Item<> val(area::GRIB::create(test1));
	Item<> val1(area::GRIB::create(test2));
	ensure_stores(types::Area, md, val, val1);
}

// Test proddefs
template<> template<>
void to::test<10>()
{
	ValueBag test1;
	test1.set("foo", Value::createInteger(5));
	test1.set("bar", Value::createInteger(5000));
	test1.set("baz", Value::createInteger(-200));
	test1.set("moo", Value::createInteger(0x5ffffff));
	test1.set("pippo", Value::createString("pippo"));
	test1.set("pluto", Value::createString("12"));
	test1.set("cippo", Value::createString(""));

	ValueBag test2;
	test2.set("antani", Value::createInteger(-1));
	test2.set("blinda", Value::createInteger(0));
	test2.set("supercazzola", Value::createInteger(-1234567));

	Item<> val(proddef::GRIB::create(test1));
	Item<> val1(proddef::GRIB::create(test2));
	ensure_stores(types::Proddef, md, val, val1);
}

// Test dataset info
template<> template<>
void to::test<11>()
{
	Item<> val(AssignedDataset::create("test", "abcdefg"));
	Item<> val1(AssignedDataset::create("test1", "abcdefgh"));
	ensure_stores(types::AssignedDataset, md, val, val1);
}


}

// vim:set ts=4 sw=4:
