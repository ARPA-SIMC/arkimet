/*
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/tests.h>
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
using namespace arki::tests;
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
		md.set(reftime::Position::create(Time(2006, 5, 4, 3, 2, 1)));
	}
};
TESTGRP(arki_itemset);

template<typename T>
static inline void test_basic_itemset_ops(const std::string& vlower, const std::string& vhigher)
{
    types::Code code = types::traits<T>::type_code;
    ItemSet md;
    unique_ptr<Type> vlo = decodeString(code, vlower);
    unique_ptr<Type> vhi = decodeString(code, vhigher);

    wassert(actual(md.size()) == 0);
    wassert(actual(md.empty()).istrue());
    wassert(actual(md.has(code)).isfalse());

    // Add one element and check that it can be retrieved
    md.set(vlo->cloneType());
    wassert(actual(md.size()) == 1);
    wassert(actual(md.empty()).isfalse());
    wassert(actual(md.has(code)).istrue());
    wassert(actual(vlo) == md.get(code));

    // Test templated get
    const T* t = md.get<T>();
    wassert(actual(t).istrue());
    wassert(actual(vlo) == t);

    // Add the same again and check that things are still fine
    md.set(vlo->cloneType());
    wassert(actual(md.size()) == 1);
    wassert(actual(md.empty()).isfalse());
    wassert(actual(md.has(code)).istrue());
    wassert(actual(vlo) == md.get(code));

    // Add a different element and check that it gets replaced
    md.set(vhi->cloneType());
    wassert(actual(md.size()) == 1);
    wassert(actual(md.empty()).isfalse());
    wassert(actual(md.has(code)).istrue());
    wassert(actual(vhi) == md.get(code));

    // Test equality and comparison between ItemSets
    ItemSet md1;
    md1.set(vhi->cloneType());
    wassert(actual(md == md1).istrue());
    wassert(actual(md.compare(md1)) == 0);

    // Set a different item and verify that we do not test equal anymore
    md.set(vlo->cloneType());
    wassert(actual(md != md1).istrue());
    wassert(actual(md.compare(md1)) < 0);
    wassert(actual(md1.compare(md)) > 0);

    // Test unset
    md.unset(code);
    wassert(actual(md.size()) == 0);
    wassert(actual(md.empty()).istrue());
    wassert(actual(md.has(code)).isfalse());

    // Test clear
    md1.clear();
    wassert(actual(md.size()) == 0);
    wassert(actual(md.empty()).istrue());
    wassert(actual(md.has(code)).isfalse());
}

/*
 * It looks like most of these tests are just testing if std::set and
 * std::vector work.
 *
 * I thought about removing them, but then I relised that they're in fact
 * testing if all our metadata items work properly inside stl containers, so I
 * decided to keep them.
 */

// Test with several item types
template<> template<>
void to::test<1>()
{
    wassert(test_basic_itemset_ops<Origin>("GRIB1(1, 2, 3)", "GRIB1(2, 3, 4)"));
    wassert(test_basic_itemset_ops<Origin>("BUFR(1, 2)", "BUFR(2, 3)"));
    wassert(test_basic_itemset_ops<Reftime>("2006-05-04T03:02:01Z", "2008-07-06T05:04:03"));
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
    wassert(test_basic_itemset_ops<Product>("GRIB1(1, 2, 3)", "GRIB1(2, 3, 4)"));
    wassert(test_basic_itemset_ops<Product>("BUFR(1, 2, 3, name=antani)", "BUFR(1, 2, 3, name=blinda)"));
    wassert(test_basic_itemset_ops<Level>("GRIB1(114, 260, 123)", "GRIB1(120,280,123)"));
    wassert(test_basic_itemset_ops<Timerange>("GRIB1(1)", "GRIB1(2, 3y, 4y)"));
}

#if 0
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

// Check compareMaps
template<> template<>
void to::test<12>()
{
	using namespace utils;
	map<string, UItem<> > a;
	map<string, UItem<> > b;

	a["antani"] = b["antani"] = types::origin::GRIB1::create(1, 2, 3);
	a["pippo"] = types::run::Minute::create(12);

	ensure_equals(compareMaps(a, b), 1);
	ensure_equals(compareMaps(b, a), -1);
}
#endif

}
