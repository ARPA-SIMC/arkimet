/*
 * Copyright (C) 2012--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/tests.h>
#include "data.h"
#include "data/lines.h"

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;
using namespace wibble::tests;

struct arki_data_shar {
};

TESTGRP(arki_data);

namespace {

class TestItem
{
public:
    std::string relname;

    TestItem(const std::string& relname) : relname(relname) {}
};

}

// Test the item cache
template<> template<>
void to::test<1>()
{
    data::impl::Cache<TestItem, 3> cache;

    // Empty cache does not return things
    wassert(actual(cache.get("foo") == 0).istrue());

    // Add returns the item we added
    TestItem* foo = new TestItem("foo");
    wassert(actual(cache.add(auto_ptr<TestItem>(foo)) == foo).istrue());

    // Get returns foo
    wassert(actual(cache.get("foo") == foo).istrue());

    // Add two more items
    TestItem* bar = new TestItem("bar");
    wassert(actual(cache.add(auto_ptr<TestItem>(bar)) == bar).istrue());
    TestItem* baz = new TestItem("baz");
    wassert(actual(cache.add(auto_ptr<TestItem>(baz)) == baz).istrue());

    // With max_size=3, the cache should hold them all
    wassert(actual(cache.get("foo") == foo).istrue());
    wassert(actual(cache.get("bar") == bar).istrue());
    wassert(actual(cache.get("baz") == baz).istrue());

    // Add an extra item: the last recently used was foo, which gets popped
    TestItem* gnu = new TestItem("gnu");
    wassert(actual(cache.add(auto_ptr<TestItem>(gnu)) == gnu).istrue());

    // Foo is not in cache anymore, bar baz and gnu are
    wassert(actual(cache.get("foo") == 0).istrue());
    wassert(actual(cache.get("bar") == bar).istrue());
    wassert(actual(cache.get("baz") == baz).istrue());
    wassert(actual(cache.get("gnu") == gnu).istrue());
}

template<> template<>
void to::test<2>()
{
    using namespace arki::data;
#if 0

    Writer w1 = Writer::get("grib", "test-data-writer");
    Writer w2 = Writer::get("grib", "test-data-writer");

    // Check that the implementation is reused
    wassert(actual(w1._implementation()) == w2._implementation());
#endif
}

}
