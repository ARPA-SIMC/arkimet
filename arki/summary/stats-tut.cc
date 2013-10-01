/*
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/summary/stats.h>
#include <arki/metadata.h>
#include <wibble/sys/buffer.h>

#include "config.h"

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
using namespace wibble;
using namespace wibble::tests;

struct arki_summary_stats_shar {
    Metadata md;

    arki_summary_stats_shar()
    {
        md.set(types::reftime::Position::create(types::Time::create(2009, 8, 7, 6, 5, 4)));
        sys::Buffer buf;
        md.setInlineData("grib1", buf);
    }
};
TESTGRP(arki_summary_stats);

// Basic stats tests
template<> template<>
void to::test<1>()
{
    using namespace arki::summary;

    Stats* s;
    Item<Stats> st(s = new Stats);
    ensure_equals(st->count, 0u);
    ensure_equals(st->size, 0u);
    ensure(st == Item<Stats>(new Stats()));

    s->merge(md);
    ensure_equals(st->count, 1u);
    ensure_equals(st->size, 0u);

    Item<Stats> st1(s = new Stats);
    s->merge(md);
    s->merge(md);

    wassert(actual(st).compares(st1, st1));
}

// Basic encode/decode tests
template<> template<>
void to::test<2>()
{
    using namespace arki::summary;

    Stats* s;
    Item<Stats> st(s = new Stats);
    s->merge(md);
    wassert(actual(st).serializes());
}

// Basic encode/decode tests with large numbers
template<> template<>
void to::test<3>()
{
    using namespace arki::summary;

    Stats* s;
    Item<Stats> st(s = new Stats);
    s->merge(md);
    s->count = 0x7FFFffffUL;
    s->size = 0x7FFFffffFFFFffffUL;
    wassert(actual(st).serializes());
}

// Test Lua functions
template<> template<>
void to::test<4>()
{
#ifdef HAVE_LUA
    /*
    s.add(md2);

    tests::Lua test(
        "function test(s) \n"
        "  if s:count() ~= 3 then return 'count is '..s.count()..' instead of 3' end \n"
        "  if s:size() ~= 50 then return 'size is '..s.size()..' instead of 50' end \n"
        "  i = 0 \n"
        "  items = {} \n"
        "  for idx, entry in ipairs(s:data()) do \n"
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
    */
#endif
}

}

// vim:set ts=4 sw=4:
