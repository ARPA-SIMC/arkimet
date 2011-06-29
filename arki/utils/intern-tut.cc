/*
 * Copyright (C) 2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/utils/intern.h>
#include <wibble/string.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::utils;

struct arki_utils_intern_shar {
};
TESTGRP(arki_utils_intern);

template<> template<>
void to::test<1>()
{
    StringInternTable table;

    // Check initial state
    ensure_equals(table.table_size, 1024u);
    for (size_t i = 0; i < table.table_size; ++i)
        ensure(table.table[i] == 0);
    ensure(table.string_table.empty());

    // Check that interning returns stable IDs
    unsigned val = table.intern("foo");
    ensure_equals(table.intern("foo"), val);
    ensure_equals(table.intern(string("foo")), val);

    // Ensure that different strings have different IDs
    unsigned val1 = table.intern("bar");
    ensure(val != val1);

    // Check what happens if we fill the hashtable and need the overflow
    // buckets
    unsigned ids[2000];
    for (unsigned i = 0; i < 2000; ++i)
        ids[i] = table.intern(str::fmtf("val%u", i));
    for (unsigned i = 0; i < 2000; ++i)
        ensure_equals(ids[i], table.intern(str::fmtf("val%u", i)));
}

}

// vim:set ts=4 sw=4:
