/*
 * Copyright (C) 2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types/test-utils.h>
#include <arki/types/value.h>

#include <sstream>
#include <iostream>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_types_value_shar {
};
TESTGRP(arki_types_value);

// Check MINUTE
template<> template<>
void to::test<1>()
{
    using namespace utils::codec;
    Item<Value> o = Value::create("ciao");

    string encoded;
    Encoder enc(encoded);
    o->encodeWithoutEnvelope(enc);
    ensure_equals(encoded.size(), 4);
    ensure_equals(encoded, "ciao");

    ensure_equals(o, Item<Value>(Value::create("ciao")));

    ensure(o != Value::create("cia"));
    ensure(o != Value::create("ciap"));

    // Test encoding/decoding
    ensure_serialises(o, types::TYPE_VALUE);
}

// Test Lua functions
template<> template<>
void to::test<2>()
{
#if 0 // TODO
#ifdef HAVE_LUA
	Item<Run> o = run::Minute::create(12, 30);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'MINUTE' then return 'style is '..o.style..' instead of MINUTE' end \n"
		"  if o.hour ~= 12 then return 'o.hour is '..o.hour..' instead of 12' end \n"
		"  if o.min ~= 30 then return 'o.min is '..o.min..' instead of 30' end \n"
		"  if tostring(o) ~= 'MINUTE(12:30)' then return 'tostring gave '..tostring(o)..' instead of MINUTE(12:30)' end \n"
		"  local o1 = arki_run.minute(12, 30)\n"
		"  if o ~= o1 then return 'new run is '..tostring(o1)..' instead of '..tostring(o) end\n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
#endif
#endif
}

// Check comparisons
template<> template<>
void to::test<3>()
{
    ensure_compares(
            Value::create("ciao"),
            Value::create("ciap"),
            Value::create("ciap"));
}

}

// vim:set ts=4 sw=4:
