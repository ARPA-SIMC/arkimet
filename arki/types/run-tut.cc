/*
 * Copyright (C) 2008--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/run.h>
#include <arki/matcher.h>

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

struct arki_types_run_shar {
};
TESTGRP(arki_types_run);

// Check MINUTE
template<> template<>
void to::test<1>()
{
	Item<Run> o = run::Minute::create(12);
	ensure_equals(o->style(), Run::MINUTE);
	const run::Minute* v = o->upcast<run::Minute>();
	ensure(v);

	ensure_equals(o, Item<Run>(run::Minute::create(12)));
	ensure_equals(o, Item<Run>(run::Minute::create(12, 0)));

	ensure(o != run::Minute::create(12, 1));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_RUN);

	// Test generating a matcher expression
	ensure_equals(o->exactQuery(), "MINUTE,12:00");
	Matcher m = Matcher::parse("run:" + o->exactQuery());
	ensure(m(o));
}

// Test Lua functions
template<> template<>
void to::test<2>()
{
#ifdef HAVE_LUA
	Item<Run> o = run::Minute::create(12, 30);

	tests::Lua test(
		"function test(o) \n"
		"  if o.style ~= 'MINUTE' then return 'style is '..o.style..' instead of MINUTE' end \n"
		"  if o.hour ~= 12 then return 'o.hour is '..o.hour..' instead of 12' end \n"
		"  if o.min ~= 30 then return 'o.min is '..o.min..' instead of 30' end \n"
		"  if tostring(o) ~= 'MINUTE(12:30)' then return 'tostring gave '..tostring(o)..' instead of MINUTE(12:30)' end \n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
#endif
}

// Check comparisons
template<> template<>
void to::test<3>()
{
	ensure_compares(
		run::Minute::create(12, 30),
		run::Minute::create(13, 00),
		run::Minute::create(13, 00));
}

}

// vim:set ts=4 sw=4:
