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

#include <arki/types/test-utils.h>
#include <arki/types/note.h>

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

struct arki_types_note_shar {
};
TESTGRP(arki_types_note);

// Check Note, created with current time
template<> template<>
void to::test<1>()
{
	Item<Time> pre = Time::createNow();
	Item<Note> o = Note::create("test");
	Item<Time> post = Time::createNow();
	ensure_equals(o->content, "test");
	ensure(o->time >= pre);
	ensure(o->time <= post);

	ensure_equals(o, Item<Note>(Note::create(o->time, "test")));
	ensure(o != Item<Note>(Note::create(o->time, "test1")));
	ensure(o != Item<Note>(Note::create(Time::create(1800, 1, 2, 3, 4, 5), "test")));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_NOTE);
}

// Check Note, created with arbitrary time
template<> template<>
void to::test<2>()
{
	Item<Time> time = Time::create(2007, 6, 5, 4, 3, 2);
	Item<Note> o = Note::create(time, "test");
	ensure_equals(o->time, time);
	ensure_equals(o->content, "test");

	ensure_equals(o, Item<Note>(Note::create(time, "test")));
	ensure(o != Item<Note>(Note::create(time, "test1")));
	ensure(o != Item<Note>(Note::create(Time::create(2007, 6, 5, 4, 3, 1), "test")));

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_NOTE);
}

#ifdef HAVE_LUA
// Test Lua functions
template<> template<>
void to::test<3>()
{
	Item<Time> time = Time::create(2007, 6, 5, 4, 3, 2);
	Item<Note> o = Note::create(time, "test");

	tests::Lua test(
		"function test(o) \n"
		"  if o.content() ~= 'test' then return 'content is '..o.content()..' instead of test' end \n"
		"  t = o.time() \n"
		"  if t.year() ~= 2007 then return 't.year() is '..t.year()..' instead of 2007' end \n"
		"  if t.month() ~= 6 then return 't.month() is '..t.month()..' instead of 6' end \n"
		"  if t.day() ~= 5 then return 't.day() is '..t.day()..' instead of 5' end \n"
		"  if t.hour() ~= 4 then return 't.hour() is '..t.hour()..' instead of 4' end \n"
		"  if t.minute() ~= 3 then return 't.minute() is '..t.minute()..' instead of 3' end \n"
		"  if t.second() ~= 2 then return 't.second() is '..t.second()..' instead of 2' end \n"
		"  if tostring(o) ~= '[2007-06-05T04:03:02Z]test' then return 'tostring gave '..tostring(o)..' instead of [2007-06-05T04:03:02Z]test' end \n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
}
#endif

}
// vim:set ts=4 sw=4:
