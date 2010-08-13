
/*
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include <arki/types/test-utils.h>
#include <arki/types/task.h>

#include <sstream>
#include <iostream>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

namespace tut {

/*============================================================================*/

using namespace std;
using namespace arki;
using namespace arki::types;

struct arki_types_task_shar
{
};

TESTGRP(arki_types_task);

template<> template<> void to::test<1>()
{
	Item<Task> o = Task::create("task");
	ensure_equals(o->task, "task");

	ensure_equals(o, Item<Task>(Task::create("task")));
	ensure(o != Item<Task>(Task::create("baaa")));
	ensure(o != Item<Task>(Task::create("")));
}

template<> template<> void to::test<2>()
{
	Item<Task> o = Task::create("task");
	ensure_equals(o->task, "task");

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_TASK);
}

#ifdef HAVE_LUA
// Test Lua functions
template<> template<> void to::test<3>()
{
	Item<Task> o = Task::create("task");

	tests::Lua test(
		"function test(o) \n"
		"  if o.task ~= 'task' then return 'content is '..o.task..' instead of task' end \n"
		"  if tostring(o) ~= 'task' then return 'tostring gave '..tostring(o)..' instead of task' end \n"
		"end \n"
	);

	test.pusharg(*o);
	ensure_equals(test.run(), "");
}
#endif

/*============================================================================*/

}

// vim:set ts=4 sw=4:

































