
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
#include <arki/types/quantity.h>

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

struct arki_types_quantity_shar
{
};

TESTGRP(arki_types_quantity);

template<> template<> void to::test<1>()
{
	Item<Quantity> o = Quantity::create("a,b,c");
	//ensure_equals(o->task, "a,b,c");

	ensure_equals(o, Item<Quantity>(Quantity::create("a,b,c")));
	ensure(o != Item<Quantity>(Quantity::create("a")));
	ensure(o != Item<Quantity>(Quantity::create("a,a")));
	ensure(o != Item<Quantity>(Quantity::create("b")));
	ensure(o != Item<Quantity>(Quantity::create("c")));
	ensure(o != Item<Quantity>(Quantity::create("1,b,c")));
	ensure(o != Item<Quantity>(Quantity::create("a,1,c")));
	ensure(o != Item<Quantity>(Quantity::create("a,b,1")));
	ensure(o != Item<Quantity>(Quantity::create("a,b,1")));
	ensure(o != Item<Quantity>(Quantity::create("1,2,3")));
}

template<> template<> void to::test<2>()
{
	Item<Quantity> o = Quantity::create("a,b,c");
	//ensure_equals(o->task, "a,b,c");

	// Test encoding/decoding
	ensure_serialises(o, types::TYPE_QUANTITY);
}

#ifdef HAVE_LUA
// Test Lua functions
template<> template<> void to::test<3>()
{
	//TODO xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
	//Item<Quantity> o = Quantity::create("a,b,c");
	//
	//tests::Lua test(
	//	"function test(o) \n"
	//	"  if o.task ~= 'task' then return 'content is '..o.task..' instead of task' end \n"
	//	"  if tostring(o) ~= 'task' then return 'tostring gave '..tostring(o)..' instead of task' end \n"
	//	"end \n"
	//);
	//test.pusharg(*o);
	//ensure_equals(test.run(), "");
}
#endif

/*============================================================================*/

}

// vim:set ts=4 sw=4:

































