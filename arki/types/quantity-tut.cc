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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include <arki/types/tests.h>
#include <arki/types/quantity.h>
#include <arki/tests/lua.h>

namespace tut {

using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_types_quantity_shar
{
};

TESTGRP(arki_types_quantity);

template<> template<> void to::test<1>()
{
    tests::TestGenericType t("quantity", "a,b,c");
    t.lower.push_back("a");
    t.lower.push_back("a,a");
    t.lower.push_back("1,b,c");
    t.lower.push_back("a,1,c");
    t.lower.push_back("a,b,1");
    t.lower.push_back("1,2,3");
    t.higher.push_back("b");
    t.higher.push_back("c");
    t.higher.push_back("c,d");
    t.higher.push_back("c,d,e");
    wassert(t);
}

#ifdef HAVE_LUA
// Test Lua functions
template<> template<> void to::test<2>()
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

}
