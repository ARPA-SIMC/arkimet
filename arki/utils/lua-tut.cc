/*
 * Copyright (C) 2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/utils/lua.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;

struct arki_utils_lua_shar {
};
TESTGRP(arki_utils_lua);

// Run lua-example/*.lua files, doctest style
template<> template<>
void to::test<1>()
{
	Lua L;

	// Define 'ensure' function
	string ensure = "function ensure(val)\n"
			"  if (not val) then error('assertion failed:\\n' .. debug.traceback()) end\n"
			"end\n"
			"function ensure_equals(a, b)\n"
			"  if (a ~= b) then error('assertion failed: [' .. tostring(a) .. '] ~= [' .. tostring(b) .. ']\\n' .. debug.traceback()) end\n"
			"end\n";
	if (luaL_dostring(L, ensure.c_str()))
        {
                // Copy the error, so that it will exist after the pop
                string error = lua_tostring(L, -1);
                // Pop the error from the stack
                lua_pop(L, 1);
		ensure_equals(error, "");
        }
	
	// Run the various lua examples
	string path = "lua-examples";
	sys::fs::Directory dir(path);
	for (sys::fs::Directory::const_iterator d = dir.begin(); d != dir.end(); ++d)
	{
		if (!str::endsWith(*d, ".lua")) continue;
		string fname = str::joinpath(path, *d);
		if (luaL_dofile(L, fname.c_str()))
		{
			// Copy the error, so that it will exist after the pop
			string error = lua_tostring(L, -1);
			// Pop the error from the stack
			lua_pop(L, 1);
			ensure_equals(error, "");
		}
	}
}

}

// vim:set ts=4 sw=4:
