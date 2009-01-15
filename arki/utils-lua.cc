/*
 * utils-lua - Lua-specific utility functions
 *
 * Copyright (C) 2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/utils-lua.h>
#include <ostream>

using namespace std;

namespace arki {
namespace utils {
namespace lua {

// From src/lbaselib.c in lua 5.1
/*
** If your system does not support `stdout', you can just remove this function.
** If you need, you can define your own `print' function, following this
** model but changing `fputs' to put the strings at a proper place
** (a console window or a log file, for instance).
*/
static int lua_ostream_print(lua_State *L)
{
	// Access the closure parameters
	int outidx = lua_upvalueindex(1);
	void* ud = lua_touserdata(L, outidx);
	if (!ud)
		return luaL_error(L, "lua_report_print must be called as a closure with one userdata");
	std::ostream& out = **(std::ostream**)ud;

	int n = lua_gettop(L);  /* number of arguments */
	int i;
	lua_getglobal(L, "tostring");
	for (i=1; i<=n; i++) {
		const char *s;
		lua_pushvalue(L, -1);  /* function to be called */
		lua_pushvalue(L, i);   /* value to print */
		lua_call(L, 1, 1);
		s = lua_tostring(L, -1);  /* get result */
		if (s == NULL)
			return luaL_error(L, LUA_QL("tostring") " must return a string to "
					LUA_QL("print"));
		if (i>1) out << '\t';
		out << s;
		lua_pop(L, 1);  /* pop result */
	}
	out << endl;
	return 0;
}

void capturePrintOutput(lua_State* L, std::ostream& buf)
{
	// Create a C closure with the print function and the ostream to use

	std::ostream** s = (std::ostream**)lua_newuserdata(L, sizeof(std::ostream*));
	*s = &buf;
	lua_pushcclosure(L, lua_ostream_print, 1);

	// redefine print
	lua_setglobal(L, "print");
}

std::string dumptablekeys(lua_State* L, int index)
{
	std::string res;
	// Start iteration
	lua_pushnil(L);
	while (lua_next(L, index) != 0)
	{
		if (!res.empty()) res += ", ";
		// key is at index -2 and we want it to be a string
		if (lua_type(L, -2) != LUA_TSTRING)
			res += "<not a string>";
		else
			res += lua_tostring(L, -2);
		lua_pop(L, 1);
	}
	return res;
}

void dumpstack(lua_State* L, const std::string& title, std::ostream& out)
{
	out << title << endl;
	for (int i = lua_gettop(L); i; --i)
	{
		int t = lua_type(L, i);
		out << " " << t << ": ";
		switch (t)
		{
			case LUA_TNIL:
				out << "nil";
				break;
			case LUA_TBOOLEAN:
				out << (lua_toboolean(L, i) ? "true" : "false");
				break;
			case LUA_TNUMBER:
				out << lua_tonumber(L, i);
				break;
			case LUA_TSTRING:
				out << '"' << lua_tostring(L, i) << '"';
				break;
			case LUA_TTABLE:
				out << " table: " << dumptablekeys(L, i);
				break;
			default:
				out << '<' << lua_typename(L, t) << '>';
				break;
		}
		out << endl;
	}
}

}
}
}
// vim:set ts=4 sw=4:
