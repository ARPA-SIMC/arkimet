/*
 * utils-lua - Lua-specific utility functions
 *
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

#include <arki/utils/lua.h>
#include <arki/types.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <ostream>

using namespace std;
using namespace wibble;

namespace arki {

Lua::Lua() : L(0)
{
	// Initialise the lua logic
	L = lua_open();

	// NOTE: This one is optional: only use it for debugging
	#if LUA_VERSION_NUM >= 501
	luaL_openlibs(L);
	#else
	luaopen_base(L);              /* opens the basic library */
	luaopen_table(L);             /* opens the table library */
	luaopen_io(L);                /* opens the I/O library */
	luaopen_string(L);            /* opens the string lib. */
	luaopen_math(L);              /* opens the math lib. */
	luaopen_loadlib(L);           /* loadlib function */
	luaopen_debug(L);             /* debug library  */
	lua_settop(L, 0);
	#endif

	types::Type::lua_loadlib(L);
}

Lua::~Lua()
{
	if (L) lua_close(L);
}

void Lua::functionFromFile(const std::string& name, const std::string& fname)
{
	if (luaL_loadfile(L, fname.c_str()))
	{
		// Copy the error, so that it will exist after the pop
		string error = lua_tostring(L, -1);
		// Pop the error from the stack
		lua_pop(L, 1);
		throw wibble::exception::Consistency("parsing Lua code for function " + name, error);
	}

	// Set the function as a global variable
	lua_setglobal(L, name.c_str());
}

void Lua::functionFromString(const std::string& name, const std::string& buf, const std::string& fname)
{
	if (luaL_loadbuffer(L, buf.data(), buf.size(), fname.c_str()))
	{
		// Copy the error, so that it will exist after the pop
		string error = lua_tostring(L, -1);
		// Pop the error from the stack
		lua_pop(L, 1);
		throw wibble::exception::Consistency("parsing Lua code for function " + name, error);
	}
	// Set the function as the global variable "GRIB1"
	lua_setglobal(L, name.c_str());
}

std::string Lua::runFunctionSequence(const std::string& prefix, size_t count)
{
	for (size_t i = 0; i < count; ++i)
	{
		string name = prefix + str::fmt(i);
		lua_getglobal(L, name.c_str()); // function to be called
		if (lua_pcall(L, 0, 0, 0))
		{
			string error = lua_tostring(L, -1);
			lua_pop(L, 1);
			return error;
		}
	}
	return string();
}

int Lua::backtrace_error_handler(lua_State *L)
{
	lua_getfield(L, LUA_GLOBALSINDEX, "debug");
	if (!lua_istable(L, -1)) {
		lua_pop(L, 1);
		return 1;
	}
	lua_getfield(L, -1, "traceback");
	if (!lua_isfunction(L, -1)) {
		lua_pop(L, 2);
		return 1;
	}
	lua_pushvalue(L, 1);
	lua_pushinteger(L, 2);
	lua_call(L, 2, 1);
	return 1;
}
 
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
