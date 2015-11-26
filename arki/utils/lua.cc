/*
 * utils-lua - Lua-specific utility functions
 *
 * Copyright (C) 2008--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/wibble/exception.h>
#include <arki/wibble/string.h>
#include <ostream>

using namespace std;
using namespace wibble;

namespace arki {

namespace utils {
namespace lua {
void dumpstack(lua_State* L, const std::string& title, std::ostream& out);
}
}

Lua::Lua(bool load_libs, bool load_arkimet) : L(0)
{
    // Initialise the lua logic
    L = luaL_newstate();

    if (load_libs)
        luaL_openlibs(L);

    if (load_arkimet)
    {
        types::Type::lua_loadlib(L);
        Metadata::lua_openlib(L);
        Summary::lua_openlib(L);
        Matcher::lua_openlib(L);
    }
}

Lua::~Lua()
{
	if (L) lua_close(L);
}

std::string Lua::run_string(const std::string& str)
{
    if (luaL_dostring(L, str.c_str()))
    {
        // Copy the error, so that it will exist after the pop
        string error = lua_tostring(L, -1);
        // Pop the error from the stack
        lua_pop(L, 1);
        if (error.empty())
            return "empty error message>";
        else
            return error;
    }
    return string();
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
    lua_getglobal(L, "debug");
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
    int top = lua_gettop(L);
	for (int i = 1; i <= top; ++i)
	{
		int t = lua_type(L, i);
		out << i << ": " << t << ": ";
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

void add_global_library(lua_State* L, const char* name, const luaL_Reg* lib)
{
    lua_getglobal(L, name);
    if (lua_isnil(L, -1)) {
        lua_pop(L, 1);
        lua_newtable(L);
    }
    add_functions(L, lib);
    lua_setglobal(L, name);
}

void add_arki_global_library(lua_State* L, const char* name, const luaL_Reg* lib)
{
    // Get/create the 'arki' table
    lua_getglobal(L, "arki");
    if (lua_isnil(L, -1)) {
        lua_pop(L, 1);
        lua_newtable(L);
        // Push an extra reference to the table, so we still have it on the
        // stack after lua_setglobal
        lua_pushvalue(L, -1);
        lua_setglobal(L, "arki");
    }

    lua_pushstring(L, name);
    lua_newtable(L);
    add_functions(L, lib);
    lua_rawset(L, -3);

    lua_pop(L, 1);
}

void add_functions(lua_State* L, const luaL_Reg* lib)
{
#if LUA_VERSION_NUM >= 502
        luaL_setfuncs(L, lib, 0);
#else
        luaL_register(L, NULL, lib);
#endif
}

}
}
}
// vim:set ts=4 sw=4:
