/*
 * arki/formatter/lua - Format arkimet values via a LUA script
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

#include <arki/formatter/lua.h>
#include <wibble/exception.h>
#include <arki/types.h>
// TODO: remove these two headers when possible
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/runtime.h>

extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}

using namespace std;

namespace arki {
namespace formatter {

#if 0
static string arkilua_dumptablekeys(lua_State* L, int index)
{
	string res;
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

static void arkilua_dumpstack(lua_State* L, const std::string& title, FILE* out)
{
	fprintf(out, "%s\n", title.c_str());
	for (int i = lua_gettop(L); i; --i)
	{
		int t = lua_type(L, i);
		switch (t)
		{
			case LUA_TNIL:
				fprintf(out, " %d: nil\n", i);
				break;
			case LUA_TBOOLEAN:
				fprintf(out, " %d: %s\n", i, lua_toboolean(L, i) ? "true" : "false");
				break;
			case LUA_TNUMBER:
				fprintf(out, " %d: %g\n", i, lua_tonumber(L, i));
				break;
			case LUA_TSTRING:
				fprintf(out, " %d: '%s'\n", i, lua_tostring(L, i));
				break;
			case LUA_TTABLE:
				fprintf(out, " %d: table: '%s'\n", i, arkilua_dumptablekeys(L, i).c_str());
				break;
			default:
				fprintf(out, " %d: <%s>\n", i, lua_typename(L, t));
				break;
		}
	}
}
#endif

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

	/// Load the prettyprinting functions

	// TODO: rcFiles also supports a 3rd StringOption parameter
	vector<string> sources = runtime::rcFiles("format", "ARKI_FORMATTER");
	for (vector<string>::const_iterator i = sources.begin(); i != sources.end(); ++i)
	{
		if (luaL_loadfile(L, i->c_str()))
			throw wibble::exception::Consistency("parsing Lua code for pretty printing", lua_tostring(L, -1));
		if (lua_pcall(L, 0, 0, 0))
		{
			string error = lua_tostring(L, -1);
			lua_pop(L, 1);
			throw wibble::exception::Consistency(error, "defining pretty printing functions");
		}
	}

	//arkilua_dumpstack(L, "Afterinit", stderr);
}

Lua::~Lua()
{
	if (L) lua_close(L);
}

std::string Lua::operator()(const Item<>& v) const
{
	string tag = v->tag();
	string func = "fmt_"+tag;

	lua_getglobal(L, func.c_str()); // function to be called
	//arkilua_dumpstack(L, func, stderr);
	if (lua_type(L, -1) == LUA_TNIL)
	{
		// If the formatter was not defined, we fall back to the parent
		// formatter
		lua_pop(L, 1);
		return Formatter::operator()(v);
	}
	v->lua_push(L);
	if (lua_pcall(L, 1, 1, 0))
	{
		string error = "Formatting failed: ";
		error += lua_tostring(L, -1);
		lua_pop(L, 1);
		return error;
	} else {
		if (lua_type(L, -1) == LUA_TNIL)
		{
			// The LUA function did not know how to format this: we fall back
			// to the parent formatter
			lua_pop(L, 1);
			return Formatter::operator()(v);
		} else {
			string res = lua_tostring(L, -1);
			lua_pop(L, 1);
			return res;
		}
	}
}

}
}
// vim:set ts=4 sw=4:
