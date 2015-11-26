/*
 * arki/formatter/lua - Format arkimet values via a LUA script
 *
 * Copyright (C) 2008--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/formatter/lua.h>
#include <arki/wibble/exception.h>
#include <arki/types.h>
#include <arki/utils/lua.h>
#include <arki/runtime/config.h>

extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}

using namespace std;
using namespace arki::types;

namespace arki {
namespace formatter {

Lua::Lua() : L(new arki::Lua)
{
	/// Load the prettyprinting functions

	// TODO: rcFiles also supports a 3rd StringOption parameter
	vector<string> sources = runtime::rcFiles("format", "ARKI_FORMATTER");
	for (vector<string>::const_iterator i = sources.begin(); i != sources.end(); ++i)
	{
		if (luaL_loadfile(*L, i->c_str()))
			throw wibble::exception::Consistency("parsing Lua code for pretty printing", lua_tostring(*L, -1));
		if (lua_pcall(*L, 0, 0, 0))
		{
			string error = lua_tostring(*L, -1);
			lua_pop(*L, 1);
			throw wibble::exception::Consistency(error, "defining pretty printing functions");
		}
	}

	//arkilua_dumpstack(L, "Afterinit", stderr);
}

Lua::~Lua()
{
	if (L) delete L;
}

std::string Lua::operator()(const Type& v) const
{
    string tag = v.tag();
    string func = "fmt_" + tag;

	lua_getglobal(*L, func.c_str()); // function to be called
	//arkilua_dumpstack(L, func, stderr);
	if (lua_type(*L, -1) == LUA_TNIL)
	{
		// If the formatter was not defined, we fall back to the parent
		// formatter
		lua_pop(*L, 1);
		return Formatter::operator()(v);
    }
    v.lua_push(*L);
    if (lua_pcall(*L, 1, 1, 0))
    {
		string error = "Formatting failed: ";
		error += lua_tostring(*L, -1);
		lua_pop(*L, 1);
		return error;
	} else {
		if (lua_type(*L, -1) == LUA_TNIL)
		{
			// The Lua function did not know how to format this: we fall back
			// to the parent formatter
			lua_pop(*L, 1);
			return Formatter::operator()(v);
		} else {
			string res = lua_tostring(*L, -1);
			lua_pop(*L, 1);
			return res;
		}
	}
}

}
}
// vim:set ts=4 sw=4:
