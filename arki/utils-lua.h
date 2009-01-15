#ifndef ARKI_UTILS_LUA_H
#define ARKI_UTILS_LUA_H

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

#include <wibble/string.h>
#include <sstream>
#include <iosfwd>

// This header is only included when we have lua support, so we do not need
// conditional compilation here
extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}

namespace arki {
namespace utils {
namespace lua {

/**
 * Replace the 'print' function inside \a L so that all its output goes to the
 * given ostream.
 */
void capturePrintOutput(lua_State* L, std::ostream& buf);

/**
 * Create a string with a formatted dump of all the keys of the table at the
 * given index
 */
std::string dumptablekeys(lua_State* L, int index);

/**
 * Dump the Lua stack to the given ostream
 */
void dumpstack(lua_State* L, const std::string& title, std::ostream& out);

/**
 * Generic __tostring method template to use in bindings
 */
template<typename T>
int tostring(lua_State* L)
{
	T& t = **(T**)lua_touserdata(L, 1);
	lua_pushstring(L, wibble::str::fmt(t).c_str());
	return 1;
}

/**
 * Generic __tostring method template to use in bindings, for arkimet raw types
 */
template<typename T>
int tostring_arkitype(lua_State* L)
{
	T& t = **(T**)lua_touserdata(L, 1);
	std::stringstream str;
	t.writeToOstream(str);
	lua_pushstring(L, str.str().c_str());
	return 1;
}

}
}
}

// vim:set ts=4 sw=4:
#endif
