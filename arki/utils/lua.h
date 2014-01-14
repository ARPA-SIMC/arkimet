#ifndef ARKI_UTILS_LUA_H
#define ARKI_UTILS_LUA_H

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

struct Lua
{
	lua_State *L;

	Lua(bool load_libs=true, bool load_arkimet=true);
	~Lua();

	operator lua_State*()
	{
		return L;
	}

    /**
     * Run a Lua string, returning an empty string if it succeeded, else the
     * error message.
     *
     * If the string fails with an empty error message, it returns the string
     * "<empty error message>", so testing for an empty result value will
     * always do the right thing.
     */
    std::string run_string(const std::string& str);

	/**
	 * Load a function from a file, and assign it to a global with the given
	 * name
	 */
	void functionFromFile(const std::string& name, const std::string& fname);

	/**
	 * Load a function from a string, and assign it to a global with the given
	 * name
	 */
	void functionFromString(const std::string& name, const std::string& buf, const std::string& fname);

	/**
	 * Call the functions prefix0...prefixCount, in sequence, passing no arguments.
	 *
	 * If no function raises any errors, returns an empty string.  Else,
	 * returns the error message raised by the first function that failed.  All
	 * the functions following the one that failed are not executed.
	 */
	std::string runFunctionSequence(const std::string& prefix, size_t count);

	static int backtrace_error_handler(lua_State *L);
};


namespace utils {
namespace lua {

template<typename T>
struct ManagedUD
{
	T* val;
	bool collected;

	static ManagedUD<T>* create(lua_State* L, T* val, bool collected = false)
	{
		ManagedUD<T>* ud = (ManagedUD<T>*)lua_newuserdata(L, sizeof(ManagedUD<T>));
		ud->val = val;
		ud->collected = collected;
		return ud;
	}
};


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

/**
 * Register lib in a global library with the given name.
 *
 * If no global with the given name exists, it is created as a new table.
 * If it already exists, entries from lib are merged into the existing table.
 * Nothing is left on the stack.
 */
void add_global_library(lua_State* L, const char* name, const luaL_Reg* lib);

/**
 * Add functions to the table currently at the top of the stack.
 */
void add_functions(lua_State* L, const luaL_Reg* lib);

/**
 * Push a userdata on the stack with a pointer to an object, and a metatable
 * pointing to a Lua library for accessing it as an object.
 */
template<typename T>
void push_object_ptr(lua_State* L, T* obj, const char* class_name, const luaL_Reg* lib)
{
    // The object we create is a userdata that holds a pointer to obj
    T** s = (T**)lua_newuserdata(L, sizeof(T*));
    *s = obj;

    // Set the metatable for the userdata
    if (luaL_newmetatable(L, class_name))
    {
        // If the metatable wasn't previously created, create it now
        lua_pushstring(L, "__index");
        lua_pushvalue(L, -2);  /* pushes the metatable */
        lua_settable(L, -3);  /* metatable.__index = metatable */

        // Load normal methods
        add_functions(L, lib);
    }

    lua_setmetatable(L, -2);
}

/**
 * Push a userdata on the stack with a copy of an object, and a metatable
 * pointing to a Lua library for accessing it as an object.
 */
template<typename T>
void push_object_copy(lua_State* L, const T& obj, const char* class_name, const luaL_Reg* lib)
{
    // The object we create is a userdata that holds a pointer to obj
    T* s = (T*)lua_newuserdata(L, sizeof(T));
    new(s) T(obj);

    // Set the metatable for the userdata
    if (luaL_newmetatable(L, class_name))
    {
        // If the metatable wasn't previously created, create it now
        lua_pushstring(L, "__index");
        lua_pushvalue(L, -2);  /* pushes the metatable */
        lua_settable(L, -3);  /* metatable.__index = metatable */

        // Load normal methods
        add_functions(L, lib);
    }

    lua_setmetatable(L, -2);
}

}
}
}

// vim:set ts=4 sw=4:
#endif
