/**
 * Copyright (C) 2008  ARPAE-SIMC <simc-urp@arpae.it>
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
#ifndef ARKI_TESTS_LUA_H
#define ARKI_TESTS_LUA_H

#include <arki/utils/lua.h>

#ifdef HAVE_LUA
#include <string>

namespace arki {
struct Lua;

namespace tests {

struct Lua
{
	arki::Lua *L;
	std::string m_filename;
	size_t arg_count;

	Lua(const std::string& src = std::string());
	~Lua();

	/// Load the test code from the given file
	void loadFile(const std::string& fname);

	/// Load the test code from the given string containing Lua source code
	void loadString(const std::string& buf);

	/// Runs the parsed code to let it define the 'test' function we are going
	/// to use
	void create_lua_object();
		
	/// Send Lua's print output to an ostream
	void captureOutput(std::ostream& buf);

	/// Call the lua_push method of \a arg to push it into the Lua stack as an
	/// argument to 'test'
	template<typename T>
	void pusharg(T& arg)
	{
		// If we are pushing the first argument, then also push the function
		if (arg_count == 0)
			lua_getglobal(*L, "test");
		arg.lua_push(*L);
		++arg_count;
	}

	/// Run the 'test' function and return its result, as a string
	std::string run();
};

}
}
#endif

#endif
