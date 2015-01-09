/*
 * metadata/consumer - Metadata consumer interface and tools
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/metadata/consumer.h>
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/formatter.h>
#include <arki/emitter/json.h>
#include "config.h"
#include <ostream>

#ifdef HAVE_LUA
extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace metadata {

bool FilteredEater::eat(auto_ptr<Metadata> md)
{
    if (!matcher(*md))
        return true;
    return next.eat(md);
}

bool FilteredObserver::observe(const Metadata& md) override
{
    if (!matcher(md))
        return true;
    return next.observe(md);
}

bool SummarisingObserver::observe(const Metadata& md)
{
    s.add(md);
    return true;
}

bool SummarisingEater::eat(auto_ptr<Metadata> md)
{
    s.add(*md);
    return true;
}

#ifdef HAVE_LUA

LuaConsumer::LuaConsumer(lua_State* L, int funcid) : L(L), funcid(funcid) {}
LuaConsumer::~LuaConsumer()
{
	// Unindex the function
	luaL_unref(L, LUA_REGISTRYINDEX, funcid);
}

bool LuaConsumer::eat(auto_ptr<Metadata> md)
{
    // Get the function
    lua_rawgeti(L, LUA_REGISTRYINDEX, funcid);

    // Push the metadata, handing it over to Lua's garbage collector
    Metadata::lua_push(L, md);

    // Call the function
    if (lua_pcall(L, 1, 1, 0))
    {
        string error = lua_tostring(L, -1);
        lua_pop(L, 1);
        throw wibble::exception::Consistency("running metadata consumer function", error);
    }

    int res = lua_toboolean(L, -1);
    lua_pop(L, 1);
    return res;
}

auto_ptr<LuaConsumer> LuaConsumer::lua_check(lua_State* L, int idx)
{
	luaL_checktype(L, idx, LUA_TFUNCTION);

	// Ref the created function into the registry
	lua_pushvalue(L, idx);
	int funcid = luaL_ref(L, LUA_REGISTRYINDEX);

	// Create a consumer using the function
	return auto_ptr<LuaConsumer>(new LuaConsumer(L, funcid));
}

#endif

}
}

// vim:set ts=4 sw=4:
