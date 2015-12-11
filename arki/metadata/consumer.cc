#include <arki/metadata/consumer.h>
#include <arki/metadata.h>
#include "config.h"
#include <ostream>

#ifdef HAVE_LUA
extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}
#endif

using namespace std;
using namespace arki::utils;

namespace arki {
namespace metadata {

#ifdef HAVE_LUA

LuaConsumer::LuaConsumer(lua_State* L, int funcid) : L(L), funcid(funcid) {}
LuaConsumer::~LuaConsumer()
{
	// Unindex the function
	luaL_unref(L, LUA_REGISTRYINDEX, funcid);
}

bool LuaConsumer::eat(unique_ptr<Metadata>&& md)
{
    // Get the function
    lua_rawgeti(L, LUA_REGISTRYINDEX, funcid);

    // Push the metadata, handing it over to Lua's garbage collector
    Metadata::lua_push(L, move(md));

    // Call the function
    if (lua_pcall(L, 1, 1, 0))
    {
        string error = lua_tostring(L, -1);
        lua_pop(L, 1);
        throw std::runtime_error("cannot run metadata consumer function: " + error);
    }

    int res = lua_toboolean(L, -1);
    lua_pop(L, 1);
    return res;
}

unique_ptr<LuaConsumer> LuaConsumer::lua_check(lua_State* L, int idx)
{
	luaL_checktype(L, idx, LUA_TFUNCTION);

	// Ref the created function into the registry
	lua_pushvalue(L, idx);
	int funcid = luaL_ref(L, LUA_REGISTRYINDEX);

	// Create a consumer using the function
	return unique_ptr<LuaConsumer>(new LuaConsumer(L, funcid));
}

#endif

}
}
