#include "arki/scan/bufrlua.h"
#include "arki/metadata.h"
#include "arki/runtime/config.h"
#include "arki/utils/string.h"
#include "arki/exceptions.h"
#include <wreport/utils/lua.h>
#include <wreport/var.h>
#include <dballe/msg/msg.h>

using namespace std;
using namespace dballe;
using namespace arki::utils;

namespace arki {
namespace scan {
namespace bufr {

static Message* msg_lua_check(struct lua_State* L, int idx);

static wreport::Varcode dbalua_to_varcode(lua_State* L, int idx)
{
    const char* str = lua_tostring(L, idx);
    if (str == NULL)
        return 0;
    else
        return wreport::varcode_parse(str);
}

static int dbalua_msg_type(lua_State *L)
{
    Message* msg = msg_lua_check(L, 1);
    lua_pushstring(L, format_message_type(msg->get_type()));
    return 1;
}

static int dbalua_msg_foreach(lua_State *L)
{
    Message* message = msg_lua_check(L, 1);
    message->foreach_var([&](const Level& l, const Trange& t, const wreport::Var& v) {
        lua_pushvalue(L, -1);
        lua_newtable(L);
        lua_pushstring(L, "ltype1"); lua_pushinteger(L, l.ltype1); lua_settable(L, -3);
        lua_pushstring(L, "l1"); lua_pushinteger(L, l.l1); lua_settable(L, -3);
        lua_pushstring(L, "ltype2"); lua_pushinteger(L, l.ltype2); lua_settable(L, -3);
        lua_pushstring(L, "l2"); lua_pushinteger(L, l.l2); lua_settable(L, -3);
        lua_pushstring(L, "pind"); lua_pushinteger(L, t.pind); lua_settable(L, -3);
        lua_pushstring(L, "p1"); lua_pushinteger(L, t.p1); lua_settable(L, -3);
        lua_pushstring(L, "p2"); lua_pushinteger(L, t.p2); lua_settable(L, -3);
        wreport::Var var(v);
        lua_pushstring(L, "var"); var.lua_push(L); lua_settable(L, -3);

        /* Do the call (1 argument, 0 results) */
        if (lua_pcall(L, 1, 0, 0) != 0)
            lua_error(L);
        return true;
    });
    return 0;
}

static int dbalua_msg_find(lua_State *L)
{
    Msg& msg = Msg::downcast(*msg_lua_check(L, 1));
    wreport::Var* res = NULL;
    if (lua_gettop(L) == 2)
    {
        // By ID
        size_t len;
        const char* name = lua_tolstring(L, 2, &len);
        int id = resolve_var_substring(name, len);
        res = msg.edit_by_id(id);
    } else {
        // By all details
        wreport::Varcode code = dbalua_to_varcode(L, 2);
        Level level;
        level.ltype1 = lua_isnil(L, 3) ? MISSING_INT : lua_tointeger(L, 3);
        level.l1 = lua_isnil(L, 4) ? MISSING_INT : lua_tointeger(L, 4);
        level.ltype2 = lua_isnil(L, 5) ? MISSING_INT : lua_tointeger(L, 5);
        level.l2 = lua_isnil(L, 6) ? MISSING_INT : lua_tointeger(L, 6);
        Trange trange;
        trange.pind = lua_isnil(L, 7) ? MISSING_INT : lua_tointeger(L, 7);
        trange.p1 = lua_isnil(L, 8) ? MISSING_INT : lua_tointeger(L, 8);
        trange.p2 = lua_isnil(L, 9) ? MISSING_INT : lua_tointeger(L, 9);
        res = msg.edit(code, level, trange);
    }
    if (res == NULL)
        lua_pushnil(L);
    else
        res->lua_push(L);
    return 1;
}

static int dbalua_msg_tostring(lua_State *L)
{
    Message* message = msg_lua_check(L, 1);
    lua_pushfstring(L, "dba_msg(%s)", format_message_type(message->get_type()));
    return 1;
}

static const struct luaL_Reg dbalua_msg_lib [] = {
    { "type", dbalua_msg_type },
    { "foreach", dbalua_msg_foreach },
    { "find", dbalua_msg_find },
    { "__tostring", dbalua_msg_tostring },
    {nullptr, nullptr}
};

static void msg_lua_push(dballe::Message* msg, struct lua_State* L)
{
    wreport::lua::push_object(L, msg, "dballe.msg", dbalua_msg_lib);
}

static Message* msg_lua_check(struct lua_State* L, int idx)
{
    Message** v = (Message**)luaL_checkudata(L, idx, "dballe.msg");
    return (v != NULL) ? *v : NULL;
}


BufrLua::BufrLua()
{
    // Load common library functions, if they exist
    string fname = runtime::Config::get().dir_scan_bufr.find_file_noerror("common.lua");
    if (!fname.empty())
    {
        if (luaL_dofile(L, fname.c_str()))
        {
            // Copy the error, so that it will exist after the pop
            string error = lua_tostring(L, -1);
            // Pop the error from the stack
            lua_pop(L, 1);
            throw_consistency_error("parsing " + fname, error);
        }
    }
}

BufrLua::~BufrLua()
{
}

int BufrLua::get_scan_func(MessageType type)
{
    // First look up in cache
    auto i = scan_funcs.find(type);
    if (i != scan_funcs.end()) return i->second;

    // Else try to load it
    string name = str::lower(format_message_type(type));

    // Load the right bufr scan file
    string fname = runtime::Config::get().dir_scan_bufr.find_file_noerror(name + ".lua");

    // If the fine does not exist, we are done
    if (fname.empty())
        return scan_funcs[type] = -1;

    // Compile the macro
    if (luaL_dofile(L, fname.c_str()))
    {
        // Copy the error, so that it will exist after the pop
        string error = lua_tostring(L, -1);
        // Pop the error from the stack
        lua_pop(L, 1);
        throw_consistency_error("parsing " + fname, error);
    }

	// Index the queryData function
	int id = -1;
        lua_getglobal(L, "scan");
        if (lua_isfunction(L, -1))
                id = luaL_ref(L, LUA_REGISTRYINDEX);

	// Save it in cache
	return scan_funcs[type] = id;
}

void BufrLua::scan(Message& message, Metadata& md)
{
    int funcid = get_scan_func(message.get_type());

    // If we do not have a scan function for this message type, we are done
    if (funcid == -1) return;

    // Retrieve the Lua function registered for this
    lua_rawgeti(L, LUA_REGISTRYINDEX, funcid);

    // Pass msg
    msg_lua_push(&message, L);

    // Pass md
    md.lua_push(L);

    // Call the function
    if (lua_pcall(L, 2, 0, 0))
    {
            string error = lua_tostring(L, -1);
            lua_pop(L, 1);
            throw_consistency_error("running BUFR scan function", error);
    }
}

}
}
}
