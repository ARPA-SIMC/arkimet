#include "config.h"
#include "arki/scan/bufrlua.h"
#include "arki/metadata.h"
#include "arki/runtime/config.h"
#include "arki/utils/string.h"
#include "arki/exceptions.h"
#include <wreport/utils/lua.h>
#include <dballe/msg/msg.h>
#include <dballe/msg/context.h>

using namespace std;
using namespace dballe;
using namespace arki::utils;

namespace arki {
namespace scan {
namespace bufr {

static Msg* msg_lua_check(struct lua_State* L, int idx);
static void msg_context_lua_push(dballe::msg::Context* ctx, struct lua_State* L);
static msg::Context* msg_context_lua_check(struct lua_State* L, int idx);

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
    Msg* msg = msg_lua_check(L, 1);
    lua_pushstring(L, format_message_type(msg->type));
    return 1;
}

static int dbalua_msg_size(lua_State *L)
{
    Msg* msg = msg_lua_check(L, 1);
    lua_pushinteger(L, msg->data.size());
    return 1;
}

static int dbalua_msg_foreach(lua_State *L)
{
    Msg* msg = msg_lua_check(L, 1);
    for (size_t i = 0; i < msg->data.size(); ++i)
    {
        lua_pushvalue(L, -1);
        msg_context_lua_push(msg->data[i], L);
        /* Do the call (1 argument, 0 results) */
        if (lua_pcall(L, 1, 0, 0) != 0)
            lua_error(L);
    }
    return 0;
}

static int dbalua_msg_find(lua_State *L)
{
    Msg* msg = msg_lua_check(L, 1);
    wreport::Var* res = NULL;
    if (lua_gettop(L) == 2)
    {
        // By ID
        size_t len;
        const char* name = lua_tolstring(L, 2, &len);
        int id = resolve_var_substring(name, len);
        res = msg->edit_by_id(id);
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
        res = msg->edit(code, level, trange);
    }
    if (res == NULL)
        lua_pushnil(L);
    else
        res->lua_push(L);
    return 1;
}

static int dbalua_msg_tostring(lua_State *L)
{
    Msg* msg = msg_lua_check(L, 1);
    int varcount = 0;
    for (size_t i = 0; i < msg->data.size(); ++i)
        varcount += msg->data[i]->data.size();
    lua_pushfstring(L, "dba_msg(%s, %d ctx, %d vars)",
        format_message_type(msg->type), msg->data.size(), varcount);
    return 1;
}

static const struct luaL_Reg dbalua_msg_lib [] = {
    { "type", dbalua_msg_type },
    { "size", dbalua_msg_size },
    { "foreach", dbalua_msg_foreach },
    { "find", dbalua_msg_find },
    { "__tostring", dbalua_msg_tostring },
    {nullptr, nullptr}
};


static int dbalua_msg_context_size(lua_State *L)
{
    msg::Context* ctx = msg_context_lua_check(L, 1);
    lua_pushinteger(L, ctx->data.size());
    return 1;
}

static int dbalua_msg_context_foreach(lua_State *L)
{
    msg::Context* ctx = msg_context_lua_check(L, 1);
    for (size_t i = 0; i < ctx->data.size(); ++i)
    {
        lua_pushvalue(L, -1);
        ctx->data[i]->lua_push(L);
        /* do the call (1 argument, 0 results) */
        if (lua_pcall(L, 1, 0, 0) != 0)
            lua_error(L);
    }
    return 0;
}

static int dbalua_msg_context_tostring(lua_State *L)
{
    msg::Context* ctx = msg_context_lua_check(L, 1);
    lua_pushfstring(L, "dba_msg_context(%d vars)", ctx->data.size());
    return 1;
}

#define make_accessor(name, acc) static int dbalua_msg_context_##name(lua_State *L) { \
    msg::Context* ctx = msg_context_lua_check(L, 1); \
    lua_pushinteger(L, ctx->acc); \
    return 1; \
}
make_accessor(ltype1, level.ltype1)
make_accessor(l1, level.l1)
make_accessor(ltype2, level.ltype2)
make_accessor(l2, level.l2)
make_accessor(pind, trange.pind)
make_accessor(p1, trange.p1)
make_accessor(p2, trange.p2)
#undef make_accessor

static const struct luaL_Reg dbalua_msg_context_lib [] = {
    { "ltype1", dbalua_msg_context_ltype1 },
    { "l1", dbalua_msg_context_l1 },
    { "ltype2", dbalua_msg_context_ltype2 },
    { "l2", dbalua_msg_context_l2 },
    { "pind", dbalua_msg_context_pind },
    { "p1", dbalua_msg_context_p1 },
    { "p2", dbalua_msg_context_p2 },
    { "size", dbalua_msg_context_size },
    { "foreach", dbalua_msg_context_foreach },
    { "__tostring", dbalua_msg_context_tostring },
    {nullptr, nullptr}
};

static void msg_context_lua_push(dballe::msg::Context* ctx, struct lua_State* L)
{
    wreport::lua::push_object(L, ctx, "dballe.msg.context", dbalua_msg_context_lib);
}

static msg::Context* msg_context_lua_check(struct lua_State* L, int idx)
{
    msg::Context** v = (msg::Context**)luaL_checkudata(L, idx, "dballe.msg.context");
    return (v != NULL) ? *v : NULL;
}

static void msg_lua_push(dballe::Msg* msg, struct lua_State* L)
{
    wreport::lua::push_object(L, msg, "dballe.msg", dbalua_msg_lib);
}

static Msg* msg_lua_check(struct lua_State* L, int idx)
{
    Msg** v = (Msg**)luaL_checkudata(L, idx, "dballe.msg");
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
    Msg& msg = Msg::downcast(message);
    int funcid = get_scan_func(msg.type);

    // If we do not have a scan function for this message type, we are done
    if (funcid == -1) return;

    // Retrieve the Lua function registered for this
    lua_rawgeti(L, LUA_REGISTRYINDEX, funcid);

    // Pass msg
    msg_lua_push(&msg, L);

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
