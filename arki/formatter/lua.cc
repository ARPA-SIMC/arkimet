#include "config.h"
#include "arki/formatter/lua.h"
#include "arki/exceptions.h"
#include "arki/types.h"
#include "arki/utils/lua.h"
#include "arki/utils/files.h"

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
    vector<string> sources = utils::files::rcFiles("format", "ARKI_FORMATTER");
    for (vector<string>::const_iterator i = sources.begin(); i != sources.end(); ++i)
    {
        if (luaL_loadfile(*L, i->c_str()))
            throw_consistency_error("parsing Lua code for pretty printing", lua_tostring(*L, -1));
        if (lua_pcall(*L, 0, 0, 0))
        {
            string error = lua_tostring(*L, -1);
            lua_pop(*L, 1);
            throw_consistency_error(error, "defining pretty printing functions");
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
