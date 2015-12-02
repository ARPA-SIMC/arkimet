#include "arki/targetfile.h"
#include "arki/metadata.h"
#include "arki/metadata/consumer.h"
#include "arki/runtime/config.h"
#include "arki/runtime/io.h"
#include "arki/utils/string.h"
#include <sys/fcntl.h>

using namespace std;
using namespace arki::utils;

namespace arki {

namespace targetfile {
// FIXME: move as a singleton to arki/bbox.cc?
static __thread Targetfile* targetfile = 0;
}

Targetfile& Targetfile::instance()
{
	if (targetfile::targetfile == 0)
		targetfile::targetfile = new Targetfile();
	return *targetfile::targetfile;
}

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


Targetfile::Targetfile(const std::string& code) : L(new Lua)
{
	/// Load the target file functions

	// Create the function table
	lua_newtable(*L);
	lua_setglobal(*L, "targetfile");

    /// Load the targetfile functions
    if (code.empty())
    {
        loadRCFiles();
    } else {
        if (luaL_dostring(*L, code.c_str()))
        {
            // Copy the error, so that it will exist after the pop
            string error = lua_tostring(*L, -1);
            // Pop the error from the stack
            lua_pop(*L, 1);
            throw std::runtime_error("cannot execute Lua code from memory: " + error);
        }
    }

    //arkilua_dumpstack(L, "Afterinit", stderr);
}

Targetfile::~Targetfile()
{
	if (L) delete L;
}

void Targetfile::loadRCFiles()
{
    vector<string> files = runtime::Config::get().dir_targetfile.list_files(".lua");
    for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
    {
        if (luaL_dofile(*L, i->c_str()))
        {
            // Copy the error, so that it will exist after the pop
            string error = lua_tostring(*L, -1);
            // Pop the error from the stack
            lua_pop(*L, 1);
            throw std::runtime_error("cannot execute Lua file " + *i + ": " + error);
        }
    }
}

Targetfile::Func Targetfile::get(const std::string& def)
{
    std::map<std::string, int>::iterator i = ref_cache.find(def);

    if (i == ref_cache.end())
    {
        size_t pos = def.find(':');
        if (pos == string::npos)
            throw std::runtime_error("cannot parse targetfile definition \"" + def + "\": definition not in the form type:parms");
        string type = def.substr(0, pos);
        string parms = def.substr(pos+1);

        // Get targetfile[type]
        lua_getglobal(*L, "targetfile");
        lua_pushlstring(*L, type.data(), type.size());
        lua_gettable(*L, -2);
        if (lua_type(*L, -1) == LUA_TNIL)
        {
            lua_pop(*L, 2);
            throw std::runtime_error("cannot parse targetfile definition \"" + def + "\": no targetfile found of type \"" + type + "\"");
        }

        // Call targetfile[type](parms)
        lua_pushlstring(*L, parms.data(), parms.size());
        if (lua_pcall(*L, 1, 1, 0))
        {
            string error = lua_tostring(*L, -1);
            lua_pop(*L, 2);
            throw std::runtime_error("cannot create targetfile function \"" + def + "\": " + error);
        }

		// Ref the created function into the registry
		int idx = luaL_ref(*L, LUA_REGISTRYINDEX);
		lua_pop(*L, 1);

		pair< std::map<std::string, int>::iterator, bool > res =
			ref_cache.insert(make_pair(def, idx));
		i = res.first;
	}

	// Return the functor wrapper to the function
	return Func(L, i->second);
}

std::string Targetfile::Func::operator()(Metadata& md)
{
	// Get the function
	lua_rawgeti(*L, LUA_REGISTRYINDEX, idx);
	
	// Push the metadata
	md.lua_push(*L);

    // Call the function
    if (lua_pcall(*L, 1, 1, 0))
    {
        string error = lua_tostring(*L, -1);
        lua_pop(*L, 1);
        throw std::runtime_error("cannot run targetfile function" + error);
    }

	string res = lua_tostring(*L, -1);
	lua_pop(*L, 1);
	return res;
}

TargetfileSpy::TargetfileSpy(ReadonlyDataset& ds, sys::NamedFileDescriptor& output, const std::string& def)
    : func(Targetfile::instance().get(def)), ds(ds), output(output)
{
}

TargetfileSpy::~TargetfileSpy()
{
    delete cur_output;
}

void TargetfileSpy::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    ds.query_data(q, [&](unique_ptr<Metadata> md) {
        redirect(*md);
        return dest(move(md));
    });
}

void TargetfileSpy::querySummary(const Matcher& matcher, Summary& summary)
{
    ds.querySummary(matcher, summary);
}

void TargetfileSpy::redirect(Metadata& md)
{
    string pathname = func(md);
    if (!cur_output || cur_output->name() != pathname)
    {
        delete cur_output;
        cur_output = new sys::File(pathname, O_WRONLY | O_CREAT | O_APPEND);
        output = *cur_output;
    }
}

}
