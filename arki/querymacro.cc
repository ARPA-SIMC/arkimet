#include "config.h"
#include "querymacro.h"
#include "configfile.h"
#include "metadata.h"
#include "metadata/consumer.h"
#include "summary.h"
#include "dataset/gridquery.h"
#include "nag.h"
#include "runtime/config.h"
#include "runtime/io.h"
#include "utils/lua.h"
#include "sort.h"
#include "utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {

static Querymacro* checkqmacro(lua_State *L, int idx = 1)
{
	void* ud = luaL_checkudata(L, idx, "arki.querymacro");
	luaL_argcheck(L, ud != NULL, idx, "`querymacro' expected");
	return *(Querymacro**)ud;
}

static int arkilua_dataset(lua_State *L)
{
    Querymacro* rd = checkqmacro(L);
    const char* name = luaL_checkstring(L, 2);
    dataset::Reader* ds = rd->dataset(name);
    if (ds)
    {
        ds->lua_push(L);
        return 1;
    } else {
        lua_pushnil(L);
        return 1;
    }
}

static int arkilua_metadataconsumer(lua_State *L)
{
    Metadata* md = Metadata::lua_check(L, 1);
    luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");

    int considx = lua_upvalueindex(1);
    metadata_dest_func* cons = (metadata_dest_func*)lua_touserdata(L, considx);

    // FIXME: this copy may not be needed, but before removing it we need to
    // review memory ownership for this case
    unique_ptr<Metadata> copy(new Metadata(*md));
    lua_pushboolean(L, (*cons)(move(copy)));
    return 1;
}

static int arkilua_tostring(lua_State *L)
{
	lua_pushstring(L, "querymacro");
	return 1;
}

static const struct luaL_Reg querymacrolib[] = {
	{ "dataset", arkilua_dataset },	                // qm:dataset(name) -> dataset
	{ "__tostring", arkilua_tostring },
	{NULL, NULL}
};

Querymacro::Querymacro(const ConfigFile& ds_cfg, const ConfigFile& dispatch_cfg, const std::string& name, const std::string& query)
    : dispatch_cfg(dispatch_cfg), L(new Lua), funcid_querydata(-1), funcid_querysummary(-1)
{
    m_config = std::shared_ptr<const dataset::Config>(new dataset::Config(ds_cfg));
	Summary::lua_openlib(*L);
	Matcher::lua_openlib(*L);
	dataset::GridQuery::lua_openlib(*L);

    // Create the Querymacro object
    utils::lua::push_object_ptr(*L, this, "arki.querymacro", querymacrolib);

	// global qmacro = our userdata object
	lua_setglobal(*L, "qmacro");

	// Load the data as a global variable
	lua_pushstring(*L, query.c_str());
	lua_setglobal(*L, "query");

	// Set 'debug' and 'verbose' globals
	lua_pushboolean(*L, nag::is_verbose());
	lua_setglobal(*L, "verbose");
	lua_pushboolean(*L, nag::is_debug());
	lua_setglobal(*L, "debug");
	
	// Split macro name and arguments
	string macroname;
	size_t pos = name.find(" ");
	if (pos == string::npos)
	{
		macroname = name;
		lua_pushnil(*L);
    } else {
        macroname = name.substr(0, pos);
        string macroargs = str::strip(name.substr(pos + 1));
        lua_pushstring(*L, macroargs.c_str());
    }
	// Load the arguments as a global variable
	lua_setglobal(*L, "args");

    /// Load the right qmacro file
    string fname = runtime::Config::get().dir_qmacro.find_file(macroname + ".lua");
    if (luaL_dofile(*L, fname.c_str()))
    {
        // Copy the error, so that it will exist after the pop
        string error = lua_tostring(*L, -1);
        // Pop the error from the stack
        lua_pop(*L, 1);
        throw std::runtime_error("cannot parse " + fname + ": " + error);
    }

	/// Index the queryData function
	lua_getglobal(*L, "queryData");
	if (lua_isfunction(*L, -1))
		funcid_querydata = luaL_ref(*L, LUA_REGISTRYINDEX);

	/// Index the querySummary function
	lua_getglobal(*L, "querySummary");
	if (lua_isfunction(*L, -1))
		funcid_querysummary = luaL_ref(*L, LUA_REGISTRYINDEX);

	// utils::lua::dumpstack(*L, "Afterinit", cerr);
}

Querymacro::~Querymacro()
{
	if (L) delete L;
	for (std::map<std::string, Reader*>::iterator i = ds_cache.begin();
			i != ds_cache.end(); ++i)
		delete i->second;
}

std::string Querymacro::type() const { return "querymacro"; }

dataset::Reader* Querymacro::dataset(const std::string& name)
{
    std::map<std::string, dataset::Reader*>::iterator i = ds_cache.find(name);
    if (i == ds_cache.end())
    {
        ConfigFile* dscfg = dispatch_cfg.section(name);
        if (!dscfg) return 0;
        auto ds = dataset::Reader::create(*dscfg);
        pair<map<string, dataset::Reader*>::iterator, bool> res = ds_cache.insert(make_pair(name, ds.release()));
        i = res.first;
    }
    return i->second;
}

void Querymacro::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    if (funcid_querydata == -1) return;

    // Retrieve the Lua function registered for this
    lua_rawgeti(*L, LUA_REGISTRYINDEX, funcid_querydata);

    // Pass DataQuery
    lua_newtable(*L);
    q.lua_push_table(*L, -1);

    // Push consumer C closure
    if (q.sorter)
    {
        shared_ptr<sort::Stream> sorter(new sort::Stream(*q.sorter, dest));
        dest = [sorter](unique_ptr<Metadata> md) { return sorter->add(move(md)); };
    }

    lua_pushlightuserdata(*L, &dest);
    lua_pushcclosure(*L, arkilua_metadataconsumer, 1);

    // Call the function
    if (lua_pcall(*L, 2, 0, 0))
    {
        string error = lua_tostring(*L, -1);
        lua_pop(*L, 1);
        throw std::runtime_error("cannot run queryData function: " + error);
    }
}

void Querymacro::query_summary(const Matcher& matcher, Summary& summary)
{
	if (funcid_querysummary == -1) return;

	// Retrieve the Lua function registered for this
	lua_rawgeti(*L, LUA_REGISTRYINDEX, funcid_querysummary);

	// Pass matcher
	string m = matcher.toString();
	lua_pushstring(*L, m.c_str());

	// Pass summary
	summary.lua_push(*L);

    // Call the function
    if (lua_pcall(*L, 2, 0, 0))
    {
        string error = lua_tostring(*L, -1);
        lua_pop(*L, 1);
        throw std::runtime_error("cannot run querySummary function: " + error);
    }
}

#if 0
Querymacro::Func Querymacro::get(const std::string& def)
{
	std::map<std::string, int>::iterator i = ref_cache.find(def);

	if (i == ref_cache.end())
	{
		size_t pos = def.find(':');
		if (pos == string::npos)
			throw_consistency_error(
					"parsing targetfile definition \""+def+"\"",
					"definition not in the form type:parms");
		string type = def.substr(0, pos);
		string parms = def.substr(pos+1);

		// Get targetfile[type]
		lua_getglobal(*L, "targetfile");
		lua_pushlstring(*L, type.data(), type.size());
		lua_gettable(*L, -2);
		if (lua_type(*L, -1) == LUA_TNIL)
		{
			lua_pop(*L, 2);
			throw_consistency_error(
					"parsing targetfile definition \""+def+"\"",
					"no targetfile found of type \""+type+"\"");
		}

		// Call targetfile[type](parms)
		lua_pushlstring(*L, parms.data(), parms.size());
		if (lua_pcall(*L, 1, 1, 0))
		{
			string error = lua_tostring(*L, -1);
			lua_pop(*L, 2);
			throw_consistency_error(
					"creating targetfile function \""+def+"\"",
					error);
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

std::string Querymacro::Func::operator()(const Metadata& md)
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
		throw_consistency_error("running targetfile function", error);
	}

	string res = lua_tostring(*L, -1);
	lua_pop(*L, 1);
	return res;
}
#endif

}
// vim:set ts=4 sw=4:
