/*
 * arki/querymacro - Macros implementing special query strategies
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/querymacro.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/runtime/config.h>
#include <arki/runtime/io.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include "config.h"

using namespace std;
using namespace wibble;

namespace arki {

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

static Querymacro* checkqmacro(lua_State *L)
{
	void* ud = luaL_checkudata(L, 1, "arki.querymacro");
	luaL_argcheck(L, ud != NULL, 1, "`querymacro' expected");
	return *(Querymacro**)ud;
}

static int arkilua_dataset(lua_State *L)
{
	Querymacro* rd = checkqmacro(L);
	const char* name = luaL_checkstring(L, 2);
	ReadonlyDataset* ds = rd->dataset(name);
	if (ds) 
	{
		ds->lua_push(L);
		return 1;
	} else {
		lua_pushnil(L);
		return 1;
	}
}

static const struct luaL_reg querymacrolib [] = {
	// TODO: add newsummary()
	{ "dataset", arkilua_dataset },
	// TODO: add onQueryData(func)
	// TODO: add onQuerySummary(func)
	//{ "queryData", arkilua_queryData },
	//{ "querySummary", arkilua_querySummary },
	{NULL, NULL}
};

Querymacro::Querymacro(const ConfigFile& cfg, const std::string& name, const std::string& data)
	: cfg(cfg), L(new Lua)
{
	// Create the Querymacro object
	Querymacro** s = (Querymacro**)lua_newuserdata(*L, sizeof(Querymacro*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(*L, "arki.querymacro"));
	{
		// If the metatable wasn't previously created, create it now
		lua_pushstring(*L, "__index");
		lua_pushvalue(*L, -2);  /* pushes the metatable */
		lua_settable(*L, -3);  /* metatable.__index = metatable */

		// Load normal methods
		luaL_register(*L, NULL, querymacrolib);
	}

	lua_setmetatable(*L, -2);

	// global qmacro = our userdata object
	lua_setglobal(*L, "qmacro");

	// Load the data as a global variable
	lua_pushstring(*L, data.c_str());
	lua_setglobal(*L, "data");
	
	/// Load the right qmacro file
	string dirname = runtime::rcDirName("qmacro", "ARKI_QMACRO");
	string fname = str::joinpath(dirname, name + ".lua");
	if (luaL_dofile(*L, fname.c_str()))
	{
		// Copy the error, so that it will exist after the pop
		string error = lua_tostring(*L, -1);
		// Pop the error from the stack
		lua_pop(*L, 1);
		throw wibble::exception::Consistency("parsing " + fname, error);
	}
		
	//arkilua_dumpstack(L, "Afterinit", stderr);
}

Querymacro::~Querymacro()
{
	if (L) delete L;
	for (std::map<std::string, ReadonlyDataset*>::iterator i = ds_cache.begin();
			i != ds_cache.end(); ++i)
		delete i->second;
}

ReadonlyDataset* Querymacro::dataset(const std::string& name)
{
	std::map<std::string, ReadonlyDataset*>::iterator i = ds_cache.find(name);
	if (i == ds_cache.end())
	{
		ConfigFile* dscfg = cfg.section(name);
		if (!dscfg) return 0;
		ReadonlyDataset* ds = ReadonlyDataset::create(*dscfg);
		pair<map<string, ReadonlyDataset*>::iterator, bool> res = ds_cache.insert(make_pair(name, ds));
		i = res.first;
	}
	return i->second;
}

void Querymacro::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
}

void Querymacro::querySummary(const Matcher& matcher, Summary& summary)
{
}

#if 0
Querymacro::Func Querymacro::get(const std::string& def)
{
	std::map<std::string, int>::iterator i = ref_cache.find(def);

	if (i == ref_cache.end())
	{
		size_t pos = def.find(':');
		if (pos == string::npos)
			throw wibble::exception::Consistency(
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
			throw wibble::exception::Consistency(
					"parsing targetfile definition \""+def+"\"",
					"no targetfile found of type \""+type+"\"");
		}

		// Call targetfile[type](parms)
		lua_pushlstring(*L, parms.data(), parms.size());
		if (lua_pcall(*L, 1, 1, 0))
		{
			string error = lua_tostring(*L, -1);
			lua_pop(*L, 2);
			throw wibble::exception::Consistency(
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
		throw wibble::exception::Consistency("running targetfile function", error);
	}

	string res = lua_tostring(*L, -1);
	lua_pop(*L, 1);
	return res;
}
#endif

}
// vim:set ts=4 sw=4:
