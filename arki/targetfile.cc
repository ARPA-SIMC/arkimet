/*
 * arki/targetfile - Compute file names out of metadata
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

#include <arki/targetfile.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/runtime/config.h>
#include <arki/runtime/io.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include "config.h"

using namespace std;
using namespace wibble;

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
			throw wibble::exception::Consistency("executing Lua code from memory", error);
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
			throw wibble::exception::Consistency("executing Lua file " + *i, error);
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
		throw wibble::exception::Consistency("running targetfile function", error);
	}

	string res = lua_tostring(*L, -1);
	lua_pop(*L, 1);
	return res;
}

TargetfileSpy::TargetfileSpy(ReadonlyDataset& ds, runtime::Output& output, const std::string& def)
	: func(Targetfile::instance().get(def)), ds(ds), output(output)
{
}

namespace {
struct MetadataSpy : public metadata::Consumer
{
	TargetfileSpy& tfs;
	metadata::Consumer& next;

	MetadataSpy(TargetfileSpy& tfs, metadata::Consumer& next) : tfs(tfs), next(next) {}

	bool operator()(Metadata& md)
	{
		tfs.redirect(md);
		return next(md);
	}
};
}

void TargetfileSpy::queryData(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
	MetadataSpy spy(*this, consumer);
	ds.queryData(q, spy);
}

void TargetfileSpy::querySummary(const Matcher& matcher, Summary& summary)
{
	ds.querySummary(matcher, summary);
}

void TargetfileSpy::redirect(Metadata& md)
{
	output.openFile(func(md), true);
}

}
// vim:set ts=4 sw=4:
