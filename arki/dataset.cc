/*
 * dataset - Handle arkimet datasets
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"
#include <arki/dataset.h>
#include <arki/configfile.h>
#include <arki/dataset/file.h>
#include <arki/dataset/ondisk.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/outbound.h>
#include <arki/dataset/discard.h>
#include <arki/dataset/empty.h>
#include <arki/metadata.h>
#include <arki/sort.h>
#include <arki/types/assigneddataset.h>
#include <arki/utils.h>
#include <arki/utils/dataset.h>
#include <arki/postprocess.h>
#include <arki/report.h>
#include <arki/summary.h>

#include <wibble/exception.h>
#include <wibble/string.h>

#ifdef HAVE_LIBCURL
#include <arki/dataset/http.h>
#endif

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

using namespace std;
using namespace wibble;

namespace arki {

void WritableDataset::flush() {}

void WritableDataset::repack(std::ostream& log, bool writable) {}
void WritableDataset::check(std::ostream& log, bool fix, bool quick) {}

void WritableDataset::remove(Metadata& md)
{
	Item<types::AssignedDataset> ds = md.get(types::TYPE_ASSIGNEDDATASET).upcast<types::AssignedDataset>();
	if (!ds.defined())
		throw wibble::exception::Consistency("removing metadata from dataset", "the metadata is not assigned to this dataset");

	remove(ds->id);

	// reset source and dataset in the metadata
	md.source.clear();
	md.unset(types::TYPE_ASSIGNEDDATASET);
}

void ReadonlyDataset::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	using namespace arki::utils;

	switch (q.type)
	{
		case dataset::ByteQuery::BQ_DATA: {
			ds::DataOnly dataonly(out);
			queryData(q, dataonly);
			break;
		}
		case dataset::ByteQuery::BQ_POSTPROCESS: {
			Postprocess postproc(q.param, out, cfg);
			queryData(q, postproc);
			postproc.flush();
			break;
		}
		case dataset::ByteQuery::BQ_REP_METADATA: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(q.param);
			queryData(q, rep);
			rep.report();
#endif
			break;
		}
		case dataset::ByteQuery::BQ_REP_SUMMARY: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(q.param);
			Summary s;
			querySummary(q.matcher, s);
			rep(s);
			rep.report();
#endif
			break;
		}
		default:
			throw wibble::exception::Consistency("querying dataset", "unsupported query type: " + str::fmt((int)q.type));
	}
}

#ifdef HAVE_LUA

#if 0
int ReadonlyDataset::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Summary reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_readonlydataset");
	void* userdata = lua_touserdata(L, udataidx);
	const Summary& v = **(const Summary**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "count")
	{
		lua_pushinteger(L, v.count());
		return 1;
	}
	else if (name == "size")
	{
		lua_pushinteger(L, v.size());
		return 1;
	}
	else if (name == "data")
	{
		// Return a big table with a dump of the summary inside
		lua_newtable(L);
		if (v.root.ptr())
		{
			summary::LuaPusher pusher(L);
			vector< UItem<> > visitmd(summary::msoSize, UItem<>());
			v.root->visit(pusher, visitmd);
		}
		return 1;
	}
#if 0
	else if (name == "iter")
	{
		// Iterate

		/* create a userdatum to store an iterator structure address */
		summary::LuaIter**d = (summary::LuaIter**)lua_newuserdata(L, sizeof(summary::LuaIter*));

		// Get the metatable for the iterator
		if (luaL_newmetatable(L, "arki_summary_iter"));
		{
			/* set its __gc field */
			lua_pushstring(L, "__gc");
			lua_pushcfunction(L, summary::LuaIter::gc);
			lua_settable(L, -3);
		}

		// Set its metatable
		lua_setmetatable(L, -2);

		// Instantiate the iterator
		*d = new summary::LuaIter(v);

		// Creates and returns the iterator function (its sole upvalue, the
		// iterator userdatum, is already on the stack top
		lua_pushcclosure(L, summary::LuaIter::iterate, 1);
		return 1;
	}
#endif
	else
	{
		lua_pushnil(L);
		return 1;
	}
	return 0;
}
static int arkilua_lookup_readonlydataset(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, ReadonlyDataset::lua_lookup, 2);
	return 1;
}
#endif

static ReadonlyDataset* checkrodataset(lua_State *L)
{
	void* ud = luaL_checkudata(L, 1, "arki.rodataset");
	luaL_argcheck(L, ud != NULL, 1, "`rodataset' expected");
	return *(ReadonlyDataset**)ud;
}

namespace {
struct LuaMetadataConsumer : public MetadataConsumer
{
	lua_State* L;
	int funcid;

	LuaMetadataConsumer(lua_State* L, int funcid) : L(L), funcid(funcid) {}
	virtual ~LuaMetadataConsumer() {}

	virtual bool operator()(Metadata& md)
	{
		// Get the function
		lua_rawgeti(L, LUA_REGISTRYINDEX, funcid);

		// Push the metadata
		md.lua_push(L);

		// Call the function
		if (lua_pcall(L, 1, 1, 0))
		{
			string error = lua_tostring(L, -1);
			lua_pop(L, 1);
			throw wibble::exception::Consistency("running targetfile function", error);
		}

		int res = lua_toboolean(L, -1);
		lua_pop(L, 1);
		return res;
	}
};

}

static int arkilua_queryData(lua_State *L)
{
	// queryData(self, { matcher="", withdata=false, sorter="" }, consumer_func)
	ReadonlyDataset* rd = checkrodataset(L);
	luaL_argcheck(L, lua_istable(L, 2), 2, "`table' expected");
	luaL_argcheck(L, lua_isfunction(L, 3), 3, "`function' expected");

	// Create a DataQuery with data from the table
	dataset::DataQuery dq;
	lua_pushstring(L, "matcher");
	lua_gettable(L, 2);
	const char* matcher = lua_tostring(L, -1);
	lua_pop(L, 1);
	if (matcher) dq.matcher = Matcher::parse(matcher);

	lua_pushstring(L, "withdata");
	lua_gettable(L, 2);
	dq.withData = lua_toboolean(L, -1);
	lua_pop(L, 1);

	lua_pushstring(L, "sorter");
	lua_gettable(L, 2);
	const char* sorter = lua_tostring(L, -1);
	lua_pop(L, 1);
	auto_ptr<sort::Compare> compare;
	if (sorter)
	{
		compare = sort::Compare::parse(sorter);
		dq.sorter = compare.get();
	}

	// Ref the created function into the registry
	lua_pushvalue(L, 3);
	int funcid = luaL_ref(L, LUA_REGISTRYINDEX);

	// Create a consumer using the function
	LuaMetadataConsumer mdc(L, funcid);

	// Run the query
	rd->queryData(dq, mdc);
	
	// Unindex the function
	luaL_unref(L, LUA_REGISTRYINDEX, funcid);

	// lua_pushnumber(L, 5);
	// return 1;
	return 0;
}

static int arkilua_querySummary(lua_State *L)
{
	// TODO: add querySummary(self, matcher="", summary)
	ReadonlyDataset* rd = checkrodataset(L);
	const char* matcher = luaL_checkstring(L, 2);
	luaL_argcheck(L, matcher != NULL, 2, "`string' expected");
	// lua_pushnumber(L, 5);
	// return 1;
	return 0;
}

static const struct luaL_reg readonlydatasetlib [] = {
	{ "queryData", arkilua_queryData },
	{ "querySummary", arkilua_querySummary },
	{NULL, NULL}
};

void ReadonlyDataset::lua_push(lua_State* L)
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	ReadonlyDataset** s = (ReadonlyDataset**)lua_newuserdata(L, sizeof(ReadonlyDataset*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki.rodataset"));
	{
		// If the metatable wasn't previously created, create it now
		lua_pushstring(L, "__index");
		lua_pushvalue(L, -2);  /* pushes the metatable */
		lua_settable(L, -3);  /* metatable.__index = metatable */

		// Load normal methods
		luaL_register(L, NULL, readonlydatasetlib);
	}

	lua_setmetatable(L, -2);
}
#endif


ReadonlyDataset* ReadonlyDataset::create(const ConfigFile& cfg)
{
	string type = wibble::str::tolower(cfg.value("type"));
	if (type.empty())
		type = "local";
	
	if (type == "local" || type == "test" || type == "error" || type == "duplicates")
		return new dataset::ondisk::Reader(cfg);
	if (type == "ondisk2")
		return new dataset::ondisk2::Reader(cfg);
#ifdef HAVE_LIBCURL
	if (type == "remote")
		return new dataset::HTTP(cfg);
#endif
	if (type == "outbound")
		return new dataset::Empty(cfg);
	if (type == "discard")
		return new dataset::Empty(cfg);
	if (type == "file")
		return dataset::File::create(cfg);

	throw wibble::exception::Consistency("creating a dataset", "unknown dataset type \""+type+"\"");
}

void ReadonlyDataset::readConfig(const std::string& path, ConfigFile& cfg)
{
#ifdef HAVE_LIBCURL
	if (str::startsWith(path, "http://") || str::startsWith(path, "https://"))
	{
		return dataset::HTTP::readConfig(path, cfg);
	} else
#endif
	if (utils::isdir(path))
		return dataset::Local::readConfig(path, cfg);
	else
		return dataset::File::readConfig(path, cfg);
}

WritableDataset* WritableDataset::create(const ConfigFile& cfg)
{
	string type = wibble::str::tolower(cfg.value("type"));
	if (type.empty())
		type = "local";
	
	if (type == "local" || type == "test" || type == "error" || type == "duplicates")
		return new dataset::ondisk::Writer(cfg);
	if (type == "ondisk2")
		return new dataset::ondisk2::Writer(cfg);
	if (type == "remote")
		throw wibble::exception::Consistency("creating a dataset", "remote datasets are not writable");
	if (type == "outbound")
		return new dataset::Outbound(cfg);
	if (type == "discard")
		return new dataset::Discard(cfg);

	throw wibble::exception::Consistency("creating a dataset", "unknown dataset type \""+type+"\"");
}

WritableDataset::AcquireResult WritableDataset::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	string type = wibble::str::tolower(cfg.value("type"));
	if (type.empty())
		type = "local";
	
	if (type == "local" || type == "test" || type == "error" || type == "duplicates")
		return dataset::ondisk::Writer::testAcquire(cfg, md, out);
	if (type == "ondisk2")
		return dataset::ondisk2::Writer::testAcquire(cfg, md, out);
	if (type == "remote")
		throw wibble::exception::Consistency("simulating dataset acquisition", "remote datasets are not writable");
	if (type == "outbound")
		return dataset::Outbound::testAcquire(cfg, md, out);
	if (type == "discard")
		return dataset::Discard::testAcquire(cfg, md, out);

	throw wibble::exception::Consistency("simulating dataset acquisition", "unknown dataset type \""+type+"\"");
}

}
// vim:set ts=4 sw=4:
