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
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/simple/writer.h>
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
ReadonlyDataset* ReadonlyDataset::lua_check(lua_State* L, int idx)
{
	return *(ReadonlyDataset**)luaL_checkudata(L, idx, "arki.rodataset");
}

namespace dataset {
auto_ptr<sort::Compare> DataQuery::lua_from_table(lua_State* L, int idx)
{
	lua_pushstring(L, "matcher");
	lua_gettable(L, 2);
	matcher = Matcher::lua_check(L, -1);
	lua_pop(L, 1);

	lua_pushstring(L, "withdata");
	lua_gettable(L, 2);
	withData = lua_toboolean(L, -1);
	lua_pop(L, 1);

	lua_pushstring(L, "sorter");
	lua_gettable(L, 2);
	const char* str_sorter = lua_tostring(L, -1);
	lua_pop(L, 1);
	auto_ptr<sort::Compare> compare;
	if (str_sorter)
	{
		compare = sort::Compare::parse(str_sorter);
		sorter = compare.get();
	}
	return compare;
}

void DataQuery::lua_push_table(lua_State* L, int idx) const
{
	string str;

	if (idx < 0) idx -= 2;

	// table["matcher"] = this->matcher
	lua_pushstring(L, "matcher");
	str = matcher.toString();
	lua_pushstring(L, str.c_str());
	lua_settable(L, idx);

	// table["withdata"] = this->withData
	lua_pushstring(L, "withdata");
	lua_pushboolean(L, withData);
	lua_settable(L, idx);

	// table["sorter"] = this->sorter
	lua_pushstring(L, "sorter");
	if (sorter)
	{
		str = sorter->toString();
		lua_pushstring(L, str.c_str());
	}
	else
	{
		lua_pushnil(L);
	}
	lua_settable(L, idx);
}

}


static int arkilua_queryData(lua_State *L)
{
	// queryData(self, { matcher="", withdata=false, sorter="" }, consumer_func)
	ReadonlyDataset* rd = ReadonlyDataset::lua_check(L, 1);
	luaL_argcheck(L, lua_istable(L, 2), 2, "`table' expected");
	luaL_argcheck(L, lua_isfunction(L, 3), 3, "`function' expected");

	// Create a DataQuery with data from the table
	dataset::DataQuery dq;
	auto_ptr<sort::Compare> compare = dq.lua_from_table(L, 2);

	// Create metadata consumer proxy
	std::auto_ptr<LuaMetadataConsumer> mdc = LuaMetadataConsumer::lua_check(L, 3);

	// Run the query
	rd->queryData(dq, *mdc);

	return 0;
}

static int arkilua_querySummary(lua_State *L)
{
	// querySummary(self, matcher="", summary)
	ReadonlyDataset* rd = ReadonlyDataset::lua_check(L, 1);
	Matcher matcher = Matcher::lua_check(L, 2);
	Summary* sum = Summary::lua_check(L, 3);
	luaL_argcheck(L, sum != NULL, 3, "`arki.summary' expected");
	rd->querySummary(matcher, *sum);
	return 0;
}

static int arkilua_tostring(lua_State *L)
{
	lua_pushstring(L, "dataset");
	return 1;
}

static const struct luaL_reg readonlydatasetlib [] = {
	{ "queryData", arkilua_queryData },
	{ "querySummary", arkilua_querySummary },
	{ "__tostring", arkilua_tostring },
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
	
	if (type == "ondisk2")
		return new dataset::ondisk2::Reader(cfg);
	if (type == "simple" || type == "error" || type == "duplicates")
		return new dataset::simple::Reader(cfg);
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
	
	if (type == "ondisk2")
		return new dataset::ondisk2::Writer(cfg);
	if (type == "simple" || type == "error" || type == "duplicates")
		return new dataset::simple::Writer(cfg);
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
	
	if (type == "ondisk2")
		return dataset::ondisk2::Writer::testAcquire(cfg, md, out);
	if (type == "simple" || type == "error" || type == "duplicates")
		return dataset::simple::Writer::testAcquire(cfg, md, out);
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
