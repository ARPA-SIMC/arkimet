/*
 * arki/report - Build a report of an arkimet metadata or summary stream
 *
 * Copyright (C) 2008--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/report.h>
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/utils/lua.h>
#include <arki/runtime/config.h>
#include <wibble/regexp.h>
#include <wibble/sys/fs.h>

using namespace std;
using namespace wibble;

namespace arki {

Report::Report(const std::string& params) : L(new Lua), m_accepts_metadata(false), m_accepts_summary(false)
{
	if (!params.empty())
		load(params);
}

Report::~Report()
{
	if (L) delete L;
}

void Report::load(const std::string& params)
{
	// Parse params into its components
	Splitter sp("[[:space:]]*,[[:space:]]*|[[:space:]]+", REG_EXTENDED);
	vector<string> args;
	for (Splitter::const_iterator j = sp.begin(params); j != sp.end(); ++j)
		args.push_back(*j);

	if (args.empty())
		throw wibble::exception::Consistency("initialising report", "report command is empty");

    // Look for the script in all report paths
    std::string filename;
    const runtime::Config& conf = runtime::Config::get();
    for (vector<string>::const_iterator i = conf.dir_report.begin();
            i != conf.dir_report.end(); ++i)
    {
        filename = str::joinpath(*i, args[0]);
        if (sys::fs::exists(filename))
            break;
    }
    loadFile(filename);
}

void Report::loadFile(const std::string& fname)
{
	/// Load the report function
	m_filename = fname;

	if (luaL_loadfile(*L, fname.c_str()))
		throw wibble::exception::Consistency("parsing Lua code", lua_tostring(*L, -1));

	create_lua_object();
}

void Report::loadString(const std::string& buf)
{
	m_filename = "memory buffer";

	if (luaL_loadbuffer(*L, buf.data(), buf.size(), m_filename.c_str()))
		throw wibble::exception::Consistency("parsing Lua code", lua_tostring(*L, -1));

	create_lua_object();
}

void Report::create_lua_object()
{
	if (lua_pcall(*L, 0, 0, 0))
	{
		string error = lua_tostring(*L, -1);
		lua_pop(*L, 1);
		throw wibble::exception::Consistency(error, "defining report functions");
	}

	// ensure that there is a Report variable
    lua_getglobal(*L, "Report");
	int type = lua_type(*L, -1);
	if (type == LUA_TNIL)
		throw wibble::exception::Consistency(
			"loading report code from " + m_filename,
			"code did not define a variable called Report");
	if (type != LUA_TTABLE)
		throw wibble::exception::Consistency(
			"loading report code from " + m_filename,
			"the Report variable is not a table");

	// Get the readMetadata function
	lua_pushstring(*L, "readMetadata");
	lua_gettable(*L, -2);
	if (lua_type(*L, -1) == LUA_TNIL)
	{
		m_accepts_metadata = false;
		lua_pop(*L, 1);
	} else {
		m_accepts_metadata = true;
		lua_setglobal(*L, "_md");
	}

	// Get the readSummary function
	lua_pushstring(*L, "readSummary");
	lua_gettable(*L, -2);
	if (lua_type(*L, -1) == LUA_TNIL)
	{
		m_accepts_summary = false;
		lua_pop(*L, 1);
	} else {
		m_accepts_summary = true;
		lua_setglobal(*L, "_su");
	}

	// Get the report function
	lua_pushstring(*L, "report");
	lua_gettable(*L, -2);
	if (lua_type(*L, -1) == LUA_TNIL)
		throw wibble::exception::Consistency(
			"loading report code from " + m_filename,
			"Report.report is nil");
	lua_setglobal(*L, "_re");

	// Pop the Report table from the stack
	lua_pop(*L, 1);

	//utils::lua::dumpstack(L, "Afterinit", stderr);
}

void Report::captureOutput(std::ostream& buf)
{
	utils::lua::capturePrintOutput(*L, buf);
}

bool Report::operator()(Metadata& md)
{
	if (!acceptsMetadata()) return true;

    lua_getglobal(*L, "_md");
    lua_getglobal(*L, "Report");
	md.lua_push(*L);
	if (lua_pcall(*L, 2, 0, 0))
	{
		string error = lua_tostring(*L, -1);
		lua_pop(*L, 1);
		throw wibble::exception::Consistency(error, "processing metadata");
	}
	return true;
}

bool Report::operator()(Summary& s)
{
	if (!acceptsSummary()) return true;

    lua_getglobal(*L, "_su");
    lua_getglobal(*L, "Report");
	s.lua_push(*L);
	if (lua_pcall(*L, 2, 0, 0))
	{
		string error = lua_tostring(*L, -1);
		lua_pop(*L, 1);
		throw wibble::exception::Consistency(error, "processing summary");
	}
	return true;
}

void Report::report()
{
    lua_getglobal(*L, "_re");
    lua_getglobal(*L, "Report");
	if (lua_pcall(*L, 1, 0, 0))
	{
		string error = lua_tostring(*L, -1);
		lua_pop(*L, 1);
		throw wibble::exception::Consistency(error, "generating report");
	}
}

}
// vim:set ts=4 sw=4:
