/*
 * scan/grib - Scan a GRIB file for metadata.
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

#include "grib.h"
#include <grib_api.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/ensemble.h>
#include <arki/types/run.h>
#include <arki/runtime/config.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <arki/utils/lua.h>
#include <arki/scan/any.h>
#include <cstring>

using namespace std;
using namespace wibble;

namespace arki {
namespace scan {

namespace grib {

struct GribValidator : public Validator
{
	virtual ~GribValidator() {}

	// Validate data found in a file
	virtual void validate(int fd, off_t offset, size_t size, const std::string& fname) const
	{
		char buf[4];
		ssize_t res;
		if ((res = pread(fd, buf, 4, offset)) == -1)
			throw wibble::exception::System("reading 4 bytes of GRIB header from " + fname);
		if (res != 4)
			throw wibble::exception::Consistency("reading 4 bytes of GRIB header from " + fname, "partial read");
		if (memcmp(buf, "GRIB", 4) != 0)
			throw wibble::exception::Consistency("checking GRIB segment in file " + fname, "segment does not start with 'GRIB'");
		if ((res = pread(fd, buf, 4, offset + size - 4)) == -1)
			throw wibble::exception::System("reading 4 bytes of GRIB trailer from " + fname);
		if (res != 4)
			throw wibble::exception::Consistency("reading 4 bytes of GRIB trailer from " + fname, "partial read");
		if (memcmp(buf, "7777", 4) != 0)
			throw wibble::exception::Consistency("checking GRIB segment in file " + fname, "segment does not end with '7777'");
	}

	// Validate a memory buffer
	virtual void validate(const void* buf, size_t size) const
	{
		if (size < 8)
			throw wibble::exception::Consistency("checking GRIB buffer", "buffer is shorter than 8 bytes");
		if (memcmp(buf, "GRIB", 4) != 0)
			throw wibble::exception::Consistency("checking GRIB buffer", "buffer does not start with 'GRIB'");
		if (memcmp((const char*)buf + size - 4, "7777", 4) != 0)
			throw wibble::exception::Consistency("checking GRIB buffer", "buffer does not end with '7777'");
	}
};

static GribValidator grib_validator;

const Validator& validator() { return grib_validator; }

}

struct GribLua : public Lua
{
	/**
	 * Create the 'grib' object in the interpreter, to access the grib data of
	 * the current grib read by the scanner
	 */
	void makeGribObject(Grib* scanner);

	/**
	 * Set the 'arki' global variable to nil
	 */
	void resetArkiObject();
};

void GribLua::makeGribObject(Grib* scanner)
{
	/// Create the 'grib' Lua object

	// Create the metatable for the grib object
	luaL_newmetatable(L, "grib");

	// Set the __index metamethod to the arkilua_lookup_grib function
	lua_pushstring(L, "__index");
	lua_pushcfunction(L, Grib::arkilua_lookup_grib);
	lua_settable(L, -3);

	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	Grib** s = (Grib**)lua_newuserdata(L, sizeof(Grib*));
	*s = scanner;

	// Set the metatable for the userdata
	luaL_getmetatable(L, "grib");
	lua_setmetatable(L, -2);

	// Set the userdata object as the global 'grib' variable
	lua_setglobal(L, "grib");

	// Pop the metatable from the stack
	lua_pop(L, 1);


	/// Create the 'gribl' Lua object

	// Create the metatable for the grib object
	luaL_newmetatable(L, "gribl");

	// Set the __index metamethod to the arkilua_lookup_grib function
	lua_pushstring(L, "__index");
	lua_pushcfunction(L, Grib::arkilua_lookup_gribl);
	lua_settable(L, -3);

	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	s = (Grib**)lua_newuserdata(L, sizeof(Grib*));
	*s = scanner;

	// Set the metatable for the userdata
	luaL_getmetatable(L, "gribl");
	lua_setmetatable(L, -2);

	// Set the userdata object as the global 'grib' variable
	lua_setglobal(L, "gribl");

	// Pop the metatable from the stack
	lua_pop(L, 1);


	/// Create the 'gribs' Lua object

	// Create the metatable for the grib object
	luaL_newmetatable(L, "gribs");

	// Set the __index metamethod to the arkilua_lookup_grib function
	lua_pushstring(L, "__index");
	lua_pushcfunction(L, Grib::arkilua_lookup_gribs);
	lua_settable(L, -3);

	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	s = (Grib**)lua_newuserdata(L, sizeof(Grib*));
	*s = scanner;

	// Set the metatable for the userdata
	luaL_getmetatable(L, "gribs");
	lua_setmetatable(L, -2);

	// Set the userdata object as the global 'grib' variable
	lua_setglobal(L, "gribs");

	// Pop the metatable from the stack
	lua_pop(L, 1);


	/// Create the 'gribd' Lua object

	// Create the metatable for the grib object
	luaL_newmetatable(L, "gribd");

	// Set the __index metamethod to the arkilua_lookup_grib function
	lua_pushstring(L, "__index");
	lua_pushcfunction(L, Grib::arkilua_lookup_gribd);
	lua_settable(L, -3);

	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	s = (Grib**)lua_newuserdata(L, sizeof(Grib*));
	*s = scanner;

	// Set the metatable for the userdata
	luaL_getmetatable(L, "gribd");
	lua_setmetatable(L, -2);

	// Set the userdata object as the global 'grib' variable
	lua_setglobal(L, "gribd");

	// Pop the metatable from the stack
	lua_pop(L, 1);
}

void GribLua::resetArkiObject()
{
    // arki = { area = {}, ensemble = {} }

	/// Create/recreate the 'arki' Lua object
	lua_newtable(L);

	// One table to store the area information
	lua_pushstring(L, "area");
	lua_newtable(L);
	lua_rawset(L, -3);

	// One table to store the ensemble information
	lua_pushstring(L, "ensemble");
	lua_newtable(L);
	lua_rawset(L, -3);

	lua_setglobal(L, "arki");
}

static void check_grib_error(int error, const char* context)
{
	if (error != GRIB_SUCCESS)
		throw wibble::exception::Consistency(context, grib_get_error_message(error));
}

// Never returns in case of error
static void arkilua_check_gribapi(lua_State* L, int error, const char* context)
{
	if (error != GRIB_SUCCESS)
		luaL_error(L, "grib_api error \"%s\" while %s",
					grib_get_error_message(error), context);
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

struct TableAccess
{
	lua_State* L;
	int index;
	string fname;

	// Index must be positive, as we push things in the stack and a negative
	// index would become invalid
	TableAccess(lua_State* L, int index, const std::string& fname) : L(L), index(index), fname(fname) {}

	bool has(const std::string& name)
	{
		lua_pushlstring(L, name.data(), name.size());
		lua_rawget(L, index);
		int type = lua_type(L, -1);
		lua_pop(L, 1);
		return type != LUA_TNIL;
	}

	// Get a number from the table
	lua_Number num(const std::string& name)
	{
		lua_pushlstring(L, name.data(), name.size());
		lua_rawget(L, index);
		int type = lua_type(L, -1);
		switch (type)
		{
			case LUA_TNIL:
				lua_pop(L, 1);
				throw wibble::exception::Consistency("reading values from " + fname, "key \"" + name + "\" has not been set");
			case LUA_TNUMBER: {
				lua_Number num = lua_tonumber(L, -1);
				lua_pop(L, 1);
				return num;
			}
			default:
				lua_pop(L, 1);
				throw wibble::exception::Consistency("reading values from " + fname, "key \"" + name + "\" has type " + lua_typename(L, type) + " instead of number");
		}
	}

	ValueBag table(const std::string& name)
	{
		lua_pushlstring(L, name.data(), name.size());
		lua_rawget(L, index);
		int type = lua_type(L, -1);
		switch (type)
		{
			case LUA_TNIL:
				lua_pop(L, 1);
				throw wibble::exception::Consistency("reading values from " + fname, "key " + name + " has not been set");
			case LUA_TTABLE: {
				ValueBag res;
				// Start iteration
				lua_pushnil(L);
				while (lua_next(L, -2) != 0) {
					// key is at index -2 and we want it to be a string
					if (lua_type(L, -2) != LUA_TSTRING)
					{
						lua_pop(L, 3);
						throw wibble::exception::Consistency("reading values from " + fname, "value " + name + " contains a non-string key");
					}
					string key = lua_tostring(L, -2);

					// value is at index -1 and we want it to be a number
					int type = lua_type(L, -1);
					switch (type)
					{
						case LUA_TNIL:
							// Skip keys with nil values
							lua_pop(L, 1);
							continue;
						case LUA_TNUMBER: {
							lua_Number val = lua_tonumber(L, -1);
							res.set(key, Value::createInteger(val));
							// removes 'value'; keeps 'key' for next iteration
							lua_pop(L, 1);
							break;
						}
						case LUA_TSTRING: {
							const char* val = lua_tostring(L, 3);
							res.set(key, Value::createString(val));
							// removes 'value'; keeps 'key' for next iteration
							lua_pop(L, 1);
							break;
						}
						default:
							lua_pop(L, 3);
							throw wibble::exception::Consistency("reading values from " + fname, "value " + name + "." + key + " has type " + lua_typename(L, type) + " instead of number");
					}
				}
				lua_pop(L, 1);
				return res;
			}
			default:
				lua_pop(L, 1);
				throw wibble::exception::Consistency("reading values from " + fname, "key " + name + " has type " + lua_typename(L, type) + " instead of table");
		}
	}
};

// Lookup a grib value for grib.<fieldname>
int Grib::arkilua_lookup_grib(lua_State* L)
{
	// Fetch the Scanner reference from the userdata value
	luaL_checkudata(L, 1, "grib");
	void* userdata = lua_touserdata(L, 1);
	Grib& s = **(Grib**)userdata;
	grib_handle* gh = s.gh;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, 2);
	const char* name = lua_tostring(L, 2);

	// Get information about the type
	int type;
	int res = grib_get_native_type(gh, name, &type);
	if (res == GRIB_NOT_FOUND)
		type = GRIB_TYPE_MISSING;
	else
		arkilua_check_gribapi(L, res, "getting type of key");

	// Look up the value and push the function result for lua
	switch (type)
	{
		case GRIB_TYPE_LONG: {
			long val;
			arkilua_check_gribapi(L, grib_get_long(gh, name, &val), "reading long value");
			lua_pushnumber(L, val);
			break;
		}
		case GRIB_TYPE_DOUBLE: {
			double val;
			arkilua_check_gribapi(L, grib_get_double(gh, name, &val), "reading double value");
			lua_pushnumber(L, val);
			break;
		} 
		case GRIB_TYPE_STRING: {
			const int maxsize = 1000;
			char buf[maxsize];
			size_t len = maxsize;
			arkilua_check_gribapi(L, grib_get_string(gh, name, buf, &len), "reading string value");
			if (len > 0) --len;
			lua_pushlstring(L, buf, len);
			break;
		} 
		default:
		    lua_pushnil(L);
			break;
	}

	// Number of values we're returning to lua
	return 1;
}

// Lookup a grib value for grib.<fieldname>
int Grib::arkilua_lookup_gribl(lua_State* L)
{
	// Fetch the Scanner reference from the userdata value
	luaL_checkudata(L, 1, "gribl");
	void* userdata = lua_touserdata(L, 1);
	Grib& s = **(Grib**)userdata;
	grib_handle* gh = s.gh;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, 2);
	const char* name = lua_tostring(L, 2);

	// Push the function result for lua
	long val;
	int res = grib_get_long(gh, name, &val);
	if (res == GRIB_NOT_FOUND)
	{
		lua_pushnil(L);
	} else {
		arkilua_check_gribapi(L, res, "reading long value");
		lua_pushnumber(L, val);
	}

	// Number of values we're returning to lua
	return 1;
}

// Lookup a grib value for grib.<fieldname>
int Grib::arkilua_lookup_gribs(lua_State* L)
{
	// Fetch the Scanner reference from the userdata value
	luaL_checkudata(L, 1, "gribs");
	void* userdata = lua_touserdata(L, 1);
	Grib& s = **(Grib**)userdata;
	grib_handle* gh = s.gh;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, 2);
	const char* name = lua_tostring(L, 2);

	// Push the function result for lua
	const int maxsize = 1000;
	char buf[maxsize];
	size_t len = maxsize;
	int res = grib_get_string(gh, name, buf, &len);
	if (res == GRIB_NOT_FOUND)
	{
		lua_pushnil(L);
	} else {
		arkilua_check_gribapi(L, res, "reading string value");
		if (len > 0) --len;
		lua_pushlstring(L, buf, len);
	}

	// Number of values we're returning to lua
	return 1;
}

// Lookup a grib value for grib.<fieldname>
int Grib::arkilua_lookup_gribd(lua_State* L)
{
	// Fetch the Scanner reference from the userdata value
	luaL_checkudata(L, 1, "gribd");
	void* userdata = lua_touserdata(L, 1);
	Grib& s = **(Grib**)userdata;
	grib_handle* gh = s.gh;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, 2);
	const char* name = lua_tostring(L, 2);

	// Push the function result for lua
	double val;
	int res = grib_get_double(gh, name, &val);
	if (res == GRIB_NOT_FOUND)
	{
		lua_pushnil(L);
	} else {
		arkilua_check_gribapi(L, res, "reading double value");
		lua_pushnumber(L, val);
	}

	// Number of values we're returning to lua
	return 1;
}

Grib::Grib(bool inlineData, const std::string& grib1code, const std::string& grib2code)
	: in(0), context(0), gh(0), L(new GribLua), m_inline_data(inlineData), grib1FuncCount(0), grib2FuncCount(0)
{
	// Get a grib_api context
	context = grib_context_get_default();
	if (!context)
		throw wibble::exception::Consistency("getting grib_api default context", "default context is not available");

	if (m_inline_data)
	{
		// If we inline the data, we can also do multigrib
		grib_multi_support_on(context);
	} else {
		// Multi support is off unless a child class specifies otherwise
		grib_multi_support_off(context);
	}

	/// Create the 'grib' Lua object
	L->makeGribObject(this);

	/// Load the grib1 scanning functions
	if (grib1code.empty())
	{
		vector<string> files = runtime::rcFiles("scan-grib1", "ARKI_SCAN_GRIB1");
		for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
			L->functionFromFile("GRIB1_" + str::fmt(grib1FuncCount++), *i);
	} else
		L->functionFromString("GRIB1_" + str::fmt(grib1FuncCount++), grib1code, "grib1 scan instructions");

	/// Load the grib2 scanning functions
	if (grib2code.empty())
	{
		vector<string> files = runtime::rcFiles("scan-grib2", "ARKI_SCAN_GRIB2");
		for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
			L->functionFromFile("GRIB2_" + str::fmt(grib2FuncCount++), *i);
	} else
		L->functionFromString("GRIB2_" + str::fmt(grib1FuncCount++), grib2code, "grib2 scan instructions");

	//arkilua_dumpstack(L, "Afterinit", stderr);
}

Grib::~Grib()
{
	if (gh) check_grib_error(grib_handle_delete(gh), "closing GRIB message");
	if (in) fclose(in);
	if (L) delete L;
}

MultiGrib::MultiGrib(const std::string& tmpfilename, std::ostream& tmpfile)
	: tmpfilename(tmpfilename), tmpfile(tmpfile)
{
	// Turn on multigrib support: we can handle them
	grib_multi_support_on(context);
}


void Grib::open(const std::string& filename)
{
	// Close the previous file if needed
	close();
	this->filename = sys::fs::abspath(filename);
	this->basename = str::basename(filename);
	if (!(in = fopen(filename.c_str(), "rb")))
		throw wibble::exception::File(filename, "opening file for reading");
}

void Grib::close()
{
	filename.clear();
	if (in)
	{
		fclose(in);
		in = 0;
	}
}

bool Grib::next(Metadata& md)
{
	int griberror;

	// If there's still a grib_handle around (for example, if a previous call
	// to next() threw an exception), deallocate it here to prevent a leak
	if (gh)
	{
		check_grib_error(grib_handle_delete(gh), "closing GRIB message");
		gh = 0;
	}

	gh = grib_handle_new_from_file(context, in, &griberror);
	if (gh == 0 && griberror == GRIB_END_OF_FILE)
		return false;
	check_grib_error(griberror, "reading GRIB from file");
	if (gh == 0)
		return false;

	md.create();

	setSource(md);

	long edition;
	check_grib_error(grib_get_long(gh, "editionNumber", &edition), "reading edition number");
	switch (edition)
	{
		case 1: scanGrib1(md); break;
		case 2: scanGrib2(md); break;
		default:
			throw wibble::exception::Consistency("reading grib message", "GRIB edition " + str::fmt(edition) + " is not supported");
	}

	check_grib_error(grib_handle_delete(gh), "closing GRIB message");
	gh = 0;

	return true;
}

void Grib::setSource(Metadata& md)
{
	long edition;
	check_grib_error(grib_get_long(gh, "editionNumber", &edition), "reading edition number");

	// Get the encoded GRIB buffer from the GRIB handle
	const void* vbuf;
	size_t size;
	check_grib_error(grib_get_message(gh, &vbuf, &size), "accessing the encoded GRIB data");

	// Get the position in the file of the en of the grib
	long offset;
	check_grib_error(grib_get_long(gh, "offset", &offset), "reading offset");

	if (m_inline_data)
	{
		md.setInlineData("grib" + str::fmt(edition), wibble::sys::Buffer(vbuf, size));
	}
	else
	{
		md.source = types::source::Blob::create("grib" + str::fmt(edition), filename, offset, size);
	}
	md.add_note(types::Note::create("Scanned from " + basename + ":" + str::fmt(offset) + "+" + str::fmt(size)));
}

void MultiGrib::setSource(Metadata& md)
{
	long edition;
	check_grib_error(grib_get_long(gh, "editionNumber", &edition), "reading edition number");

	// Get the encoded GRIB buffer from the GRIB handle
	const void* vbuf;
	size_t size;
	check_grib_error(grib_get_message(gh, &vbuf, &size), "accessing the encoded GRIB data");
	const char* buf = static_cast<const char*>(vbuf);

	// Get the write position in the file
	size_t offset = tmpfile.tellp();
	if (tmpfile.fail())
		throw wibble::exception::File(tmpfilename, "reading the current position");

	// Write the data
	tmpfile.write(buf, size);
	if (tmpfile.fail())
		throw wibble::exception::File(tmpfilename, "writing " + str::fmt(size) + " bytes to the file");

	tmpfile.flush();

	md.source = types::source::Blob::create("grib" + str::fmt(edition), tmpfilename, offset, size);
}

void Grib::scanGrib1(Metadata& md)
{
	L->resetArkiObject();
	std::string error = L->runFunctionSequence("GRIB1_", grib1FuncCount);
	if (!error.empty())
	{
		md.add_note(types::Note::create("Scanning GRIB1 failed: " + error));
	} else {
		//arkilua_dumpstack(L, "Afterscan", stderr);

		// Access the arki objext
		lua_getglobal(*L, "arki");
		TableAccess t(*L, 1, filename);
		md.set(types::reftime::Position::create(new types::Time(t.num("year"), t.num("month"), t.num("day"),
							t.num("hour"), t.num("minute"), t.num("second"))));
		md.set(types::origin::GRIB1::create(t.num("centre"), t.num("subcentre"), t.num("process")));
		md.set(types::product::GRIB1::create(t.num("centre"), t.num("table"), t.num("product")));
		if (t.has("l2"))
			md.set(types::level::GRIB1::create(t.num("ltype"), t.num("l1"), t.num("l2")));
		else
			md.set(types::level::GRIB1::create(t.num("ltype"), t.num("l1")));
		md.set(types::timerange::GRIB1::create(t.num("ptype"), t.num("punit"), t.num("p1"), t.num("p2")));
		ValueBag table = t.table("area");
		if (!table.empty())
			md.set(types::area::GRIB::create(table));
		table = t.table("ensemble");
		if (!table.empty())
			md.set(types::ensemble::GRIB::create(table));

		if (t.has("run_hour"))
		{
			unsigned hour = t.num("run_hour");
			unsigned min = t.has("run_minute") ? t.num("run_minute") : 0;
			md.set(types::run::Minute::create(hour, min));
		}

		lua_pop(*L, 1);
	}
}

void Grib::scanGrib2(Metadata& md)
{
	L->resetArkiObject();
	std::string error = L->runFunctionSequence("GRIB2_", grib2FuncCount);
	if (!error.empty())
	{
		md.add_note(types::Note::create("Scanning GRIB2 failed: " + error));
	} else {
		// Access the arki object
		lua_getglobal(*L, "arki");
		TableAccess t(*L, 1, filename);
		md.set(types::reftime::Position::create(new types::Time(t.num("year"), t.num("month"), t.num("day"),
							t.num("hour"), t.num("minute"), t.num("second"))));

		md.set(types::origin::GRIB2::create(t.num("centre"), t.num("subcentre"), t.num("processtype"), t.num("bgprocessid"), t.num("processid")));
		md.set(types::product::GRIB2::create(t.num("centre"), t.num("discipline"), t.num("category"), t.num("number")));
		if (t.has("ltype2"))
			md.set(types::level::GRIB2D::create(t.num("ltype1"), t.num("lscale1"), t.num("lvalue1"),
			                                    t.num("ltype2"), t.num("lscale2"), t.num("lvalue2")));
		else
			md.set(types::level::GRIB2S::create(t.num("ltype1"), t.num("lscale1"), t.num("lvalue1")));

		md.set(types::timerange::GRIB2::create(t.num("ptype"), t.num("punit"), t.num("p1"), t.num("p2")));

		ValueBag table = t.table("area");
		if (!table.empty())
			md.set(types::area::GRIB::create(table));
		table = t.table("ensemble");
		if (!table.empty())
			md.set(types::ensemble::GRIB::create(table));

		if (t.has("run_hour"))
		{
			unsigned hour = t.num("run_hour");
			unsigned min = t.has("run_minute") ? t.num("run_minute") : 0;
			md.set(types::run::Minute::create(hour, min));
		}

		lua_pop(*L, 1);
	}
}

}
}
// vim:set ts=4 sw=4:
