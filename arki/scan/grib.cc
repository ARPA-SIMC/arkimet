/*
 * scan/grib - Scan a GRIB (version 1 or 2) file for metadata
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include "config.h"
#include <grib_api.h>
#include <arki/metadata.h>
#include <arki/runtime/config.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <arki/utils/lua.h>
#include <arki/scan/any.h>
#include <cstring>
#include <unistd.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace scan {

namespace grib {

struct GribValidator : public Validator
{
	virtual ~GribValidator() {}

	// Validate data found in a file
	virtual void validate(int fd, off64_t offset, size_t size, const std::string& fname) const
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
	GribLua(Grib* scanner);

	/**
	 * Load scan function from file, returning the stored function ID
	 *
	 * It can return -1 if \a fname did not define a `scan' function. This
	 * can happen, for example, if one adds a .lua file that only provides
	 * a library of conveniency functions
	 */
	int load_function(const std::string& fname);

	/**
	 * Same as load_function, but reads Lua code from a string instead of a
	 * file. The file name is still provided as a description of the code,
	 * to use in error messages.
	 */
	int load_function(const std::string& fname, const std::string& code);

	/**
	 * Run previously loaded scan function, passing the given metadata
	 *
	 * Return the error message, or an empty string if no error
	 */
	std::string run_function(int id, Metadata& md);

    /**
     * If code is non-empty, load scan code from it. Else, load scan code
     * from configuration files. Store scan function IDs in \a ids
     */
    void load_scan_code(const std::string& code, const runtime::Config::Dirlist& src, vector<int>& ids)
    {
		/// Load the grib1 scanning functions
		if (!code.empty())
		{
			int id = load_function("grib scan instructions", code);
			if (id != -1) ids.push_back(id);
			return;
		}
        vector<string> files = src.list_files(".lua");
        for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
        {
            int id = load_function(*i);
            if (id != -1) ids.push_back(id);
        }
    }
};

GribLua::GribLua(Grib* scanner)
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

int GribLua::load_function(const std::string& fname)
{
	// Compile the macro
        if (luaL_dofile(L, fname.c_str()))
        {
                // Copy the error, so that it will exist after the pop
                string error = lua_tostring(L, -1);
                // Pop the error from the stack
                lua_pop(L, 1);
                throw wibble::exception::Consistency("parsing " + fname, error);
        }

	// Index the scan function
	int id = -1;
        lua_getglobal(L, "scan");
        if (lua_isfunction(L, -1))
                id = luaL_ref(L, LUA_REGISTRYINDEX);

	// Return the ID, or -1 if no 'scan' function was defined
	return id;
}

int GribLua::load_function(const std::string& fname, const std::string& code)
{
	// Compile the macro
	if (luaL_loadbuffer(L, code.data(), code.size(), fname.c_str()))
        {
                // Copy the error, so that it will exist after the pop
                string error = lua_tostring(L, -1);
                // Pop the error from the stack
                lua_pop(L, 1);
                throw wibble::exception::Consistency("parsing " + fname, error);
        }

	// Index the scan function
	int id = -1;
        lua_getglobal(L, "scan");
        if (lua_isfunction(L, -1))
                id = luaL_ref(L, LUA_REGISTRYINDEX);

	// Return the ID, or -1 if no 'scan' function was defined
	return id;
}

string GribLua::run_function(int id, Metadata& md)
{
        // Retrieve the Lua function registered for this
        lua_rawgeti(L, LUA_REGISTRYINDEX, id);

	// Pass md
	md.lua_push(L);

        // Call the function
        if (lua_pcall(L, 1, 0, 0))
        {
                string error = lua_tostring(L, -1);
                lua_pop(L, 1);
		return error;
	} else
		return std::string();
}

#define check_grib_error(error, ...) do { \
        if (error != GRIB_SUCCESS) \
            throw wibble::exception::Consistency(str::fmtf(__VA_ARGS__), grib_get_error_message(error)); \
    } while (0)

// Never returns in case of error
#define arkilua_check_gribapi(L, error, ...) do { \
        if (error != GRIB_SUCCESS) \
            luaL_error(L, "grib_api error \"%s\" while %s", \
                        grib_get_error_message(error), str::fmtf(__VA_ARGS__).c_str()); \
    } while (0)

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
		arkilua_check_gribapi(L, res, "getting type of key %s", name);

	// Look up the value and push the function result for lua
	switch (type)
	{
		case GRIB_TYPE_LONG: {
			long val;
			arkilua_check_gribapi(L, grib_get_long(gh, name, &val), "reading long value %s", name);
			lua_pushnumber(L, val);
			break;
		}
		case GRIB_TYPE_DOUBLE: {
			double val;
			arkilua_check_gribapi(L, grib_get_double(gh, name, &val), "reading double value %s", name);
			lua_pushnumber(L, val);
			break;
		} 
		case GRIB_TYPE_STRING: {
			const int maxsize = 1000;
			char buf[maxsize];
			size_t len = maxsize;
			arkilua_check_gribapi(L, grib_get_string(gh, name, buf, &len), "reading string value %s", name);
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
	: in(0), context(0), gh(0), L(new GribLua(this)), m_inline_data(inlineData)
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

    /// Load the grib1 scanning functions
    L->load_scan_code(grib1code, runtime::Config::get().dir_scan_grib1, grib1_funcs);

    /// Load the grib2 scanning functions
    L->load_scan_code(grib2code, runtime::Config::get().dir_scan_grib2, grib2_funcs);

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
		md.setCachedData(wibble::sys::Buffer(vbuf, size));
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

bool Grib::scanLua(std::vector<int> ids, Metadata& md)
{
	for (vector<int>::const_iterator i = ids.begin(); i != ids.end(); ++i)
	{
		string error = L->run_function(*i, md);
		if (!error.empty())
		{
			md.add_note(types::Note::create("Scanning failed: " + error));
			return false;
		}
	}
	return true;
}


void Grib::scanGrib1(Metadata& md)
{
	scanLua(grib1_funcs, md);
}

void Grib::scanGrib2(Metadata& md)
{
	scanLua(grib2_funcs, md);
}

}
}
// vim:set ts=4 sw=4:
