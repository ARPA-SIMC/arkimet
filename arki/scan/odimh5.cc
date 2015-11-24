/*
 * scan/odimh5 - Scan a ODIMH5 file for metadata
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Guido Billi <guidobilli@gmail.com>
 * Author: Enrico Zini <enrico@enricozini.org>
 */

#include "config.h"

#include <arki/scan/odimh5.h>

#include <arki/metadata.h>

#include <arki/types/origin.h>
#include <arki/types/reftime.h>
#include <arki/types/task.h>
#include <arki/types/quantity.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/area.h>
#include <arki/types/timerange.h>

#include <arki/runtime/config.h>

#include <wibble/exception.h>
#include <arki/utils/string.h>
#include <wibble/sys/fs.h>

#include <arki/utils/files.h>
#include <arki/utils/lua.h>
#include <arki/scan/any.h>

#include <cstring>
#include <unistd.h>
#include <fstream>
#include <vector>
#include <stdexcept>
#include <set>
#include <string>
#include <sstream>
#include <memory>
#include <iostream>

using namespace std;
using namespace arki::types;

namespace arki {
namespace scan {
namespace odimh5 {

/* taken from: http://www.hdfgroup.org/HDF5/doc/H5.format.html#Superblock */
static const unsigned char hdf5sign[8] = { 0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a };

struct OdimH5Validator : public Validator
{
	virtual ~OdimH5Validator() { }

	virtual void validate(int fd, off_t offset, size_t size, const std::string& fname) const
	{
		/* we check that file header is a valid HDF5 header */

		char buf[8];
		ssize_t res;

		if ((res = pread(fd, buf, 8, offset)) == -1)
			throw wibble::exception::System("reading 8 bytes of ODIMH5 header from " + fname);

		if (memcmp(buf, hdf5sign, 8) != 0)
			throw wibble::exception::Consistency("checking ODIMH5 file " + fname, "signature does not match with supposed value");
	}

	virtual void validate(const void* buf, size_t size) const
	{
		/* we check that file header is a valid HDF5 header */

		if (size < 8)
			throw wibble::exception::Consistency("checking ODIMH5 buffer", "buffer is shorter than 8 bytes");

		if (memcmp(buf, hdf5sign, 8) != 0)
			throw wibble::exception::Consistency("checking ODIMH5 buffer", "buffer does not start with hdf5 signature");
	}
};

static OdimH5Validator odimh5_validator;

const Validator& validator() { return odimh5_validator; }

}


struct OdimH5Lua : public Lua {
    OdimH5Lua(OdimH5* scanner) {
        // Create the metatable for the odimh5 object
        luaL_newmetatable(L, "odimh5");

        lua_pushvalue(L, -1);
        lua_setfield(L, -2, "__index"); // metatable.__index = metatable
        lua_pushcfunction(L, OdimH5::arkilua_find_attr);
        lua_setfield(L, -2, "find_attr");
        lua_pushcfunction(L, OdimH5::arkilua_get_groups);
        lua_setfield(L, -2, "get_groups");

        // The 'odimh5' object is a userdata that holds a pointer to this OdimH5 structure
        OdimH5** s = (OdimH5**)lua_newuserdata(L, sizeof(OdimH5*));
        *s = scanner;

        // Set the metatable for the userdata
        luaL_getmetatable(L, "odimh5");
        lua_setmetatable(L, -2);

        // Set the userdata object as the global 'odimh5' variable
        lua_setglobal(L, "odimh5");

        // Pop the metatable from the stack
        lua_pop(L, 1);
    }

    /**
     * Load scan function from file, returning the stored function ID
     *
     * It can return -1 if \a fname did not define a `scan' function. This
     * can happen, for example, if one adds a .lua file that only provides
     * a library of conveniency functions
     */
    int load_function(const std::string& fname) {
        // Compile the macro
        if (luaL_dofile(L, fname.c_str()))
        {
            // Copy the error, so that it will exist after the pop
            std::string error = lua_tostring(L, -1);
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

    /**
     * Load scanning functions
     */
    void load_scan_functions(const runtime::Config::Dirlist& src, std::vector<int>& ids)
    {
        std::vector<std::string> files = src.list_files(".lua");
        for (std::vector<std::string>::const_iterator i = files.begin(); i != files.end(); ++i)
        {
            int id = load_function(*i);
            if (id != -1) ids.push_back(id);
        }
    }

    /**
     * Run previously loaded scan function, passing the given metadata
     *
     * Return the error message, or an empty string if no error
     */
    std::string run_function(int id, Metadata& md) {
        // Retrieve the Lua function registered for this
        lua_rawgeti(L, LUA_REGISTRYINDEX, id);

        // Pass h5
        
        // Pass md
        md.lua_push(L);

        // Call the function
        if (lua_pcall(L, 1, 0, 0))
        {
            std::string error = lua_tostring(L, -1);
            lua_pop(L, 1);
            return error;
        } else
            return std::string();
    }
};

OdimH5::OdimH5()
    : h5file(-1), read(false), L(new OdimH5Lua(this))
{
    L->load_scan_functions(runtime::Config::get().dir_scan_odimh5, odimh5_funcs);
}

OdimH5::~OdimH5()
{
    if (h5file >= 0) H5Fclose(h5file);
}

void OdimH5::open(const std::string& filename)
{
    std::string basedir, relname;
    utils::files::resolve_path(filename, basedir, relname);
    open(wibble::sys::fs::abspath(filename), basedir, relname);
}

void OdimH5::open(const std::string& filename, const std::string& basedir, const std::string& relname)
{
    using namespace arki::utils::h5;

    // Close the previous file if needed
    close();
    this->filename = filename;
    this->basedir = basedir;
    this->relname = relname;

    // Open H5 file
    read = false;
    if (wibble::sys::fs::stat(filename)->st_size == 0) {
        // If the file is empty, don't open it
        read = true;
    } else {
        MuteErrors h5e;
        if ((h5file = H5Fopen(filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT)) < 0)
            // If the file is not HDF5, don't open it
            read = true;
            //h5e.throw_error("opening file " + filename);
    }
}

void OdimH5::close()
{
    filename.clear();
    basedir.clear();
    relname.clear();
    read = false;
    if (h5file >= 0)
    {
        H5Fclose(h5file);
        h5file = -1;
    }
}

bool OdimH5::next(Metadata& md)
{
    if (read) return false;
    md.clear();
    setSource(md);

    /* NOTA: per ora la next estrare un metadato unico per un intero file */
    try {
        scanLua(md);
    } catch (const std::exception& e) {
        throw wibble::exception::Consistency("while scanning file " + filename, e.what());
    }

    read = true;

    return true;
}

void OdimH5::setSource(Metadata& md)
{
	int length;
	/* for ODIMH5 files the source is the entire file */

	std::ifstream is;
	is.open(filename.c_str(), std::ios::binary );

	/* calcualte file size */
	is.seekg (0, std::ios::end);
	length = is.tellg();
	is.seekg (0, std::ios::beg);

    char* buff = (char*)malloc(length);
    try {
        is.read(buff,length);
        is.close();

        md.set_source(Source::createBlob("odimh5", basedir, relname, 0, length));
        md.set_cached_data(wibble::sys::Buffer(buff, length));
    } catch (...) {
        free(buff);
        throw;
    }

    md.add_note("Scanned from " + relname + ":0+" + wibble::str::fmt(length));
}

bool OdimH5::scanLua(Metadata& md)
{
    using namespace arki::utils::h5;
    MuteErrors h5e;
    for (std::vector<int>::const_iterator i = odimh5_funcs.begin();
         i != odimh5_funcs.end(); ++i) {
        std::string error = L->run_function(*i, md);
        if (!error.empty())
        {
            md.add_note("Scanning failed: " + error);
            return false;
        }
    }
    return true;
}

int OdimH5::arkilua_find_attr(lua_State* L)
{
    using namespace arki::utils::h5;

    luaL_checkudata(L, 1, "odimh5");
    void* userdata = lua_touserdata(L, 1);
    OdimH5& s = **(OdimH5**)userdata;

    if (s.h5file < 0)
        luaL_error(L, "h5 file not opened");

#define fail() do { lua_pushnil(L); return 1; } while(0)

    Group group(H5Gopen(s.h5file, luaL_checkstring(L, 2), H5P_DEFAULT));
    if (group < 0) fail();

    Attr attr(H5Aopen(group, luaL_checkstring(L, 3), H5P_DEFAULT));
    if (attr < 0) fail();

    Type data_type(H5Aget_type(attr));
    if (data_type < 0) fail();

    H5T_class_t type_class = H5Tget_class(data_type);
    if (type_class == H5T_NO_CLASS) fail();

    switch (type_class)
    {
        case H5T_INTEGER:
            {
                int v;
                if (H5Aread(attr, H5T_NATIVE_INT, &v) < 0) fail();
                lua_pushinteger(L, v);
                return 1;
            }
        case H5T_FLOAT:
            {
                double v;
                if (H5Aread(attr, H5T_NATIVE_DOUBLE, &v) < 0) fail();
                lua_pushnumber(L, v);
                return 1;
            }
        case H5T_STRING:
            {
                std::string v;
                if (!attr.read_string(v)) fail();
                lua_pushlstring(L, v.data(), v.size());
                return 1;
            }
        default:
            fail();
    }

    return 0;
#undef fail
}

int OdimH5::arkilua_get_groups(lua_State* L)
{
    using namespace arki::utils::h5;

    luaL_checkudata(L, 1, "odimh5");
    void* userdata = lua_touserdata(L, 1);
    OdimH5& s = **(OdimH5**)userdata;

    if (s.h5file < 0)
        luaL_error(L, "h5 file not opened");

    lua_newtable(L);

    Group group(H5Gopen(s.h5file, luaL_checkstring(L, 2), H5P_DEFAULT));
    if (group < 0) return 1;

    hsize_t num_objs;
    if (H5Gget_num_objs(group, &num_objs) < 0) return 1;

    const unsigned name_size = 512;
    char name[512];
    for (hsize_t i = 0; i < num_objs; ++i) {
        int type = H5Gget_objtype_by_idx(group, i);
        if (type < 0) continue;
        if (H5Gget_objname_by_idx(group, i, name, name_size) < 0) continue;
        if (type != H5G_GROUP) continue;
        lua_pushstring(L, "group");
        lua_setfield(L, -2, name);
    }

    return 1;
}

}
}

// vim:set ts=4 sw=4:
