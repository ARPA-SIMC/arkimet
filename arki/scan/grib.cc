#include "config.h"
#include "grib.h"
#include <grib_api.h>
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/types/source.h"
#include "arki/exceptions.h"
#include "arki/runtime.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/lua.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/scan/validator.h"
#include "arki/segment.h"
#include <system_error>
#include <cstring>
#include <unistd.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

#define check_grib_error(error, ...) do { \
        if (error != GRIB_SUCCESS) { \
            char buf[256]; \
            snprintf(buf, 256, __VA_ARGS__); \
            stringstream ss; \
            ss << buf << ": " << grib_get_error_message(error); \
            throw std::runtime_error(ss.str()); \
        } \
    } while (0)


namespace arki {
namespace scan {

namespace {

struct GribHandle
{
    grib_handle* gh = nullptr;

    GribHandle(grib_context* context, FILE* in)
    {
        int griberror;
        gh = grib_handle_new_from_file(context, in, &griberror);
        if (!gh && griberror == GRIB_END_OF_FILE)
            return;
        check_grib_error(griberror, "reading GRIB from file");
    }
    GribHandle(grib_handle* gh)
        : gh(gh) {}
    GribHandle(const GribHandle&) = delete;
    GribHandle(GribHandle&& o)
        : gh(o.gh)
    {
        o.gh = nullptr;
    }
    GribHandle& operator=(const GribHandle&) = delete;
    GribHandle&& operator=(GribHandle&&) = delete;
    ~GribHandle()
    {
        if (gh)
        {
            grib_handle_delete(gh);
            gh = nullptr;
        }
    }

    void close()
    {
        if (!gh) return;

        check_grib_error(grib_handle_delete(gh), "cannot close GRIB message");
        gh = nullptr;
    }

    operator grib_handle*() { return gh; }

    operator bool() const { return gh != nullptr; }
};

}


GribScanner::GribScanner()
{
    // Get a grib_api context
    context = grib_context_get_default();
    if (!context)
        throw std::runtime_error("cannot get grib_api default context: default context is not available");

    // Multi support is off unless a child class specifies otherwise
    grib_multi_support_off(context);
}

void GribScanner::set_source_blob(grib_handle* gh, std::shared_ptr<segment::Reader> reader, FILE* in, Metadata& md)
{
    // Get the encoded GRIB buffer from the GRIB handle
    const uint8_t* vbuf;
    size_t size;
    check_grib_error(grib_get_message(gh, (const void **)&vbuf, &size), "cannot access the encoded GRIB data");

#if 0
    // We cannot use this as long is too small for 64bit file offsets
    long offset;
    check_grib_error(grib_get_long(gh, "offset", &offset), "reading offset");
#endif
    off_t offset = ftello(in);
    offset -= size;

    md.set_source(Source::createBlob(reader, offset, size));
    md.set_cached_data(metadata::DataManager::get().to_data(reader->segment().format, vector<uint8_t>(vbuf, vbuf + size)));

    stringstream note;
    note << "Scanned from " << str::basename(reader->segment().relpath) << ":" << offset << "+" << size;
    md.add_note(note.str());
}

void GribScanner::set_source_inline(grib_handle* gh, Metadata& md)
{
    // Get the encoded GRIB buffer from the GRIB handle
    const uint8_t* vbuf;
    size_t size;
    check_grib_error(grib_get_message(gh, (const void **)&vbuf, &size), "cannot access the encoded GRIB data");
    md.set_source_inline("grib", metadata::DataManager::get().to_data("grib", vector<uint8_t>(vbuf, vbuf + size)));
}

std::unique_ptr<Metadata> GribScanner::scan_data(const std::vector<uint8_t>& data)
{
    GribHandle gh(grib_handle_new_from_message(context, (void*)data.data(), data.size()));
    if (!gh) throw std::runtime_error("GRIB memory buffer failed to scan");

    std::unique_ptr<Metadata> md(new Metadata);
    md->set_source_inline("grib", metadata::DataManager::get().to_data("grib", std::vector<uint8_t>(data)));

    scan(gh, *md);

    gh.close();

    return md;
}

bool GribScanner::scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest)
{
    files::RAIIFILE in(reader->segment().abspath, "rb");
    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        GribHandle gh(context, in);
        if (!gh) break;
        set_source_blob(gh, reader, in, *md);
        scan(gh, *md);
        gh.close();
        if (!dest(move(md))) return false;
    }
    return true;
}

void GribScanner::scan_singleton(const std::string& abspath, Metadata& md)
{
    files::RAIIFILE in(abspath, "rb");
    md.clear();
    {
        GribHandle gh(context, in);
        if (!gh) throw std::runtime_error(abspath + " contains no GRIB data");
        stringstream note;
        note << "Scanned from " << str::basename(abspath);
        md.add_note(note.str());
        scan(gh, md);
        gh.close();
    }

    {
        GribHandle gh(context, in);
        if (gh) throw std::runtime_error(abspath + " contains more than one GRIB data");
        gh.close();
    }
}

bool GribScanner::scan_pipe(core::NamedFileDescriptor& infd, metadata_dest_func dest)
{
    files::RAIIFILE in(infd, "rb");
    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        GribHandle gh(context, in);
        if (!gh) break;
        set_source_inline(gh, *md);
        stringstream note;
        note << "Scanned from standard input";
        md->add_note(note.str());
        scan(gh, *md);
        if (!dest(move(md))) return false;
    }
    return true;
}


namespace grib {

struct GribValidator : public Validator
{
    std::string format() const override { return "GRIB"; }

    // Validate data found in a file
    void validate_file(sys::NamedFileDescriptor& fd, off_t offset, size_t size) const override
    {
        if (size < 8)
            throw_check_error(fd, offset, "file segment to check is only " + std::to_string(size) + " bytes (minimum for a GRIB is 8)");

        char buf[4];
        ssize_t res = fd.pread(buf, 4, offset);
        if (res != 4)
            throw_check_error(fd, offset, "read only " + std::to_string(res) + "/4 bytes of GRIB header");
        if (memcmp(buf, "GRIB", 4) != 0)
            throw_check_error(fd, offset, "data does not start with 'GRIB'");
        res = fd.pread(buf, 4, offset + size - 4);
        if (res != 4)
            throw_check_error(fd, offset, "read only " + std::to_string(res) + "/4 bytes of GRIB trailer");
        if (memcmp(buf, "7777", 4) != 0)
            throw_check_error(fd, offset, "data does not end with '7777'");
    }

    // Validate a memory buffer
    void validate_buf(const void* buf, size_t size) const override
    {
        if (size < 8)
            throw_check_error("buffer is shorter than 8 bytes");
        if (memcmp(buf, "GRIB", 4) != 0)
            throw_check_error("buffer does not start with 'GRIB'");
        if (memcmp((const char*)buf + size - 4, "7777", 4) != 0)
            throw_check_error("buffer does not end with '7777'");
    }
};

static GribValidator grib_validator;

const Validator& validator() { return grib_validator; }

}


class GribLua : public Lua
{
private:
    std::vector<int> grib1_funcs;
    std::vector<int> grib2_funcs;

    /**
     * Load scan function from file, returning the stored function ID
     *
     * It can return -1 if \a fname did not define a `scan' function. This
     * can happen, for example, if one adds a .lua file that only provides
     * a library of conveniency functions
     */
    int load_function(const std::string& fname)
    {
        // Compile the macro
        if (luaL_dofile(L, fname.c_str()))
        {
            // Copy the error, so that it will exist after the pop
            string error = lua_tostring(L, -1);
            // Pop the error from the stack
            lua_pop(L, 1);
            throw std::runtime_error("cannot parse " + fname + ": " + error);
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
     * Same as load_function, but reads Lua code from a string instead of a
     * file. The file name is still provided as a description of the code,
     * to use in error messages.
     */
    int load_function(const std::string& fname, const std::string& code)
    {
        // Compile the macro
        if (luaL_loadbuffer(L, code.data(), code.size(), fname.c_str()))
        {
            // Copy the error, so that it will exist after the pop
            string error = lua_tostring(L, -1);
            // Pop the error from the stack
            lua_pop(L, 1);
            throw std::runtime_error("cannot parse " + fname + ": " + error);
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
     * Run previously loaded scan function, passing the given metadata
     *
     * Return the error message, or an empty string if no error
     */
    std::string run_function(int id, Metadata& md)
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

    /**
     * If code is non-empty, load scan code from it. Else, load scan code
     * from configuration files. Store scan function IDs in \a ids
     */
    void load_scan_code(const std::string& code, const Config::Dirlist& src, vector<int>& ids)
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

    /// Run Lua scanning functions on \a md
    bool scanLua(std::vector<int> ids, Metadata& md)
    {
        for (vector<int>::const_iterator i = ids.begin(); i != ids.end(); ++i)
        {
            string error = run_function(*i, md);
            if (!error.empty())
            {
                md.add_note("Scanning failed: " + error);
                return false;
            }
        }
        return true;
    }

    /// Read GRIB1 data from the currently open handle into md
    void scanGrib1(Metadata& md)
    {
        scanLua(grib1_funcs, md);
    }

    /// Read GRIB2 data from the currently open handle into md
    void scanGrib2(Metadata& md)
    {
        scanLua(grib2_funcs, md);
    }

    static int arkilua_lookup_grib(lua_State* L);
    static int arkilua_lookup_gribl(lua_State* L);
    static int arkilua_lookup_gribs(lua_State* L);
    static int arkilua_lookup_gribd(lua_State* L);

    void make_index_userdata(const char* tname, int (*lookup)(lua_State*))
    {
        // Create the metatable for the grib object
        luaL_newmetatable(L, tname);

        // Set the __index metamethod to the arkilua_lookup_grib function
        lua_pushstring(L, "__index");
        lua_pushcfunction(L, lookup);
        lua_settable(L, -3);

        // The 'grib' object is a userdata that holds a pointer to this Grib structure
        GribLua** s = (GribLua**)lua_newuserdata(L, sizeof(GribLua*));
        *s = this;

        // Set the metatable for the userdata
        luaL_getmetatable(L, tname);
        lua_setmetatable(L, -2);

        // Set the userdata object as the global 'grib' variable
        lua_setglobal(L, tname);

        // Pop the metatable from the stack
        lua_pop(L, 1);
    }

public:
    grib_handle* gh = nullptr;

    /**
     * Create the 'grib' object in the interpreter, to access the grib data of
     * the current grib read by the scanner
     */
    GribLua(const std::string& grib1code, const std::string& grib2code)
    {
        make_index_userdata("grib", GribLua::arkilua_lookup_grib);
        make_index_userdata("gribl", GribLua::arkilua_lookup_gribl);
        make_index_userdata("gribs", GribLua::arkilua_lookup_gribs);
        make_index_userdata("gribd", GribLua::arkilua_lookup_gribd);

        /// Load the grib1 scanning functions
        load_scan_code(grib1code, Config::get().dir_scan_grib1, grib1_funcs);

        /// Load the grib2 scanning functions
        load_scan_code(grib2code, Config::get().dir_scan_grib2, grib2_funcs);

        //arkilua_dumpstack(L, "Afterinit", stderr);
    }

    void scan_handle(grib_handle* gh, Metadata& md)
    {
        // Set this here so the Lua code can find it
        // Do not bother resetting it, as long as this function is the only
        // entry point into the class
        this->gh = gh;
        long edition;
        check_grib_error(grib_get_long(gh, "editionNumber", &edition), "reading edition number");
        switch (edition)
        {
            case 1: scanGrib1(md); break;
            case 2: scanGrib2(md); break;
            default:
                    {
                stringstream ss;
                ss << "cannot read grib message: GRIB edition " << edition << " is not supported";
                throw std::runtime_error(ss.str());
            }
        }
    }
};


// Never returns in case of error
#define arkilua_check_gribapi(L, error, ...) do { \
        if (error != GRIB_SUCCESS) {\
            char buf[256]; \
            snprintf(buf, 256, __VA_ARGS__); \
            luaL_error(L, "grib_api error \"%s\" while %s", \
                        grib_get_error_message(error), buf); \
        } \
    } while (0)


static GribLua& get_griblua(lua_State* L, int arg, const char* tname)
{
    luaL_checkudata(L, arg, tname);
    void* userdata = lua_touserdata(L, arg);
    return **(GribLua**)userdata;
}

// Lookup a grib value for grib.<fieldname>
int GribLua::arkilua_lookup_grib(lua_State* L)
{
    // Fetch the Scanner reference from the userdata value
    GribLua& s = get_griblua(L, 1, "grib");
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
int GribLua::arkilua_lookup_gribl(lua_State* L)
{
    GribLua& s = get_griblua(L, 1, "gribl");
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
int GribLua::arkilua_lookup_gribs(lua_State* L)
{
    GribLua& s = get_griblua(L, 1, "gribs");
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
int GribLua::arkilua_lookup_gribd(lua_State* L)
{
    GribLua& s = get_griblua(L, 1, "gribd");
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


Grib::Grib(const std::string& grib1code, const std::string& grib2code)
    : L(new GribLua(grib1code, grib2code))
{
}

Grib::~Grib()
{
    if (L) delete L;
}

void Grib::scan(grib_handle* gh, Metadata& md)
{
    L->scan_handle(gh, md);
}

}
}
