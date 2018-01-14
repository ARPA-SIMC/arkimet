#include "config.h"
#include "core/fwd.h"
#include "types.h"
#include "types/utils.h"
#include "binary.h"
#include "utils/sys.h"
#include "utils/string.h"
#include "emitter.h"
#include "emitter/memory.h"
#include "formatter.h"
#include <cstring>
#include <unistd.h>
#include <algorithm>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

using namespace std;
using namespace arki::utils;
using namespace arki::core;

namespace arki {
namespace types {

Code checkCodeName(const std::string& name)
{
    // TODO: convert into something faster, like a hash lookup or a gperf lookup
    string nname = str::strip(str::lower(name));
	if (nname == "time") return TYPE_TIME;
	if (nname == "origin") return TYPE_ORIGIN;
	if (nname == "product") return TYPE_PRODUCT;
	if (nname == "level") return TYPE_LEVEL;
	if (nname == "timerange") return TYPE_TIMERANGE;
	if (nname == "reftime") return TYPE_REFTIME;
	if (nname == "note") return TYPE_NOTE;
	if (nname == "source") return TYPE_SOURCE;
	if (nname == "assigneddataset") return TYPE_ASSIGNEDDATASET;
	if (nname == "area") return TYPE_AREA;
	if (nname == "proddef") return TYPE_PRODDEF;
	if (nname == "summaryitem") return TYPE_SUMMARYITEM;
	if (nname == "summarystats") return TYPE_SUMMARYSTATS;
	if (nname == "bbox") return TYPE_BBOX;
	if (nname == "run") return TYPE_RUN;
	if (nname == "task") 		return TYPE_TASK;
	if (nname == "quantity") 	return TYPE_QUANTITY;
	if (nname == "value") 	return TYPE_VALUE;
	return TYPE_INVALID;
}

Code parseCodeName(const std::string& name)
{
    Code res = checkCodeName(name);
    if (res == TYPE_INVALID)
    {
        string msg("cannot parse yaml data: unsupported field type: ");
        msg += name;
        throw std::runtime_error(std::move(msg));
    }
    return res;
}

std::string formatCode(const Code& c)
{
	switch (c)
	{
		case TYPE_ORIGIN: return "ORIGIN";
		case TYPE_PRODUCT: return "PRODUCT";
		case TYPE_LEVEL: return "LEVEL";
		case TYPE_TIMERANGE: return "TIMERANGE";
		case TYPE_REFTIME: return "REFTIME";
		case TYPE_NOTE: return "NOTE";
		case TYPE_SOURCE: return "SOURCE";
		case TYPE_ASSIGNEDDATASET: return "ASSIGNEDDATASET";
		case TYPE_AREA: return "AREA";
		case TYPE_PRODDEF: return "PRODDEF";
		case TYPE_SUMMARYITEM: return "SUMMARYITEM";
		case TYPE_SUMMARYSTATS: return "SUMMARYSTATS";
		case TYPE_BBOX: return "BBOX";
		case TYPE_RUN: return "RUN";
		case TYPE_TASK:	return "TASK";
		case TYPE_QUANTITY:	return "QUANTITY";
		case TYPE_VALUE:	return "VALUE";
        default: {
            stringstream res;
            res << "unknown(" << (int)c << ")";
            return res.str();
        }
    }
}

int Type::compare(const Type& o) const
{
	return type_code() - o.type_code();
}

bool Type::operator==(const std::string& o) const
{
    // Temporarily decode it for comparison
    std::unique_ptr<Type> other = decodeString(type_code(), o);
    return operator==(*other);
}

std::string Type::to_string() const
{
    stringstream ss;
    writeToOstream(ss);
    return ss.str();
}


void Type::encodeBinary(BinaryEncoder& enc) const
{
    vector<uint8_t> contents;
    contents.reserve(256);
    BinaryEncoder contentsenc(contents);
    encodeWithoutEnvelope(contentsenc);

    enc.add_varint((unsigned)type_code());
    enc.add_varint(contents.size());
    enc.add_raw(contents);
}

std::vector<uint8_t> Type::encodeBinary() const
{
    vector<uint8_t> contents;
    BinaryEncoder enc(contents);
    encodeBinary(enc);
    return contents;
}

void Type::serialise(Emitter& e, const Formatter* f) const
{
    e.start_mapping();
    e.add("t", tag());
    if (f) e.add("desc", (*f)(*this));
    serialiseLocal(e, f);
    e.end_mapping();
}

std::string Type::exactQuery() const
{
    stringstream ss;
    ss << "creating a query to match " << tag() << " is not implemented";
    return ss.str();
}

#ifdef HAVE_LUA
static Type* lua_check_arkitype(lua_State* L, int idx, const char* prefix)
{
    // Tweaked version of luaL_checkudata from lauxlib
    void *p = lua_touserdata(L, idx);
	// Value is a userdata?
	if (p != NULL)
	{
		// Does it have a metatable?
		if (lua_getmetatable(L, idx))
		{
			// Get the arkimet type tag
			lua_pushstring(L, "__arki_type");
			lua_gettable(L, -2);
			size_t len;
			const char* arkitype = lua_tolstring(L, -1, &len);

			// Verify we are the right type base
			int pfxlen = strlen(prefix);
			if (arkitype != NULL
			 && strncmp(arkitype, prefix, pfxlen) == 0
			 && (arkitype[pfxlen] == 0 || arkitype[pfxlen] == '.'))
            {
                // remove metatable and type name */
                lua_pop(L, 2);
                return *(Type**)p;
            }
		}
	}
	// TODO: luaL_typerror(L, idx, "arki.types.*");
	// discontinued in Lua 5.2
	luaL_error(L, "arki.types.*");
	return 0; // to avoid warnings
}
static int arkilua_tostring(lua_State* L)
{
    Type* item = Type::lua_check(L, 1);
    std::stringstream str;
    item->writeToOstream(str);
    lua_pushstring(L, str.str().c_str());
    return 1;
}
static int arkilua_eq(lua_State* L)
{
    Type* item1 = Type::lua_check(L, 1);
    Type* item2 = Type::lua_check(L, 2);
    lua_pushboolean(L, item1->equals(*item2));
    return 1;
}
static int arkilua_lt(lua_State* L)
{
    Type* item1 = Type::lua_check(L, 1);
    Type* item2 = Type::lua_check(L, 2);
    lua_pushboolean(L, item1->compare(*item2) < 0);
    return 1;
}
static int arkilua_le(lua_State* L)
{
    Type* item1 = Type::lua_check(L, 1);
    Type* item2 = Type::lua_check(L, 2);
    lua_pushboolean(L, item1->compare(*item2) <= 0);
    return 1;
}
static int arkilua_index(lua_State* L)
{
    Type* item = Type::lua_check(L, 1);
    const char* sname = lua_tostring(L, 2);
    luaL_argcheck(L, sname != NULL, 2, "`string' expected");
    string key = sname;

	if (!item->lua_lookup(L, key))
	{
		// Nothing found, delegate lookup to the metatable
		lua_getmetatable(L, 1);
		lua_pushlstring(L, key.data(), key.size());
		lua_gettable(L, -2);
	}
	// utils::lua::dumpstack(L, "postlookup", cerr);
	return 1;
}

static int arkilua_gc (lua_State *L)
{
    Type* ud = lua_check_arkitype(L, 1, "arki.types");
    delete ud;
    return 0;
}

void Type::lua_register_methods(lua_State* L) const
{
	static const struct luaL_Reg lib [] = {
		{ "__index", arkilua_index },
		{ "__tostring", arkilua_tostring },
		{ "__gc", arkilua_gc },
		{ "__eq", arkilua_eq },
		{ "__lt", arkilua_lt },
		{ "__le", arkilua_le },
		{ NULL, NULL }
	};
    utils::lua::add_functions(L, lib);

	// Set the type name in a special field in the metatable
	lua_pushstring(L, "__arki_type");
	lua_pushstring(L, lua_type_name());
	lua_settable(L, -3);  /* metatable.__arki_type = <name> */
}

bool Type::lua_lookup(lua_State* L, const std::string& name) const
{
	return false;
}

void Type::lua_push(lua_State* L) const
{
    // The userdata will be a Type*, holding a clone of this object
    Type** s = (Type**)lua_newuserdata(L, sizeof(Type*));
    *s = this->clone();

    // Set the metatable for the userdata
    if (luaL_newmetatable(L, lua_type_name()))
    {
        // If the metatable wasn't previously created, create it now
        lua_pushstring(L, "__index");
        lua_pushvalue(L, -2);  /* pushes the metatable */
        lua_settable(L, -3);  /* metatable.__index = metatable */

        lua_register_methods(L);
    }

    lua_setmetatable(L, -2);
}

Type* Type::lua_check(lua_State* L, int idx, const char* prefix)
{
    return lua_check_arkitype(L, idx, prefix);
}

void Type::lua_loadlib(lua_State* L)
{
	MetadataType::lua_loadlib(L);
}
#endif

unique_ptr<Type> decode(BinaryDecoder& dec)
{
    types::Code code;
    BinaryDecoder inner = dec.pop_type_envelope(code);
    return decodeInner(code, inner);
}

unique_ptr<Type> decodeInner(types::Code code, BinaryDecoder& dec)
{
    return types::MetadataType::get(code)->decode_func(dec);
}

unique_ptr<Type> decodeString(types::Code code, const std::string& val)
{
	return types::MetadataType::get(code)->string_decode_func(val);
}

unique_ptr<Type> decodeMapping(const emitter::memory::Mapping& m)
{
    using namespace emitter::memory;
    const std::string& type = m["t"].want_string("decoding item type");
    return decodeMapping(parseCodeName(type), m);
}

unique_ptr<Type> decodeMapping(types::Code code, const emitter::memory::Mapping& m)
{
    using namespace emitter::memory;
    return types::MetadataType::get(code)->mapping_decode_func(m);
}

std::string tag(types::Code code)
{
    return types::MetadataType::get(code)->tag;
}

bool Bundle::read_header(NamedFileDescriptor& fd)
{
    // Skip all leading blank bytes
    char c;
    while (true)
    {
        int res = fd.read(&c, 1);
        if (res == 0) return false; // EOF
        if (c) break;
    }

    // Read the rest of the first 8 bytes
    unsigned char hdr[8];
    hdr[0] = c;
    size_t res = fd.read(hdr + 1, 7);
    if (res < 7) return false; // EOF

    BinaryDecoder dec(hdr, 8);

    // Read the signature
    signature = dec.pop_string(2, "header of metadata bundle");

    // Get the version in next 2 bytes
    version = dec.pop_uint(2, "version of metadata bundle");

    // Get length from next 4 bytes
    length = dec.pop_uint(4, "size of metadata bundle");

    return true;
}

bool Bundle::read_data(NamedFileDescriptor& fd)
{
    // Use reserve, then read a bit at a time, resizing appropriately, to avoid
    // allocating and using (which triggers the kernel to actually allocate
    // memory pages) potentially a lot of ram, and then read fails right away
    // because the length field of the file was corrupted
    data.resize(0);
    data.reserve(length);
    size_t to_read = length;
    while (to_read > 0)
    {
        size_t chunk_size = min(to_read, (size_t)1024 * 1024);
        size_t pos = data.size();
        data.resize(pos + chunk_size);
        size_t res = fd.read(data.data() + pos, chunk_size);
        if (res == 0)
            return false;
        to_read -= res;
        data.resize(pos + res);
    }
    return true;
}

bool Bundle::read(NamedFileDescriptor& fd)
{
    if (!read_header(fd)) return false;
    return read_data(fd);
}

}
}
