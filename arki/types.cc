#include "config.h"
#include <arki/types.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/utils/sys.h>
#include <arki/utils/string.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include <arki/formatter.h>
#include <arki/wibble/exception.h>
#include <cstring>
#include <unistd.h>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

using namespace std;
using namespace arki::utils;

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

void Type::encodeBinary(utils::codec::Encoder& enc) const
{
    using namespace utils::codec;
    vector<uint8_t> contents;
    contents.reserve(256);
    Encoder contentsenc(contents);
    encodeWithoutEnvelope(contentsenc);

    enc.addVarint((unsigned)type_code());
    enc.addVarint(contents.size());
    enc.addBuffer(contents);
}

std::vector<uint8_t> Type::encodeBinary() const
{
    vector<uint8_t> contents;
    codec::Encoder enc(contents);
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

types::Code decodeEnvelope(const unsigned char*& buf, size_t& len)
{
	using namespace utils::codec;
	using namespace str;

	Decoder dec(buf, len);

	// Decode the element type
	Code code = (Code)dec.popVarint<unsigned>("element code");
	// Decode the element size
	size_t size = dec.popVarint<size_t>("element size");

	// Finally decode the element body
	ensureSize(dec.len, size, "element body");
	buf = dec.buf;
	len = size;
	return code;
}

unique_ptr<Type> decode(const unsigned char* buf, size_t len)
{
	types::Code code = decodeEnvelope(buf, len);
	return types::MetadataType::get(code)->decode_func(buf, len);
}

unique_ptr<Type> decode(utils::codec::Decoder& dec)
{
    using namespace utils::codec;

    // Decode the element type
    Code code = (Code)dec.popVarint<unsigned>("element code");
    // Decode the element size
    size_t size = dec.popVarint<size_t>("element size");

    // Finally decode the element body
    ensureSize(dec.len, size, "element body");
    unique_ptr<Type> res = decodeInner(code, dec.buf, size);
    dec.buf += size;
    dec.len -= size;
    return res;
}

unique_ptr<Type> decodeInner(types::Code code, const unsigned char* buf, size_t len)
{
	return types::MetadataType::get(code)->decode_func(buf, len);
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

bool readBundle(int fd, const std::string& filename, std::vector<uint8_t>& buf, std::string& signature, unsigned& version)
{
    using namespace utils::codec;
    sys::NamedFileDescriptor f(fd, filename);

    // Skip all leading blank bytes
    char c;
    while (true)
    {
        int res = f.read(&c, 1);
        if (res == 0) return false; // EOF
        if (c) break;
    }

    // Read the rest of the first 8 bytes
    unsigned char hdr[8];
    hdr[0] = c;
    size_t res = f.read(hdr + 1, 7);
    if (res < 7) return false; // EOF

	// Read the signature
	signature.resize(2);
	signature[0] = hdr[0];
	signature[1] = hdr[1];

    // Get the version in next 2 bytes
    version = decodeUInt(hdr + 2, 2);

    // Get length from next 4 bytes
    unsigned int len = decodeUInt(hdr + 4, 4);

    // Read the metadata body
    buf.resize(len);
    res = f.read(buf.data(), len);
    if ((unsigned)res != len)
    {
        stringstream ss;
        ss << "incomplete read from " << filename << " read only " << res << " of " << len << " bytes: either the file is corrupted or it does not contain arkimet metadata";
        throw std::runtime_error(ss.str());
    }

    return true;
}
bool readBundle(std::istream& in, const std::string& filename, std::vector<uint8_t>& buf, std::string& signature, unsigned& version)
{
	using namespace utils::codec;

	// Skip all leading blank bytes
	int c;
	while ((c = in.get()) == 0 && !in.eof())
		;

	if (in.eof())
		return false;

	if (in.fail())
		throw wibble::exception::File(filename, "reading first byte of header");

	// Read the rest of the first 8 bytes
	unsigned char hdr[8];
	hdr[0] = c;
	in.read((char*)hdr + 1, 7);
	if (in.fail())
	{
		if (in.eof())
			return false;
		else
			throw wibble::exception::File(filename, "reading 7 more bytes");
	}

	// Read the signature
	signature.resize(2);
	signature[0] = hdr[0];
	signature[1] = hdr[1];

	// Get the version in next 2 bytes
	version = decodeUInt(hdr+2, 2);

	// Get length from next 4 bytes
	unsigned int len = decodeUInt(hdr+4, 4);

    // Read the metadata body
    buf.resize(len);
    in.read((char*)buf.data(), len);
    if (in.fail() && in.eof())
    {
        stringstream ss;
        ss << "incomplete read from " << filename << ": read less than " << len << " bytes: either the file is corrupted or it does not contain arkimet metadata";
        throw std::runtime_error(ss.str());
    }
    if (in.bad())
    {
        stringstream ss;
        ss << "cannot read " << len << " bytes from " << filename;
        throw std::system_error(errno, std::system_category(), ss.str());
    }

    return true;
}

bool readBundle(const unsigned char*& buf, size_t& len, const std::string& filename, const unsigned char*& obuf, size_t& olen, std::string& signature, unsigned& version)
{
	using namespace utils::codec;

	// Skip all leading blank bytes
	while (len > 0 && *buf == 0)
	{
		++buf;
		--len;
	}
	if (len == 0)
		return false;

    if (len < 8)
    {
        stringstream ss;
        ss << "cannot parse " << filename << ": partial data encountered: 8 bytes of header needed, " << len << " found";
        throw std::runtime_error(ss.str());
    }

	// Read the signature
	signature.resize(2);
	signature[0] = buf[0];
	signature[1] = buf[1];
	buf += 2; len -= 2;

	// Get the version in next 2 bytes
	version = decodeUInt(buf, 2);
	buf += 2; len -= 2;
	
	// Get length from next 4 bytes
	olen = decodeUInt(buf, 4);
	buf += 4; len -= 4;

	// Save the start of the inner data
	obuf = buf;

	// Move to the end of the inner data
	buf += olen;
	len -= olen;

	return true;
}

}
}
// vim:set ts=4 sw=4:
