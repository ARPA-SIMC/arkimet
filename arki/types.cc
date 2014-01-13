/*
 * types - arkimet metadata type system
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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include "config.h"

#include <arki/types.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include <arki/formatter.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/buffer.h>

#include <cstring>
#include <unistd.h>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

using namespace std;
using namespace wibble;

namespace arki {
namespace types {

Code checkCodeName(const std::string& name)
{
	// TODO: convert into something faster, like a hash lookup or a gperf lookup
	string nname = str::trim(str::tolower(name));
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
		throw wibble::exception::Consistency("parsing yaml data", "unsupported field type: " + name);
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
		default: return "unknown(" + wibble::str::fmt((int)c) + ")";
	}
}

int Type::compare(const Type& o) const
{
	return serialisationCode() - o.serialisationCode();
}

std::string Type::encodeWithEnvelope() const
{
	using namespace utils::codec;
	string contents;
	Encoder contentsenc(contents);
	encodeWithoutEnvelope(contentsenc);
	string res;
	Encoder enc(res);
	enc.addVarint((unsigned)serialisationCode());
	enc.addVarint(contents.size());
	enc.addString(contents);
	return res;
}

void Type::serialise(Emitter& e, const Formatter* f) const
{
    e.start_mapping();
    e.add("t", tag());
    if (f) e.add("desc", (*f)(this));
    serialiseLocal(e, f);
    e.end_mapping();
}

std::string Type::exactQuery() const
{
	throw wibble::exception::Consistency("creating a query to match " + tag(), "not implemented");
}

#ifdef HAVE_LUA
static Item<>* lua_check_arkitype(lua_State* L, int idx, const char* prefix)
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
				return (Item<>*)p;
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
	UItem<> item = Type::lua_check(L, 1);
        lua_pushstring(L, wibble::str::fmt(item).c_str());
        return 1;
}
static int arkilua_eq(lua_State* L)
{
	UItem<> item1 = Type::lua_check(L, 1);
	UItem<> item2 = Type::lua_check(L, 2);
        lua_pushboolean(L, item1 == item2);
        return 1;
}
static int arkilua_lt(lua_State* L)
{
	UItem<> item1 = Type::lua_check(L, 1);
	UItem<> item2 = Type::lua_check(L, 2);
        lua_pushboolean(L, item1 < item2);
        return 1;
}
static int arkilua_le(lua_State* L)
{
	UItem<> item1 = Type::lua_check(L, 1);
	UItem<> item2 = Type::lua_check(L, 2);
        lua_pushboolean(L, item1 <= item2);
        return 1;
}
static int arkilua_index(lua_State* L)
{
	using namespace arki;

	Item<> item = types::Type::lua_check(L, 1);
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
	Item<>* ud = lua_check_arkitype(L, 1, "arki.types");
	ud->~Item<>();
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
	luaL_register(L, NULL, lib);

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
	// The userdata will be an Item<> created with placement new
	// so that it will take care of reference counting
	Item<>* s = (Item<>*)lua_newuserdata(L, sizeof(Item<>));
	new(s) Item<>(this);

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

Item<> Type::lua_check(lua_State* L, int idx, const char* prefix)
{
	return *lua_check_arkitype(L, idx, prefix);
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

Item<> decode(const unsigned char* buf, size_t len)
{
	types::Code code = decodeEnvelope(buf, len);
	return types::MetadataType::get(code)->decode_func(buf, len);
}

Item<> decode(utils::codec::Decoder& dec)
{
    using namespace utils::codec;

    // Decode the element type
    Code code = (Code)dec.popVarint<unsigned>("element code");
    // Decode the element size
    size_t size = dec.popVarint<size_t>("element size");

    // Finally decode the element body
    ensureSize(dec.len, size, "element body");
    Item<> res = decodeInner(code, dec.buf, size);
    dec.buf += size;
    dec.len -= size;
    return res;
}

Item<> decodeInner(types::Code code, const unsigned char* buf, size_t len)
{
	return types::MetadataType::get(code)->decode_func(buf, len);
}

Item<> decodeString(types::Code code, const std::string& val)
{
	return types::MetadataType::get(code)->string_decode_func(val);
}

Item<> decodeMapping(const emitter::memory::Mapping& m)
{
    using namespace emitter::memory;
    const std::string& type = m["t"].want_string("decoding item type");
    return decodeMapping(parseCodeName(type), m);
}

Item<> decodeMapping(types::Code code, const emitter::memory::Mapping& m)
{
    using namespace emitter::memory;
    return types::MetadataType::get(code)->mapping_decode_func(m);
}

std::string tag(types::Code code)
{
	return types::MetadataType::get(code)->tag;
}

bool readBundle(int fd, const std::string& filename, wibble::sys::Buffer& buf, std::string& signature, unsigned& version)
{
	using namespace utils::codec;

	// Skip all leading blank bytes
	char c;
	while (true)
	{
		int res = read(fd, &c, 1);
		if (res < 0) throw wibble::exception::File(filename, "reading first byte of header");
		if (res == 0) return false; // EOF
		if (c) break;
	}

	// Read the rest of the first 8 bytes
	unsigned char hdr[8];
	hdr[0] = c;
	int res = read(fd, hdr + 1, 7);
	if (res < 0) throw wibble::exception::File(filename, "reading 7 more bytes");
	if (res < 7) return false; // EOF

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
	res = read(fd, buf.data(), len);
	if (res < 0)
		throw wibble::exception::File(filename, "reading " + str::fmt(len) + " bytes");
	if ((unsigned)res != len)
		throw wibble::exception::Consistency("reading " + filename, "read only " + str::fmt(res) + " of " + str::fmt(res) + " bytes: either the file is corrupted or it does not contain arkimet metadata");

	return true;
}
bool readBundle(std::istream& in, const std::string& filename, wibble::sys::Buffer& buf, std::string& signature, unsigned& version)
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
		throw wibble::exception::Consistency("reading " + filename, "read less than " + str::fmt(len) + " bytes: either the file is corrupted or it does not contain arkimet metadata");
	if (in.bad())
		throw wibble::exception::File(filename, "reading " + str::fmt(len) + " bytes");

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
		throw wibble::exception::Consistency("parsing " + filename, "partial data encountered: 8 bytes of header needed, " + str::fmt(len) + " found");

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
