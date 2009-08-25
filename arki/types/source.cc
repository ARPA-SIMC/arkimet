/*
 * types/source - Source information
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

#include <wibble/exception.h>
#include <wibble/string.h>
#include <arki/types/source.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include "config.h"
#include <sstream>

#ifdef HAVE_LUA
extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}
#endif

#define CODE types::TYPE_SOURCE
#define TAG "source"
#define SERSIZELEN 2

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace types {

// Style constants
//const unsigned char Source::NONE;
const unsigned char Source::BLOB;
const unsigned char Source::URL;
const unsigned char Source::INLINE;

Source::Style Source::parseStyle(const std::string& str)
{
	if (str == "BLOB") return BLOB;
	if (str == "URL") return URL;
	if (str == "INLINE") return INLINE;
	throw wibble::exception::Consistency("parsing Source style", "cannot parse Source style '"+str+"': only BLOB, URL and INLINE are supported");
}

std::string Source::formatStyle(Source::Style s)
{
	switch (s)
	{
		//case Source::NONE: return "NONE";
		case Source::BLOB: return "BLOB";
		case Source::URL: return "URL";
		case Source::INLINE: return "INLINE";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

int Source::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Source* v = dynamic_cast<const Source*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be an Source, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Source::compare(const Source& o) const
{
	if (int res = style() - o.style()) return res;
	if (format < o.format) return -1;
	if (format > o.format) return 1;
	return 0;
}

types::Code Source::serialisationCode() const { return CODE; }
size_t Source::serialisationSizeLength() const { return SERSIZELEN; }

std::string Source::tag() const
{
	return TAG;
}

std::string Source::encodeWithoutEnvelope() const
{
	using namespace utils::codec;
	return encodeUInt(style(), 1) + encodeUInt(format.size(), 1) + format;
}

Item<Source> Source::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	Decoder dec(buf, len);
	Style s = (Style)dec.popUInt(1, "source style");
	unsigned int format_len = dec.popUInt(1, "source format length");
	string format = dec.popString(format_len, "source format name");
	switch (s)
	{
		case BLOB: {
			unsigned fname_len = dec.popVarint<unsigned>("blob source file name length");
			string fname = dec.popString(fname_len, "blob source file name");
			size_t offset = dec.popVarint<size_t>("blob source offset");
			size_t size = dec.popVarint<size_t>("blob source size");
			return source::Blob::create(format, fname, offset, size);
		}
		case URL: {
			unsigned fname_len = dec.popVarint<unsigned>("url source file name length");
			string url = dec.popString(fname_len, "url source url");
			return source::URL::create(format, url);
		}
		case INLINE: {
			size_t size = dec.popVarint<size_t>("inline source size");
			return source::Inline::create(format, size);
		}
		default:
			throw wibble::exception::Consistency("parsing Source", "style " + str::fmt(s) + "but we can only decode BLOB, URL or INLINE");
	}
}

Item<Source> Source::decodeString(const std::string& val)
{
	string inner;
	Source::Style style = outerParse<Source>(val, inner);

	// Parse the format
	size_t pos = inner.find(',');
	if (pos == string::npos)
		throw wibble::exception::Consistency("parsing Source", "source \""+inner+"\" should start with \"format,\"");
	string format = inner.substr(0, pos);
	inner = inner.substr(pos+1);

	switch (style)
	{
		case Source::BLOB: {
			size_t end = inner.rfind(':');
			if (end == string::npos)
				throw wibble::exception::Consistency("parsing Source", "source \""+inner+"\" should contain a filename followed by a colon (':')");
			string fname = inner.substr(0, end);
			pos = end + 1;
			end = inner.find('+', pos);
			if (end == string::npos)
				throw wibble::exception::Consistency("parsing Source", "source \""+inner+"\" should contain \"offset+len\" after the filename");

			return source::Blob::create(format, fname,
					strtoul(inner.substr(pos, end-pos).c_str(), 0, 10),
					strtoul(inner.substr(end+1).c_str(), 0, 10));
		}
		case Source::URL:
			return source::URL::create(format, inner);
		case Source::INLINE:
			return source::Inline::create(format, strtoul(inner.c_str(), 0, 10));
		default:
			throw wibble::exception::Consistency("parsing Source", "unknown Source style " + str::fmt(style));
	}
}

#ifdef HAVE_LUA
int Source::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Source reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const Source& v = **(const Source**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "style")
	{
		string s = Source::formatStyle(v.style());
		lua_pushlstring(L, s.data(), s.size());
		return 1;
	}
	else if (name == "blob" && v.style() == Source::BLOB)
	{
		const source::Blob* v1 = v.upcast<source::Blob>();
		lua_pushlstring(L, v1->format.data(), v1->format.size());
		lua_pushlstring(L, v1->filename.data(), v1->filename.size());
		lua_pushnumber(L, v1->offset);
		lua_pushnumber(L, v1->size);
		return 4;
	}
	else if (name == "url" && v.style() == Source::URL)
	{
		const source::URL* v1 = v.upcast<source::URL>();
		lua_pushlstring(L, v1->format.data(), v1->format.size());
		lua_pushlstring(L, v1->url.data(), v1->url.size());
		return 2;
	}
	else if (name == "inline" && v.style() == Source::INLINE)
	{
		const source::Inline* v1 = v.upcast<source::Inline>();
		lua_pushlstring(L, v1->format.data(), v1->format.size());
		lua_pushnumber(L, v1->size);
		return 2;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_source(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Source::lua_lookup, 2);
	return 1;
}
void Source::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Source** s = (const Source**)lua_newuserdata(L, sizeof(const Source*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_source);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif


namespace source {

Source::Style Blob::style() const { return Source::BLOB; }

std::string Blob::encodeWithoutEnvelope() const
{
	using namespace utils::codec;
	return Source::encodeWithoutEnvelope() + encodeVarint(filename.size()) + filename + encodeVarint(offset) + encodeVarint(size);
}

std::ostream& Blob::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << format << "," << filename << ":" << offset << "+" << size
			 << ")";
}

int Blob::compare(const Source& o) const
{
	int res = Source::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Blob* v = dynamic_cast<const Blob*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Blob Source, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int Blob::compare(const Blob& o) const
{
	if (filename < o.filename) return -1;
	if (filename > o.filename) return 1;
	if (int res = offset - o.offset) return res;
	return size - o.size;
}

bool Blob::operator==(const Type& o) const
{
	const Blob* v = dynamic_cast<const Blob*>(&o);
	if (!v) return false;
	return format == v->format && filename == v->filename && offset == v->offset && size == v->size;
}

Item<Blob> Blob::create(const std::string& format, const std::string& filename, size_t offset, size_t size)
{
	Blob* res = new Blob;
	res->format = format;
	res->filename = filename;
	res->offset = offset;
	res->size = size;
	return res;
}

Item<Blob> Blob::prependPath(const std::string& path)
{
	return Blob::create(format, wibble::str::joinpath(path, filename), offset, size);
}


Source::Style URL::style() const { return Source::URL; }

std::string URL::encodeWithoutEnvelope() const
{
	using namespace utils::codec;
	return Source::encodeWithoutEnvelope() + encodeVarint(url.size()) + url;
}

std::ostream& URL::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << format << "," << url
			 << ")";
}

int URL::compare(const Source& o) const
{
	int res = Source::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const URL* v = dynamic_cast<const URL*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a URL Source, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int URL::compare(const URL& o) const
{
	if (url < o.url) return -1;
	if (url > o.url) return 1;
	return 0;
}
bool URL::operator==(const Type& o) const
{
	const URL* v = dynamic_cast<const URL*>(&o);
	if (!v) return false;
	return format == v->format && url == v->url;
}

Item<URL> URL::create(const std::string& format, const std::string& url)
{
	URL* res = new URL;
	res->format = format;
	res->url = url;
	return res;
}


Source::Style Inline::style() const { return Source::INLINE; }

std::string Inline::encodeWithoutEnvelope() const
{
	using namespace utils::codec;
	return Source::encodeWithoutEnvelope() + encodeVarint(size);
}

std::ostream& Inline::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << format << "," << size
			 << ")";
}

int Inline::compare(const Source& o) const
{
	int res = Source::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Inline* v = dynamic_cast<const Inline*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Inline Source, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int Inline::compare(const Inline& o) const
{
	return size - o.size;
}
bool Inline::operator==(const Type& o) const
{
	const Inline* v = dynamic_cast<const Inline*>(&o);
	if (!v) return false;
	return format == v->format && size == v->size;
}

Item<Inline> Inline::create(const std::string& format, size_t size)
{
	Inline* res = new Inline;
	res->format = format;
	res->size = size;
	return res;
}

}

static MetadataType sourceType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Source::decode),
	(MetadataType::string_decoder)(&Source::decodeString));

}
}
// vim:set ts=4 sw=4:
