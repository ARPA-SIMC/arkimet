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
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
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
using namespace arki::utils::codec;

namespace arki {
namespace types {

const char* traits<Source>::type_tag = TAG;
const types::Code traits<Source>::type_code = CODE;
const size_t traits<Source>::type_sersize_bytes = SERSIZELEN;
const char* traits<Source>::type_lua_tag = LUATAG_TYPES ".source";

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

int Source::compare_local(const Source& o) const
{
	if (format < o.format) return -1;
	if (format > o.format) return 1;
	return 0;
}

void Source::encodeWithoutEnvelope(Encoder& enc) const
{
	StyledType<Source>::encodeWithoutEnvelope(enc);
	enc.addUInt(format.size(), 1);
	enc.addString(format);
}

void Source::serialiseLocal(Emitter& e, const Formatter* f) const
{
    types::StyledType<Source>::serialiseLocal(e, f);
    e.add("f"); e.add(format);
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
			throw wibble::exception::Consistency("parsing Source", "style " + formatStyle(s) + " but we can only decode BLOB, URL or INLINE");
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

Item<Source> Source::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case Source::BLOB: return source::Blob::decodeMapping(val);
        case Source::URL: return source::URL::decodeMapping(val);
        case Source::INLINE: return source::Inline::decodeMapping(val);
        default:
            throw wibble::exception::Consistency("parsing Source", "unknown Source style " + val.get_string());
    }
}

#ifdef HAVE_LUA
bool Source::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "format")
		lua_pushlstring(L, format.data(), format.size());
	else
		return StyledType<Source>::lua_lookup(L, name);
	return true;
}
#endif


namespace source {

Source::Style Blob::style() const { return Source::BLOB; }

void Blob::encodeWithoutEnvelope(Encoder& enc) const
{
	Source::encodeWithoutEnvelope(enc);
	enc.addVarint(filename.size());
	enc.addString(filename);
	enc.addVarint(offset);
	enc.addVarint(size);
}

std::ostream& Blob::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << format << "," << filename << ":" << offset << "+" << size
			 << ")";
}
void Blob::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Source::serialiseLocal(e, f);
    e.add("file", filename);
    e.add("ofs", offset);
    e.add("sz", size);
}
Item<Blob> Blob::decodeMapping(const emitter::memory::Mapping& val)
{
    return Blob::create(
            val["f"].want_string("parsing blob source format"),
            val["file"].want_string("parsing blob source filename"),
            val["ofs"].want_int("parsing blob source offset"),
            val["sz"].want_int("parsing blob source size"));
}
const char* Blob::lua_type_name() const { return "arki.types.source.blob"; }

#ifdef HAVE_LUA
bool Blob::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "file")
		lua_pushlstring(L, filename.data(), filename.size());
	else if (name == "offset")
		lua_pushnumber(L, offset);
	else if (name == "size")
		lua_pushnumber(L, size);
	else
		return Source::lua_lookup(L, name);
	return true;
}
#endif

int Blob::compare_local(const Source& o) const
{
	// We should be the same kind, so upcast
	const Blob* v = dynamic_cast<const Blob*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Blob Source, but is a ") + typeid(&o).name() + " instead");

	if (filename < v->filename) return -1;
	if (filename > v->filename) return 1;
	if (int res = offset - v->offset) return res;
	return size - v->size;
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

Item<Blob> Blob::prependPath(const std::string& path) const
{
	return Blob::create(format, wibble::str::joinpath(path, filename), offset, size);
}

Item<Blob> Blob::fileOnly() const
{
	return Blob::create(format, wibble::str::basename(filename), offset, size);
}


Source::Style URL::style() const { return Source::URL; }

void URL::encodeWithoutEnvelope(Encoder& enc) const
{
	Source::encodeWithoutEnvelope(enc);
	enc.addVarint(url.size());
	enc.addString(url);
}

std::ostream& URL::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << format << "," << url
			 << ")";
}
void URL::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Source::serialiseLocal(e, f);
    e.add("url"); e.add(url);
}
Item<URL> URL::decodeMapping(const emitter::memory::Mapping& val)
{
    return URL::create(
            val["f"].want_string("parsing url source format"),
            val["url"].want_string("parsing url source url"));
}

const char* URL::lua_type_name() const { return "arki.types.source.url"; }

#ifdef HAVE_LUA
bool URL::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "url")
		lua_pushlstring(L, url.data(), url.size());
	else
		return Source::lua_lookup(L, name);
	return true;
}
#endif

int URL::compare_local(const Source& o) const
{
	// We should be the same kind, so upcast
	const URL* v = dynamic_cast<const URL*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a URL Source, but is a ") + typeid(&o).name() + " instead");

	if (url < v->url) return -1;
	if (url > v->url) return 1;
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

void Inline::encodeWithoutEnvelope(Encoder& enc) const
{
	Source::encodeWithoutEnvelope(enc);
	enc.addVarint(size);
}

std::ostream& Inline::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << format << "," << size
			 << ")";
}
void Inline::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Source::serialiseLocal(e, f);
    e.add("sz", size);
}
Item<Inline> Inline::decodeMapping(const emitter::memory::Mapping& val)
{
    return Inline::create(
            val["f"].want_string("parsing inline source format"),
            val["sz"].want_int("parsing inline source size"));
}

const char* Inline::lua_type_name() const { return "arki.types.source.inline"; }

#ifdef HAVE_LUA
bool Inline::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "size")
		lua_pushnumber(L, size);
	else
		return Source::lua_lookup(L, name);
	return true;
}
#endif

int Inline::compare_local(const Source& o) const
{
	// We should be the same kind, so upcast
	const Inline* v = dynamic_cast<const Inline*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Inline Source, but is a ") + typeid(&o).name() + " instead");

	return size - v->size;
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

static MetadataType sourceType = MetadataType::create<Source>();

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
