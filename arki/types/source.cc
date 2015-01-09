/*
 * types/source - Source information
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
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "source.h"
#include "source/blob.h"
#include "source/inline.h"
#include "source/url.h"
#include <wibble/exception.h>
#include <wibble/string.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/utils/lua.h>
#include <arki/utils/datareader.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include <sstream>

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

auto_ptr<Source> Source::decode(const unsigned char* buf, size_t len)
{
    return decodeRelative(buf, len, string());
}

auto_ptr<Source> Source::decodeRelative(const unsigned char* buf, size_t len, const std::string& basedir)
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
            uint64_t offset = dec.popVarint<uint64_t>("blob source offset");
            uint64_t size = dec.popVarint<uint64_t>("blob source size");
            return createBlob(format, basedir, fname, offset, size);
        }
        case URL: {
            unsigned fname_len = dec.popVarint<unsigned>("url source file name length");
            string url = dec.popString(fname_len, "url source url");
            return createURL(format, url);
        }
        case INLINE: {
            uint64_t size = dec.popVarint<uint64_t>("inline source size");
            return createInline(format, size);
        }
        default:
            throw wibble::exception::Consistency("parsing Source", "style " + formatStyle(s) + " but we can only decode BLOB, URL or INLINE");
    }
}

auto_ptr<Source> Source::decodeString(const std::string& val)
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

            return createBlob(format, string(), fname,
                    strtoull(inner.substr(pos, end-pos).c_str(), 0, 10),
                    strtoull(inner.substr(end+1).c_str(), 0, 10));
        }
        case Source::URL: return createURL(format, inner);
        case Source::INLINE: return createInline(format, strtoull(inner.c_str(), 0, 10));
		default:
			throw wibble::exception::Consistency("parsing Source", "unknown Source style " + str::fmt(style));
	}
}

auto_ptr<Source> Source::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case Source::BLOB: return upcast<Source>(source::Blob::decodeMapping(val));
        case Source::URL: return upcast<Source>(source::URL::decodeMapping(val));
        case Source::INLINE: return upcast<Source>(source::Inline::decodeMapping(val));
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

auto_ptr<Source> Source::createBlob(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size)
{
    return upcast<Source>(source::Blob::create(format, basedir, filename, offset, size));
}

auto_ptr<Source> Source::createInline(const std::string& format, uint64_t size)
{
    return upcast<Source>(source::Inline::create(format, size));
}

auto_ptr<Source> Source::createInline(const std::string& format, const wibble::sys::Buffer& buf)
{
    return upcast<Source>(source::Inline::create(format, buf));
}

auto_ptr<Source> Source::createURL(const std::string& format, const std::string& url)
{
    return upcast<Source>(source::URL::create(format, url));
}

static MetadataType sourceType = MetadataType::create<Source>();

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
