#include "source.h"
#include "source/blob.h"
#include "source/inline.h"
#include "source/url.h"
#include "arki/binary.h"
#include "arki/segment.h"
#include "arki/exceptions.h"
#include "arki/types/utils.h"
#include "arki/utils/lua.h"
#include "arki/emitter.h"
#include "arki/emitter/memory.h"
#include "arki/emitter/keys.h"
#include "arki/emitter/structure.h"
#include <sstream>

#define CODE TYPE_SOURCE
#define TAG "source"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Source>::type_tag = TAG;
const types::Code traits<Source>::type_code = CODE;
const size_t traits<Source>::type_sersize_bytes = SERSIZELEN;
const char* traits<Source>::type_lua_tag = LUATAG_TYPES ".source";

source::Style Source::parseStyle(const std::string& str)
{
    if (str == "BLOB") return source::Style::BLOB;
    if (str == "URL") return source::Style::URL;
    if (str == "INLINE") return source::Style::INLINE;
    throw_consistency_error("parsing Source style", "cannot parse Source style '"+str+"': only BLOB, URL and INLINE are supported");
}

std::string Source::formatStyle(Source::Style s)
{
    switch (s)
    {
        //case Source::NONE: return "NONE";
        case source::Style::BLOB: return "BLOB";
        case source::Style::URL: return "URL";
        case source::Style::INLINE: return "INLINE";
        default: throw std::runtime_error("Unknown source style " + std::to_string((int)s));
    }
}

int Source::compare_local(const Source& o) const
{
    if (int res = StyledType<Source>::compare_local(o))
        return res;
    if (format < o.format) return -1;
    if (format > o.format) return 1;
    return 0;
}

void Source::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    StyledType<Source>::encodeWithoutEnvelope(enc);
    enc.add_unsigned(format.size(), 1);
    enc.add_raw(format);
}

void Source::serialise_local(Emitter& e, const emitter::Keys& keys, const Formatter* f) const
{
    types::StyledType<Source>::serialise_local(e, keys, f);
    e.add(keys.source_format); e.add(format);
}

unique_ptr<Source> Source::decode(BinaryDecoder& dec)
{
    return decodeRelative(dec, string());
}

unique_ptr<Source> Source::decodeRelative(BinaryDecoder& dec, const std::string& basedir)
{
    Style s = (Style)dec.pop_uint(1, "source style");
    unsigned int format_len = dec.pop_uint(1, "source format length");
    string format = dec.pop_string(format_len, "source format name");
    switch (s)
    {
        case source::Style::BLOB: {
            unsigned fname_len = dec.pop_varint<unsigned>("blob source file name length");
            string fname = dec.pop_string(fname_len, "blob source file name");
            uint64_t offset = dec.pop_varint<uint64_t>("blob source offset");
            uint64_t size = dec.pop_varint<uint64_t>("blob source size");
            return createBlobUnlocked(format, basedir, fname, offset, size);
        }
        case source::Style::URL: {
            unsigned fname_len = dec.pop_varint<unsigned>("url source file name length");
            string url = dec.pop_string(fname_len, "url source url");
            return createURL(format, url);
        }
        case source::Style::INLINE: {
            uint64_t size = dec.pop_varint<uint64_t>("inline source size");
            return createInline(format, size);
        }
        default: throw std::runtime_error("Unknown source style " + std::to_string((int)s));
    }
}

unique_ptr<Source> Source::decodeString(const std::string& val)
{
	string inner;
	Source::Style style = outerParse<Source>(val, inner);

	// Parse the format
	size_t pos = inner.find(',');
	if (pos == string::npos)
		throw_consistency_error("parsing Source", "source \""+inner+"\" should start with \"format,\"");
	string format = inner.substr(0, pos);
	inner = inner.substr(pos+1);

    switch (style)
    {
        case source::Style::BLOB: {
            size_t end = inner.rfind(':');
            if (end == string::npos)
                throw_consistency_error("parsing Source", "source \""+inner+"\" should contain a filename followed by a colon (':')");
            string fname = inner.substr(0, end);
            pos = end + 1;
            end = inner.find('+', pos);
            if (end == string::npos)
                throw_consistency_error("parsing Source", "source \""+inner+"\" should contain \"offset+len\" after the filename");

            return createBlobUnlocked(format, string(), fname,
                    strtoull(inner.substr(pos, end-pos).c_str(), 0, 10),
                    strtoull(inner.substr(end+1).c_str(), 0, 10));
        }
        case source::Style::URL: return createURL(format, inner);
        case source::Style::INLINE: return createInline(format, strtoull(inner.c_str(), 0, 10));
        default: throw std::runtime_error("Unknown source style " + std::to_string((int)style));
    }
}

unique_ptr<Source> Source::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case source::Style::BLOB: return upcast<Source>(source::Blob::decodeMapping(val));
        case source::Style::URL: return upcast<Source>(source::URL::decodeMapping(val));
        case source::Style::INLINE: return upcast<Source>(source::Inline::decodeMapping(val));
        default: throw std::runtime_error("Unknown source style " + val.get_string());
    }
}

std::unique_ptr<Source> Source::decode_structure(const emitter::Keys& keys, const emitter::Reader& val)
{
    switch (style_from_structure(keys, val))
    {
        case source::Style::BLOB: return upcast<Source>(source::Blob::decode_structure(keys, val));
        case source::Style::URL: return upcast<Source>(source::URL::decode_structure(keys, val));
        case source::Style::INLINE: return upcast<Source>(source::Inline::decode_structure(keys, val));
        default: throw std::runtime_error("Unknown source style");
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

std::unique_ptr<Source> Source::createBlob(std::shared_ptr<segment::Reader> reader, uint64_t offset, uint64_t size)
{
    return upcast<Source>(source::Blob::create(reader, offset, size));
}

unique_ptr<Source> Source::createBlob(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size, std::shared_ptr<segment::Reader> reader)
{
    return upcast<Source>(source::Blob::create(format, basedir, filename, offset, size, reader));
}

unique_ptr<Source> Source::createBlobUnlocked(const std::string& format, const std::string& basedir, const std::string& filename, uint64_t offset, uint64_t size)
{
    return upcast<Source>(source::Blob::create_unlocked(format, basedir, filename, offset, size));
}

unique_ptr<Source> Source::createInline(const std::string& format, uint64_t size)
{
    return upcast<Source>(source::Inline::create(format, size));
}

unique_ptr<Source> Source::createURL(const std::string& format, const std::string& url)
{
    return upcast<Source>(source::URL::create(format, url));
}

void Source::init()
{
    MetadataType::register_type<Source>();
}

}
}
#include <arki/types/styled.tcc>
