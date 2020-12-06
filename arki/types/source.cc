#include "source.h"
#include "source/blob.h"
#include "source/inline.h"
#include "source/url.h"
#include "arki/core/binary.h"
#include "arki/exceptions.h"
#include "arki/types/utils.h"
#include "arki/structured/emitter.h"
#include "arki/structured/keys.h"
#include "arki/structured/reader.h"
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

int Source::compare(const Type& o) const
{
    int res = Type::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Source* v = dynamic_cast<const Source*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `Source`, but it is `" << typeid(&o).name() << "' instead";
        throw std::runtime_error(ss.str());
    }

    return this->compare_local(*v);
}

int Source::compare_local(const Source& o) const
{
    if (this->style() < o.style())
        return -1;
    if (this->style() > o.style())
        return 1;
    if (format < o.format) return -1;
    if (format > o.format) return 1;
    return 0;
}

void Source::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    enc.add_unsigned(static_cast<unsigned>(style()), 1);
    enc.add_unsigned(format.size(), 1);
    enc.add_raw(format);
}

void Source::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.type_style, formatStyle(style()));
    e.add(keys.source_format); e.add(format);
}

unique_ptr<Source> Source::decode(core::BinaryDecoder& dec, bool reuse_buffer)
{
    return decodeRelative(dec, string());
}

unique_ptr<Source> Source::decodeRelative(core::BinaryDecoder& dec, const std::string& basedir)
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

std::unique_ptr<Source> Source::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    Style sty = parseStyle(val.as_string(keys.type_style, "type style"));
    switch (sty)
    {
        case source::Style::BLOB: return upcast<Source>(source::Blob::decode_structure(keys, val));
        case source::Style::URL: return upcast<Source>(source::URL::decode_structure(keys, val));
        case source::Style::INLINE: return upcast<Source>(source::Inline::decode_structure(keys, val));
        default: throw std::runtime_error("Unknown source style");
    }
}

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
