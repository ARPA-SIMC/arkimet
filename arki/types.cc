/*
 * types - arkimet metadata type system
 *
 * Copyright (C) 2007--2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types.h>
#include <arki/utils/codec.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/buffer.h>

#include <cstring>

using namespace std;
using namespace wibble;

namespace arki {
namespace types {

Code parseCodeName(const std::string& name)
{
	using namespace str;

	// TODO: convert into something faster, like a hash lookup or a gperf lookup
	string nname = trim(tolower(name));
	if (nname == "origin") return TYPE_ORIGIN;
	if (nname == "product") return TYPE_PRODUCT;
	if (nname == "level") return TYPE_LEVEL;
	if (nname == "timerange") return TYPE_TIMERANGE;
	if (nname == "reftime") return TYPE_REFTIME;
	if (nname == "note") return TYPE_NOTE;
	if (nname == "source") return TYPE_SOURCE;
	if (nname == "assigneddataset") return TYPE_ASSIGNEDDATASET;
	if (nname == "area") return TYPE_AREA;
	if (nname == "ensemble") return TYPE_ENSEMBLE;
	if (nname == "summaryitem") return TYPE_SUMMARYITEM;
	if (nname == "summarystats") return TYPE_SUMMARYSTATS;
	if (nname == "bbox") return TYPE_BBOX;
	if (nname == "run") return TYPE_RUN;
	throw wibble::exception::Consistency("parsing yaml data", "unsupported field type: " + name);
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
		case TYPE_ENSEMBLE: return "ENSEMBLE";
		case TYPE_SUMMARYITEM: return "SUMMARYITEM";
		case TYPE_SUMMARYSTATS: return "SUMMARYSTATS";
		case TYPE_BBOX: return "BBOX";
		case TYPE_RUN: return "RUN";
		default: return "unknown(" + wibble::str::fmt((int)c) + ")";
	}
}

// Registry of item information
static const int decoders_size = 1024;
static const MetadataType** decoders = 0;

MetadataType::MetadataType(
		types::Code serialisationCode,
		int serialisationSizeLen,
		const std::string& tag,
		item_decoder decode_func,
		string_decoder string_decode_func)
    : serialisationCode(serialisationCode),
	  serialisationSizeLen(serialisationSizeLen),
	  tag(tag),
	  decode_func(decode_func),
	  string_decode_func(string_decode_func)
{
	// Ensure that the map is created before we add items to it
	if (!decoders)
	{
		decoders = new const MetadataType*[decoders_size];
		memset(decoders, 0, decoders_size * sizeof(int));
	}

	decoders[serialisationCode] = this;
}

MetadataType::~MetadataType()
{
	if (!decoders)
		return;
	
	decoders[serialisationCode] = 0;
}

const MetadataType* MetadataType::get(types::Code code)
{
	return decoders[code];
}

int Type::compare(const Type& o) const
{
	return serialisationCode() - o.serialisationCode();
}

std::string Type::encodeWithEnvelope() const
{
	using namespace utils::codec;
	string contents = encodeWithoutEnvelope();
	return encodeVarint((unsigned)serialisationCode())
		 + encodeVarint(contents.size())
		 + contents;
}

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
	return decoders[code]->decode_func(buf, len);
}

Item<> decodeInner(types::Code code, const unsigned char* buf, size_t len)
{
	using namespace wibble::str;
	if (!decoders || decoders[code] == 0)
		throw wibble::exception::Consistency("parsing binary data", "no decoder found for item type " + fmt(code));
	return decoders[code]->decode_func(buf, len);
}

Item<> decodeString(types::Code code, const std::string& val)
{
	using namespace wibble::str;
	if (!decoders || decoders[code] == 0)
		throw wibble::exception::Consistency("parsing string", "no decoder found for item type " + fmt(code));
	return decoders[code]->string_decode_func(val);
}

std::string tag(types::Code code)
{
	using namespace wibble::str;
	if (!decoders || decoders[code] == 0)
		throw wibble::exception::Consistency("parsing string", "no decoder found for item type " + fmt(code));
	return decoders[code]->tag;
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
	if ((unsigned)res != len)
		throw wibble::exception::File(filename, "reading " + str::fmt(len) + " bytes");

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
	if (in.fail())
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
