#ifndef ARKI_UTILS_CODEC_H
#define ARKI_UTILS_CODEC_H

/*
 * utils/codec - Encoding/decoding utilities
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

#include <arki/utils.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <stdint.h>
#include <string>

namespace arki {
namespace utils {
namespace codec {

/// Simple boundary check
static inline void ensureSize(size_t len, size_t req, const char* what)
{
	using namespace wibble::str;
	if (len < req)
		throw wibble::exception::Consistency(std::string("parsing ") + what, "size is " + fmt(len) + " but we need at least "+fmt(req)+" for the "+what);
}

// Encodes a varint to a string
template<typename T>
std::string encodeVarint(T val)
{
	// Only work with unsigned
	ARKI_STATIC_ASSERT(((T)(-1)) > 0);

	// Varint idea taken from Google's protocol buffers, and code based on
	// protbuf's varint implementation
	uint8_t bytes[sizeof(val)+2];
	int size = 0;
	while (val > 0x7F)
	{
		bytes[size++] = ((uint8_t)val & 0x7F) | 0x80;
		val >>= 7;
	}
	bytes[size++] = (uint8_t)val & 0x7F;
	return std::string((const char*)bytes, size);
}

/**
 * Decodes a varint from a buffer
 *
 * @param buf the buffer
 * @param size the maximum buffer size
 * @retval val the decoded value
 * @returns the number of bytes decoded
 */
template<typename T, typename BTYPE>
size_t decodeVarint(const BTYPE* genbuf, unsigned int size, T& val)
{
	// Only work with unsigned
	ARKI_STATIC_ASSERT(((T)(-1)) > 0);

	// Accept whatever pointer, but case to uint8_t*
	const uint8_t* buf = (const uint8_t*)genbuf;

	// Varint idea taken from Google's protocol buffers, and code based on
	// protbuf's varint implementation
	val = 0;

	for (size_t count = 0; count < size && count < sizeof(val)+2; ++count)
	{
		val |= ((T)buf[count] & 0x7F) << (7 * count);
		if ((buf[count] & 0x80) == 0)
			return count+1;
	}

	val = 0;
	return 0;
}

/**
 * Convenience version of decodeVarint that throws an exception if decoding
 * fails
 */
template<typename T, typename BTYPE>
size_t decodeVarint(const BTYPE* genbuf, unsigned int size, T& val, const char* what)
{
	using namespace wibble::str;
	size_t res = decodeVarint(genbuf, size, val);
	if (res == 0)
		throw wibble::exception::Consistency(std::string("parsing ") + what, "invalid varint data");
}

/// Encode an unsigned integer in the given amount of bytes, big endian
std::string encodeUInt(unsigned int val, unsigned int bytes);

/// Encode an unsigned integer in the given amount of bytes, big endian
std::string encodeULInt(unsigned long long int val, unsigned int bytes);

/// Encode a signed integer in the given amount of bytes, big endian
static inline std::string encodeSInt(signed int val, unsigned int bytes)
{
	uint32_t uns;
	if (val < 0)
	{
		// If it's negative, we encode the 2-complement of the positive value
		uns = -val;
		uns = ~uns + 1;
	} else
		uns = val;
	return encodeUInt(uns, bytes);
}

/// Decode the first 'bytes' bytes from val as an int, big endian
static inline uint32_t decodeUInt(const unsigned char* val, unsigned int bytes)
{
	uint32_t res = 0;
	for (unsigned int i = 0; i < bytes; ++i)
		res |= (unsigned char)val[i] << ((bytes - i - 1) * 8);
	return res;
}

/// Decode the first 'bytes' bytes from val as an int, big endian
uint64_t decodeULInt(const unsigned char* val, unsigned int bytes);

/// Decode the first 'bytes' bytes from val as an int, big endian
static inline int decodeSInt(const unsigned char* val, unsigned int bytes)
{
	uint32_t uns = decodeUInt(val, bytes);
	// Check if it's negative
	if (uns & 0x80u << ((bytes-1)*8))
	{
		const uint32_t mask = bytes == 1 ? 0xff : bytes == 2 ? 0xffff : bytes == 3 ? 0xffffff : 0xffffffff;
		uns = (~(uns-1)) & mask;
		return -uns;
	} else
		return uns;
}

/// Decode the first 'bytes' bytes from val as an int, little endian
static inline unsigned int decodeIntLE(const unsigned char* val, unsigned int bytes)
{
	unsigned int res = 0;
	for (unsigned int i = bytes - 1; i >= 0; --i)
		res |= (unsigned char)val[i] << ((bytes - i - 1) * 8);
	return res;
}

/// Encode a IEEE754 float
static inline std::string encodeFloat(float val)
{
	std::string res(sizeof(float), 0);
	for (unsigned int i = 0; i < sizeof(float); ++i)
		res[i] = ((const unsigned char*)&val)[i];
	return res;
}

/// Decode an IEEE754 float
static inline float decodeFloat(const unsigned char* val)
{
	float res = 0;
	for (unsigned int i = 0; i < sizeof(float); ++i)
		((unsigned char*)&res)[i] = val[i];
	return res;
}

/// Encode a IEEE754 double
static inline std::string encodeDouble(double val)
{
	std::string res(sizeof(double), 0);
	for (unsigned int i = 0; i < sizeof(double); ++i)
		res[i] = ((const unsigned char*)&val)[i];
	return res;
}

/// Decode an IEEE754 double
static inline double decodeDouble(const unsigned char* val)
{
	double res = 0;
	for (unsigned int i = 0; i < sizeof(double); ++i)
		((unsigned char*)&res)[i] = val[i];
	return res;
}

/// Convenient front-end to the various decoding functions
struct Decoder
{
	const unsigned char* buf;
	size_t len;

	Decoder(const unsigned char* buf, size_t len) : buf(buf), len(len) {}

	template<typename T, typename STR>
	T popVarint(STR what)
	{
		T val;
		size_t res = decodeVarint(buf, len, val);
		if (res == 0)
			throw wibble::exception::Consistency(std::string("parsing ") + what, "invalid varint data");
		buf += res;
		len -= res;
		return val;
	}

	template<typename STR>
	uint32_t popUInt(unsigned int bytes, STR what)
	{
		ensureSize(len, bytes, what);
		uint32_t val = decodeUInt(buf, bytes);
		buf += bytes;
		len -= bytes;
		return val;
	}

	template<typename STR>
	uint64_t popULint(unsigned int bytes, STR what)
	{
		ensureSize(len, bytes, what);
		uint64_t val = decodeULInt(buf, bytes);
		buf += bytes;
		len -= bytes;
		return val;
	}

	template<typename STR>
	int popSInt(unsigned int bytes, STR what)
	{
		ensureSize(len, bytes, what);
		int val = decodeSInt(buf, bytes);
		buf += bytes;
		len -= bytes;
		return val;
	}

	template<typename STR>
	unsigned int popIntLE(unsigned int bytes, STR what)
	{
		ensureSize(len, bytes, what);
		unsigned int val = decodeIntLE(buf, bytes);
		buf += bytes;
		len -= bytes;
		return val;
	}

	template<typename STR>
	unsigned int popFloat(STR what)
	{
		ensureSize(len, sizeof(float), what);
		float val = decodeFloat(buf);
		buf += sizeof(float);
		len -= sizeof(float);
		return val;
	}

	template<typename STR>
	unsigned int popDouble(STR what)
	{
		ensureSize(len, sizeof(double), what);
		double val = decodeFloat(buf);
		buf += sizeof(double);
		len -= sizeof(double);
		return val;
	}

	std::string popString(size_t size, const char* what)
	{
		ensureSize(len, size, what);
		std::string val((const char*)buf, size);
		buf += size;
		len -= size;
		return val;
	}
};

}
}
}

// vim:set ts=4 sw=4:
#endif
