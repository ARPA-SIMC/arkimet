#ifndef ARKI_UTILS_H
#define ARKI_UTILS_H

/*
 * utils - General utility functions
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <sstream>
#include <string>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <stdint.h>

namespace arki {
namespace utils {

bool isdir(const std::string& root, wibble::sys::fs::Directory::const_iterator& i);

bool isdir(const std::string& pathname);

/// Read the entire contents of a file into a string
std::string readFile(const std::string& filename);

/// Read the entire contents of a file into a string
std::string readFile(std::istream& file, const std::string& filename);

/// Create an empty file, succeeding if it already exists
void createFlagfile(const std::string& pathname);

/// Create an empty file, failing if it already exists
void createNewFlagfile(const std::string& pathname);

/// Remove a file, succeeding if it does not exists
void removeFlagfile(const std::string& pathname);

/// Check if a file exists
bool hasFlagfile(const std::string& pathname);

/// Simple boundary check
static inline void ensureSize(size_t len, size_t req, const char* what)
{
	using namespace wibble::str;
	if (len < req)
		throw wibble::exception::Consistency(std::string("parsing ") + what, "size is " + fmt(len) + " but we need at least "+fmt(req)+" for the "+what);
}

template<typename A, typename B>
int compareMaps(const A& m1, const B& m2)
{
        typename A::const_iterator a = m1.begin();
        typename B::const_iterator b = m2.begin();
        for ( ; a != m1.end() && b != m2.end(); ++a, ++b)
        {
                if (a->first < b->first)
                        return -1;
                if (a->first > b->first)
                        return 1;
                if (int res = a->second->compare(*b->second)) return res;
        }
        if (a == m1.end() && b == m2.end())
                return 0;
        if (a == m1.end())
                return -1;
        return 1;
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

// Save the state of a stream, the RAII way
class SaveIOState
{
		std::ios_base& s;
		std::ios_base::fmtflags f;
public:
		SaveIOState(std::ios_base& s) : s(s), f(s.flags())
		{}
		~SaveIOState()
		{
				s.flags(f);
		}
};

// Dump the string, in hex, to stderr, prefixed with name
void hexdump(const char* name, const std::string& str);

// Dump the string, in hex, to stderr, prefixed with name
void hexdump(const char* name, const unsigned char* str, int len);


}
}

// vim:set ts=4 sw=4:
#endif
