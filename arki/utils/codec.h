#ifndef ARKI_UTILS_CODEC_H
#define ARKI_UTILS_CODEC_H

#include <string>
#include <vector>
#include <sstream>
#include <type_traits>
#include <cstdint>

namespace arki {
namespace utils {
namespace codec {

/// Simple boundary check
static inline void ensureSize(size_t len, size_t req, const char* what)
{
    if (len < req)
    {
        std::stringstream ss;
        ss << "cannot parse " << what << ": size is " << len << " but we need at least " << req << " for the " << what;
        throw std::runtime_error(ss.str());
    }
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
    static_assert(std::is_unsigned<T>(), "target integer must be unsigned");

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
    size_t res = decodeVarint(genbuf, size, val);
    if (res == 0)
    {
        std::stringstream ss;
        ss << "cannot parse " << what << ": invalid varint data";
        throw std::runtime_error(ss.str());
    }
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

/// Decode an IEEE754 float
static inline float decodeFloat(const unsigned char* val)
{
	float res = 0;
	for (unsigned int i = 0; i < sizeof(float); ++i)
		((unsigned char*)&res)[i] = val[i];
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

/**
 * Collection of functions that encode data and append them to a vector of
 * uint8_t.
 */
class Encoder
{
	/// Encode an unsigned integer in the given amount of bytes, big endian
	void _addUInt(unsigned int val, unsigned int bytes);

	/// Encode an unsigned integer in the given amount of bytes, big endian
	void _addULInt(unsigned long long int val, unsigned int bytes);

public:
    std::vector<uint8_t>& buf;

    Encoder(std::vector<uint8_t>& buf) : buf(buf) {}

    void addString(const char* str) { while (*str) buf.push_back(*str++); }
    void addString(const char* str, size_t n) { buf.insert(buf.end(), str, str + n); }
    void addString(const std::string& str) { buf.insert(buf.end(), str.begin(), str.end()); }
    void addBuffer(const std::vector<uint8_t>& o) { buf.insert(buf.end(), o.begin(), o.end()); }

    /// Encode an unsigned integer in the given amount of bytes, big endian
    template<typename T>
    void addU(T val, unsigned bytes)
    {
        // Only work with unsigned
        static_assert(std::is_unsigned<T>(), "source integer must be unsigned");

        for (unsigned int i = 0; i < bytes; ++i)
            buf.push_back((val >> ((bytes - i - 1) * 8)) & 0xff);
    }

    /// Encode an unsigned integer as a varint
    template<typename T>
    void addVarint(T val)
    {
        // Only work with unsigned
        static_assert(std::is_unsigned<T>(), "source integer must be unsigned");

        // Varint idea taken from Google's protocol buffers, and code based on
        // protbuf's varint implementation
        while (val > 0x7F)
        {
            buf.push_back(((uint8_t)val & 0x7F) | 0x80);
            val >>= 7;
        }
        buf.push_back((uint8_t)val & 0x7F);
    }

    /// Encode an unsigned integer in the given amount of bytes, big endian
    void addUInt(unsigned int val, unsigned int bytes) { addU(val, bytes); }

    /// Encode an unsigned integer in the given amount of bytes, big endian
    void addULInt(unsigned long long int val, unsigned int bytes) { return addU(val, bytes); }

    /// Encode a signed integer in the given amount of bytes, big endian
    void addSInt(signed int val, unsigned int bytes)
    {
        uint32_t uns;
        if (val < 0)
        {
            // If it's negative, we encode the 2-complement of the positive value
            uns = -val;
            uns = ~uns + 1;
        } else
            uns = val;
        addU(uns, bytes);
    }

    /// Encode a IEEE754 float
    void addFloat(float val)
    {
        for (unsigned int i = 0; i < sizeof(float); ++i)
            buf.push_back(((const uint8_t*)&val)[i]);
    }

    /// Encode a IEEE754 double
    void addDouble(double val)
    {
        for (unsigned int i = 0; i < sizeof(double); ++i)
            buf.push_back(((const uint8_t*)&val)[i]);
    }
};

/// Convenient front-end to the various decoding functions
struct Decoder
{
	const unsigned char* buf;
	size_t len;

    Decoder(const std::string& str) : buf((const unsigned char*)str.data()), len(str.size()) {}
    Decoder(const unsigned char* buf, size_t len) : buf(buf), len(len) {}
    Decoder(const std::vector<uint8_t>& buf, size_t offset=0);

    template<typename T, typename STR>
    T popVarint(STR what)
    {
        T val;
        size_t res = decodeVarint(buf, len, val);
        if (res == 0)
        {
            std::stringstream ss;
            ss << "cannot parse " << what << ": invalid varint data";
            throw std::runtime_error(ss.str());
        }
        buf += res;
        len -= res;
        return val;
    }

	template<typename STR>
	uint32_t popUInt(unsigned int bytes, STR what)
	{
		codec::ensureSize(len, bytes, what);
		uint32_t val = decodeUInt(buf, bytes);
		buf += bytes;
		len -= bytes;
		return val;
	}

	template<typename STR>
	uint64_t popULInt(unsigned int bytes, STR what)
	{
		codec::ensureSize(len, bytes, what);
		uint64_t val = decodeULInt(buf, bytes);
		buf += bytes;
		len -= bytes;
		return val;
	}

	template<typename STR>
	int popSInt(unsigned int bytes, STR what)
	{
		codec::ensureSize(len, bytes, what);
		int val = decodeSInt(buf, bytes);
		buf += bytes;
		len -= bytes;
		return val;
	}

	template<typename STR>
	unsigned int popIntLE(unsigned int bytes, STR what)
	{
		codec::ensureSize(len, bytes, what);
		unsigned int val = decodeIntLE(buf, bytes);
		buf += bytes;
		len -= bytes;
		return val;
	}

	template<typename STR>
	float popFloat(STR what)
	{
		codec::ensureSize(len, sizeof(float), what);
		float val = decodeFloat(buf);
		buf += sizeof(float);
		len -= sizeof(float);
		return val;
	}

	template<typename STR>
	double popDouble(STR what)
	{
		codec::ensureSize(len, sizeof(double), what);
		double val = decodeDouble(buf);
		buf += sizeof(double);
		len -= sizeof(double);
		return val;
	}

	std::string popString(size_t size, const char* what)
	{
		codec::ensureSize(len, size, what);
		std::string val((const char*)buf, size);
		buf += size;
		len -= size;
		return val;
	}
};

}
}
}
#endif
