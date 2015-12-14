#ifndef ARKI_BINARY_H
#define ARKI_BINARY_H

#include <arki/defs.h>
#include <vector>
#include <string>
#include <cstdint>

namespace arki {

struct BinaryEncoder
{
    std::vector<uint8_t>& dest;

    BinaryEncoder(std::vector<uint8_t>& dest) : dest(dest) {}

    /// Encode an unsigned integer as a varint
    template<typename T>
    void add_varint(T val)
    {
        // Only work with unsigned
        static_assert(std::is_unsigned<T>(), "source integer must be unsigned");

        // Varint idea taken from Google's protocol buffers, and code based on
        // protbuf's varint implementation
        while (val > 0x7F)
        {
            dest.push_back(((uint8_t)val & 0x7F) | 0x80);
            val >>= 7;
        }
        dest.push_back((uint8_t)val & 0x7F);
    }

    /// Encode an unsigned integer in the given amount of bytes, big endian
    template<typename T>
    void add_unsigned(T val, unsigned bytes)
    {
        // Only work with unsigned
        static_assert(std::is_unsigned<T>(), "source integer must be unsigned");

        for (unsigned int i = 0; i < bytes; ++i)
            dest.push_back((val >> ((bytes - i - 1) * 8)) & 0xff);
    }

    /// Encode a signed integer in the given amount of bytes, big endian
    void add_signed(int val, unsigned int bytes)
    {
        uint32_t uns;
        if (val < 0)
        {
            // If it's negative, we encode the 2-complement of the positive value
            uns = -val;
            uns = ~uns + 1;
        } else
            uns = val;
        add_unsigned(uns, bytes);
    }

    /// Encode a IEEE754 float
    void add_float(float val)
    {
        for (unsigned int i = 0; i < sizeof(float); ++i)
            dest.push_back(((const uint8_t*)&val)[i]);
    }

    /// Encode a IEEE754 double
    void add_double(double val)
    {
        for (unsigned int i = 0; i < sizeof(double); ++i)
            dest.push_back(((const uint8_t*)&val)[i]);
    }

    void add_string(const char* str) { while (*str) dest.push_back(*str++); }
    void add_raw(const uint8_t* buf, size_t size) { dest.insert(dest.end(), buf, buf + size); }
    void add_raw(const std::string& str) { dest.insert(dest.end(), str.begin(), str.end()); }
    void add_raw(const std::vector<uint8_t>& o) { dest.insert(dest.end(), o.begin(), o.end()); }
};

struct BinaryDecoder
{
    const uint8_t* buf = 0;
    std::size_t size = 0;

    BinaryDecoder() = default;
    BinaryDecoder(const BinaryDecoder&) = default;
    BinaryDecoder(BinaryDecoder&&) = default;
    BinaryDecoder(const std::string& str) : buf((const uint8_t*)str.data()), size(str.size()) {}
    BinaryDecoder(const std::vector<uint8_t>& buf) : buf(buf.data()), size(buf.size()) {}
    BinaryDecoder(const uint8_t* buf, std::size_t size) : buf(buf), size(size) {}
    BinaryDecoder& operator=(const BinaryDecoder&) = default;
    BinaryDecoder& operator=(BinaryDecoder&&) = default;

    /// Check if there is data still to be read in the decoder
    operator bool() const { return size != 0; }

    /// Simple boundary check
    inline void ensure_size(size_t wanted, const char* what)
    {
        if (size < wanted)
            throw_insufficient_size(what, wanted);
    }

    /// Return the first byte in the buffer
    template<typename STR>
    uint8_t pop_byte(STR what)
    {
        ensure_size(1, what);
        uint8_t res = buf[0];
        ++buf;
        --size;
        return res;
    }

    /**
     * Decode a varint from the beginning of the buffer and return it,
     * advancing the buffer to just past the varint
     */
    template<typename T, typename STR>
    T pop_varint(STR what)
    {
        T val;
        size_t res = decode_varint(buf, size, val);
        if (res == 0) throw_parse_error(what, "invalid varint data");
        buf += res;
        size -= res;
        return val;
    }

    /**
     * Decode a fixed-size unsigned integer from the beginning of the buffer
     * and return it.
     */
    template<typename STR>
    uint32_t pop_uint(unsigned int bytes, STR what)
    {
        ensure_size(bytes, what);
        uint32_t val = decode_uint(buf, bytes);
        buf += bytes;
        size -= bytes;
        return val;
    }

    /**
     * Decode a fixed-size signed integer from the beginning of the buffer
     * and return it.
     */
    template<typename STR>
    int pop_sint(unsigned int bytes, STR what)
    {
        ensure_size(bytes, what);
        int val = decode_sint(buf, bytes);
        buf += bytes;
        size -= bytes;
        return val;
    }

    template<typename STR>
    uint64_t pop_ulint(unsigned int bytes, STR what)
    {
        ensure_size(bytes, what);
        uint64_t val = decode_ulint(buf, bytes);
        buf += bytes;
        size -= bytes;
        return val;
    }

    /**
     * Decode a float value.
     *
     * Lacking enough clue about floating point representation, it is assumed
     * that the in-memory representation is standard enough to be transmitted as is.
     */
    template<typename STR>
    float pop_float(STR what)
    {
        ensure_size(sizeof(float), what);
        float val = decode_float(buf);
        buf += sizeof(float);
        size -= sizeof(float);
        return val;
    }

    /**
     * Decode a double value.
     *
     * Lacking enough clue about floating point representation, it is assumed
     * that the in-memory representation is standard enough to be transmitted as is.
     */
    template<typename STR>
    double pop_double(STR what)
    {
        ensure_size(sizeof(double), what);
        double val = decode_double(buf);
        buf += sizeof(double);
        size -= sizeof(double);
        return val;
    }

    /**
     * Decode a string from the beginning of the buffer and return it
     */
    std::string pop_string(size_t bytes, const char* what)
    {
        ensure_size(bytes, what);
        std::string res((const char*)buf, bytes);
        buf += bytes;
        size -= bytes;
        return res;
    }

    /**
     * Return a BinaryDecoder for the first \a bytes bytes, and move reading
     * after it
     */
    BinaryDecoder pop_data(size_t bytes, const char* what)
    {
        ensure_size(bytes, what);
        BinaryDecoder res(buf, bytes);
        buf += bytes;
        size -= bytes;
        return res;
    }

    /**
     * Skip all the leading bytes with the given value
     */
    void skip_leading(uint8_t val)
    {
        while (size && buf[0] == val)
        {
            ++buf;
            --size;
        }
    }

    /**
     * Decode the Type envelope at the beginning of the buffer
     *
     * @retval code the type code for the item encoded inside the envelope.
     * @returns a BinaryDecoder for the envelope contents.
     */
    BinaryDecoder pop_type_envelope(TypeCode& code);

    /**
     * Decode a metadata bundle from the beginning of the buffer.
     *
     * Returns the signature string, the version number and a BinaryDecoder
     * pointing at the data inside the buffer.
     *
     * Throws an exception if the buffer does not contain a metadata bundle.
     */
    BinaryDecoder pop_metadata_bundle(std::string& signature, unsigned& version);

    /**
     * Decodes a varint from a buffer
     *
     * @param buf the buffer
     * @param size the maximum buffer size
     * @retval val the decoded value
     * @returns the number of bytes decoded
     */
    template<typename T>
    static size_t decode_varint(const uint8_t* buf, unsigned int size, T& val)
    {
        // Only work with unsigned
        static_assert(std::is_unsigned<T>(), "target integer must be unsigned");

        // Varint idea taken from Google's protocol buffers, and code based on
        // protbuf's varint implementation
        val = 0;

        for (size_t count = 0; count < size && count < sizeof(val)+2; ++count)
        {
            val |= ((T)buf[count] & 0x7F) << (7 * count);
            if ((buf[count] & 0x80) == 0)
                return count + 1;
        }

        val = 0;
        return 0;
    }

    /// Decode the first 'bytes' bytes from val as an int, big endian
    static inline uint32_t decode_uint(const uint8_t* val, unsigned int bytes)
    {
        uint32_t res = 0;
        for (unsigned int i = 0; i < bytes; ++i)
            res |= (unsigned char)val[i] << ((bytes - i - 1) * 8);
        return res;
    }

    /// Decode the first 'bytes' bytes from val as an int, big endian
    static inline int decode_sint(const uint8_t* val, unsigned int bytes)
    {
        uint32_t uns = decode_uint(val, bytes);
        // Check if it's negative
        if (uns & 0x80u << ((bytes-1)*8))
        {
            const uint32_t mask = bytes == 1 ? 0xff : bytes == 2 ? 0xffff : bytes == 3 ? 0xffffff : 0xffffffff;
            uns = (~(uns-1)) & mask;
            return -uns;
        } else
            return uns;
    }

    /// Decode the first 'bytes' bytes from val as an int, big endian
    static inline uint64_t decode_ulint(const uint8_t* val, unsigned int bytes)
    {
        uint64_t res = 0;
        for (unsigned int i = 0; i < bytes; ++i)
        {
            res <<= 8;
            res |= val[i];
        }
        return res;
    }

    /// Decode an IEEE754 float
    static inline float decode_float(const uint8_t* val)
    {
        float res = 0;
        for (unsigned int i = 0; i < sizeof(float); ++i)
            ((unsigned char*)&res)[i] = val[i];
        return res;
    }

    /// Decode an IEEE754 double
    static inline double decode_double(const uint8_t* val)
    {
        double res = 0;
        for (unsigned int i = 0; i < sizeof(double); ++i)
            ((unsigned char*)&res)[i] = val[i];
        return res;
    }

protected:
    [[noreturn]] void throw_parse_error(const std::string& what, const std::string& errmsg);
    [[noreturn]] void throw_insufficient_size(const std::string& what, size_t wanted);
};

}

#endif
