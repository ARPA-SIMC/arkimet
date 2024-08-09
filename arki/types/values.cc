#include "values.h"
#include "arki/libconfig.h"
#include "arki/exceptions.h"
#include "arki/core/binary.h"
#include "arki/utils/string.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include <memory>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include <cstdio>
#include <algorithm>
#ifdef __cpp_lib_string_view
#include <string_view>
#endif

#ifdef HAVE_LUA
extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}
#endif

using namespace arki::utils;


static bool parsesAsNumber(const std::string& str, int& parsed)
{
	if (str.empty())
		return false;
	const char* s = str.c_str();
	char* e;
	long int ival = strtol(s, &e, 0);
	for ( ; e-s < (signed)str.size() && *e && isspace(*e); ++e)
		;
	if (e-s != (signed)str.size())
		return false;
	parsed = ival;
	return true;
}

static bool needsQuoting(const std::string& str)
{
	// Empty strings don't need quoting
	if (str.empty())
		return false;
	
	// String starting with spaces or double quotes, need quoting
	if (isspace(str[0]) || str[0] == '"')
		return true;
	
	// String ending with spaces or double quotes, need quoting
	if (isspace(str[str.size() - 1]) || str[str.size() - 1] == '"')
		return true;

    // Strings containing nulls need quoting
    if (str.find('\0', 0) != std::string::npos)
        return true;

    // Otherwise, no quoting is neeed as decoding should be unambiguous
    return false;
}

static inline size_t skipSpaces(const std::string& str, size_t cur)
{
	while (cur < str.size() && isspace(str[cur]))
		++cur;
	return cur;
}

namespace arki {
namespace types {

namespace values {

#ifdef __cpp_lib_string_view
typedef std::string_view string_view;
#else
// Implement the subset of string_view that we need here, until we can use the
// real one (it needs C++17, which is currently not available on Centos 7)
struct string_view
{
    typedef std::string::size_type size_type;

    const char* m_data;
    size_type m_size;

    constexpr string_view(const char* s, size_type count) noexcept
        : m_data(s), m_size(count)
    {
    }

    string_view(const std::string& str) noexcept
        : m_data(str.data()), m_size(str.size())
    {
    }

    const char* data() const { return m_data; }
    size_type size() const { return m_size; }

    template<typename S>
    bool operator==(const S& o) const
    {
        if (m_size != o.size()) return false;
        return memcmp(m_data, o.data(), o.size()) == 0;
    }

    bool operator!=(const string_view& o) const
    {
        if (m_size != o.m_size) return true;
        return memcmp(m_data, o.m_data, m_size) != 0;
    }

    int compare(const string_view& o) const
    {
        if (int res = memcmp(m_data, o.m_data, std::min(m_size, o.m_size))) return res;
        return m_size - o.m_size;
    }

    bool operator<(const string_view& o) const { return compare(o) < 0; }
    bool operator<=(const string_view& o) const { return compare(o) <= 0; }
    bool operator>(const string_view& o) const { return compare(o) > 0; }
    bool operator>=(const string_view& o) const { return compare(o) >= 0; }

    operator std::string() const { return std::string(m_data, m_size); }
};

#endif

static std::string quote_if_needed(const std::string& str)
{
    std::string res;
    int idummy;

    if (parsesAsNumber(str, idummy) || needsQuoting(str))
    {
        // If it is surrounded by double quotes or it parses as a number, we need to escape it
        return "\"" + str::encode_cstring(str) + "\"";
    } else {
        // Else, we can use the value as it is
        return str;
    }
}


// Main encoding type constants (fit 2 bits)

// 6bit signed
static const int ENC_SINT6 = 0;
// Number.  Type is given in the next 6 bits.
static const int ENC_NUMBER = 1;
// String whose length can be encoded in 6 bits unsigned (i.e. max 64 characters)
static const int ENC_NAME = 2;
// Extension flag.  Type is given in the next 6 bits.
static const int ENC_EXTENDED = 3;

// Number encoding constants (next 2 bits after the main encoding type in the
// case of ENC_NUMBER)

static const int ENC_NUM_INTEGER = 0;	// Sign in the next bit.  Number of bytes in the next 3 bits.
static const int ENC_NUM_FLOAT = 1;		// Floating point, type in the next 4 bits.
static const int ENC_NUM_UNUSED = 2;	// Unused.  Leave to 0.
static const int ENC_NUM_EXTENDED = 3;	// Extension flag.  Type is given in the next 4 bits.


void encode_int(core::BinaryEncoder& enc, int value)
{
    if (value >= -32 && value < 31)
    {
        // If it's a small one, encode in the remaining 6 bits
        uint8_t lead = { ENC_SINT6 << 6 };
        if (value < 0)
        {
            lead |= ((~(-value) + 1) & 0x3f);
        } else {
            lead |= (value & 0x3f);
        }
        enc.add_byte(lead);
    }
    else
    {
        // Else, encode as an integer Number

        // Type
        uint8_t lead = (ENC_NUMBER << 6) | (ENC_NUM_INTEGER << 4);
        // Value to encode
        unsigned int encoded;
        if (value < 0)
        {
            // Sign bit
            lead |= 0x8;
            encoded = -value;
        }
        else
            encoded = value;
        // Number of bytes
        unsigned nbytes;
        // TODO: add bits for 64 bits here if it's ever needed
        if (encoded & 0xff000000)
            nbytes = 4;
        else if (encoded & 0x00ff0000)
            nbytes = 3;
        else if (encoded & 0x0000ff00)
            nbytes = 2;
        else if (encoded & 0x000000ff)
            nbytes = 1;
        else
            throw std::runtime_error("cannot encode integer number: value " + std::to_string(value) + " is too large to be encoded");

        lead |= (nbytes-1);
        enc.add_byte(lead);
        enc.add_unsigned(encoded, nbytes);
    }
}

int decode_sint6(uint8_t lead)
{
    if (lead & 0x20)
        return -((~(lead-1)) & 0x3f);
    else
        return lead & 0x3f;
}

int decode_number(core::BinaryDecoder& dec, uint8_t lead)
{
    switch ((lead >> 4) & 0x3)
    {
        case ENC_NUM_INTEGER: {
            // Sign in the next bit.  Number of bytes in the next 3 bits.
            unsigned nbytes = (lead & 0x7) + 1;
            unsigned val = dec.pop_uint(nbytes, "integer number value");
            return (lead & 0x8) ? -val : val;
        }
        case ENC_NUM_FLOAT:
            throw std::runtime_error("cannot decode value: the number value to decode is a floating point number, but decoding floating point numbers is not currently implemented");
        case ENC_NUM_UNUSED:
            throw std::runtime_error("cannot decode value: the number value to decode has an unknown type");
        case ENC_NUM_EXTENDED:
            throw std::runtime_error("cannot decode value: the number value to decode has an extended type, but no extended type is currently implemented");
        default:
            throw std::runtime_error("cannot decode value: control flow should never reach here (" __FILE__ ":" + std::to_string(__LINE__) + "), but the compiler cannot easily know it.  This is here to silence a compiler warning.");
    }
    return 0;
}

int decode_int(core::BinaryDecoder& dec, uint8_t lead)
{
    switch ((lead >> 6) & 0x3)
    {
        case ENC_SINT6:
            return decode_sint6(lead);
        case ENC_NUMBER:
            return decode_number(dec, lead);
        case ENC_NAME:
            throw std::runtime_error("cannot decode number: the encoded value has type 'name'");
        case ENC_EXTENDED:
            throw std::runtime_error("cannot decode value: the encoded value has an extended type, but no extended type is currently implemented");
        default:
            throw std::runtime_error("cannot decode value: control flow should never reach here (" __FILE__ ":" + std::to_string(__LINE__) + "), but the compiler cannot easily know it.  This is here to silence a compiler warning.");
    }
}


/*
 * Value
 */

class Value
{
public:
    Value() = default;
    Value(const Value&) = delete;
    Value(Value&&) = delete;
    virtual ~Value() {}
    Value& operator=(const Value&) = delete;
    Value& operator=(Value&&) = delete;

    /// Return a copy of this value
    virtual Value* clone() const = 0;

    /// Key name
    virtual values::string_view name() const = 0;

    /**
     * Implementation identifier, used for sorting and downcasting
     *
     * Currently this is 1 for int, 2 for string.
     *
     * The corrisponding as_* method is guaranteed not to throw consistently
     * with type_id.
     */
    virtual unsigned type_id() const = 0;

    /**
     * Return the integer value, if type_id == 1, else throws an exception
     */
    virtual int as_int() const
    {
        throw std::runtime_error("as_int not implemented for this value");
    }

    /**
     * Return the string value, if type_id == 2, else throws an exception
     */
    virtual std::string as_string() const
    {
        throw std::runtime_error("as_string not implemented for this value");
    }

    /**
     * Compare two Values, keys and values
     */
    int compare(const Value& v) const
    {
        // key: compare common string prefix
        if (int res = name().compare(v.name())) return res;
        return compare_values(v);
    }

    /**
     * Compare two Values, values only
     */
    int compare_values(const Value& v) const
    {
        if (int res = type_id() - v.type_id()) return res;
        switch (type_id())
        {
            case 1: return as_int() - v.as_int();
            case 2: return as_string().compare(v.as_string());
            default: throw std::runtime_error("invalid typeid found: memory is corrupted?");
        }
    }

    bool operator==(const Value& v) const { return compare(v) == 0; }
    bool operator!=(const Value& v) const { return compare(v) != 0; }

    /**
     * Encode into a non-ambiguous string representation.
     *
     * Strings with special characters or strings that may be confused with
     * numbers are quoted and escaped.
     */
    virtual std::string to_string() const = 0;

    /// Encode to a binary representation
    virtual void encode(core::BinaryEncoder& enc) const = 0;

    /// Send contents to an emitter
    virtual void serialise(structured::Emitter& e) const = 0;
};


/// Ephemeral value used for building ValueBag in ram
class BuildValue : public Value
{
protected:
    std::string m_name;

public:
    BuildValue(const std::string& name) : m_name(name) {}

    values::string_view name() const override
    {
        return m_name;
    }

    static std::unique_ptr<Value> create(const std::string& name, int value);
    static std::unique_ptr<Value> create(const std::string& name, const std::string& value);

    static std::unique_ptr<Value> parse(const std::string& name, const std::string& str, size_t& lenParsed);
    static std::unique_ptr<Value> parse(const std::string& name, const std::string& str)
    {
        size_t dummy;
        return parse(name, str, dummy);
    }
};

class BuildValueInt : public BuildValue
{
protected:
    int m_value;

public:
    BuildValueInt(const std::string& name, int value) : BuildValue(name), m_value(value) {}

    BuildValueInt* clone() const override { return new BuildValueInt(m_name, m_value); }

    unsigned type_id() const override { return 1; }
    int as_int() const override { return m_value; }

    void encode(core::BinaryEncoder& enc) const override
    {
        // Key length
        enc.add_unsigned(m_name.size(), 1);
        // Key
        enc.add_raw(m_name);
        encode_int(enc, m_value);
    }

    void serialise(structured::Emitter& e) const override
    {
        e.add(m_name);
        e.add_int(m_value);
    }

    std::string to_string() const override { return std::to_string(as_int()); }
};

class BuildValueString : public BuildValue
{
protected:
    std::string m_value;

public:
    BuildValueString(const std::string& name, const std::string& value) : BuildValue(name), m_value(value) {}

    BuildValueString* clone() const override { return new BuildValueString(m_name, m_value); }

    unsigned type_id() const override { return 2; }
    std::string as_string() const override { return m_value; }

    void encode(core::BinaryEncoder& enc) const override
    {
        // Key length
        enc.add_unsigned(m_name.size(), 1);
        // Key
        enc.add_raw(m_name);

        // Type and value length
        uint8_t type = ENC_NAME << 6;
        type |= m_value.size() & 0x3f;
        enc.add_byte(type);
        // Value
        enc.add_raw(m_value);
    }

    void serialise(structured::Emitter& e) const override
    {
        e.add(m_name);
        e.add_string(m_value);
    }

    std::string to_string() const override { return quote_if_needed(as_string()); }
};

std::unique_ptr<Value> BuildValue::create(const std::string& name, int value) { return std::unique_ptr<Value>(new BuildValueInt(name, value)); }
std::unique_ptr<Value> BuildValue::create(const std::string& name, const std::string& value) { return std::unique_ptr<Value>(new BuildValueString(name, value)); }

std::unique_ptr<Value> BuildValue::parse(const std::string& name, const std::string& str, size_t& lenParsed)
{
    size_t begin = skipSpaces(str, 0);

    // Handle the empty string
    if (begin == str.size())
    {
        lenParsed = begin;
        return std::unique_ptr<Value>(new BuildValueString(name, std::string()));
    }

    // Handle the quoted string
    if (str[begin] == '"')
    {
        // Skip the first double quote
        ++begin;

        // Unescape the string
        size_t parsed;
        std::string res = str::decode_cstring(str.substr(begin), parsed);

        lenParsed = skipSpaces(str, begin + parsed);
        return std::unique_ptr<Value>(new BuildValueString(name, res));
    }

    // No quoted string, so we can terminate the token at the next space, ',' or ';'
    size_t end = begin;
    while (end != str.size() && !isspace(str[end]) && str[end] != ',' && str[end] != ';')
        ++end;
    std::string res = str.substr(begin, end-begin);
    lenParsed = skipSpaces(str, end);

    // If it can be parsed as a number, with maybe leading and trailing
    // spaces, return the number
    int val;
    if (parsesAsNumber(res, val))
        return std::unique_ptr<Value>(new BuildValueInt(name, val));

    // Else return the string
    return std::unique_ptr<Value>(new BuildValueString(name, res));
}

/**
 * Base class for generic scalar values.
 *
 * This is needed in order to store arbitrary types for the key=value kind of
 * types (like Area and Proddef).
 *
 * In practice, we have to create a dynamic type system.
 */
class EncodedValue : public Value
{
protected:
    const uint8_t* data = nullptr;

    /// Size of the data buffer
    virtual unsigned encoded_size() const = 0;

    values::string_view name() const override
    {
        unsigned size = static_cast<unsigned>(data[0]);
        return values::string_view(reinterpret_cast<const char*>(data) + 1, size);
    }

public:
    EncodedValue(const uint8_t* data)
        : data(data)
    {
    }

    EncodedValue* clone() const override
    {
        throw std::runtime_error("cloning EncodedValue is not yet implemented");
    }

    /**
     * Encode into a compact binary representation
     */
    void encode(core::BinaryEncoder& enc) const override
    {
        enc.add_raw(data, encoded_size());
    }

    /**
     * Decode from compact binary representation.
     */
    static std::unique_ptr<Value> decode(core::BinaryDecoder& dec);

    /**
     * Parse from a string representation
     */
    static std::unique_ptr<Value> parse(const std::string& name, const std::string& str);

    /**
     * Parse from a string representation
     */
    static std::unique_ptr<Value> parse(const std::string& name, const std::string& str, size_t& lenParsed);

    static std::unique_ptr<Value> create_integer(const std::string& name, int val);
    static std::unique_ptr<Value> create_string(const std::string& name, const std::string& val);

    friend class Values;
    friend class types::ValueBag;
    friend class types::ValueBagMatcher;
};


/*
 * IntValue
 */

class ValueInt : public EncodedValue
{
protected:
    int m_value;

public:
    ValueInt(const uint8_t* data, int value)
        : EncodedValue(data), m_value(value) {}

    unsigned type_id() const override { return 1; }

    void serialise(structured::Emitter& e) const override
    {
        auto n = this->name();
        e.add(std::string(n.data(), n.size()));
        e.add_int(as_int());
    }

    std::string to_string() const override { return std::to_string(as_int()); }
};

class ValueString : public EncodedValue
{
public:
    ValueString(const uint8_t* data)
        : EncodedValue(data) {}
};


/*
 * EncodedSInt6
 */

class EncodedSInt6 : public ValueInt
{
    unsigned encoded_size() const override
    {
        // Size of the key size and key, and one byte of type and number
        return this->data[0] + 2;
    }

public:
    using ValueInt::ValueInt;

    int as_int() const override
    {
        uint8_t lead = this->data[this->data[0] + 1];
        if (lead & 0x20)
            return -((~(lead-1)) & 0x3f);
        else
            return lead & 0x3f;
    }
};


class EncodedNumber : public ValueInt
{
protected:
    unsigned encoded_size() const override
    {
        // Size of the key size and key
        unsigned size = this->data[0] + 1;

        uint8_t lead = this->data[size++];
        switch ((lead >> 4) & 0x3)
        {
            case ENC_NUM_INTEGER:
                // Sign in the next bit.  Number of bytes in the next 3 bits.
                size += (lead & 0x7) + 1;
                break;
        }

        return size;
    }

public:
    using ValueInt::ValueInt;

    int as_int() const override
    {
        // Skip key
        unsigned pos = this->data[0] + 1;
        uint8_t lead = this->data[pos];
        switch ((lead >> 4) & 0x3)
        {
            case ENC_NUM_INTEGER: {
                // Sign in the next bit.  Number of bytes in the next 3 bits.
                unsigned nbytes = (lead & 0x7) + 1;
                unsigned val = core::BinaryDecoder::decode_uint(this->data + pos + 1, nbytes);
                return (lead & 0x8) ? -val : val;
            }
        }
        return 0;
    }
};


class EncodedString : public ValueString
{
protected:
    unsigned encoded_size() const override
    {
        return this->data[0] + 2 + (this->data[this->data[0] + 1] & 0x3f);
    }

public:
    using ValueString::ValueString;

    unsigned type_id() const override { return 2; }

    // TODO: get rid of std::string. Use string_view or a local version if we don't have C++17
    std::string as_string() const override
    {
        // Skip key
        unsigned pos = this->data[0] + 1;
        unsigned size = this->data[pos] & 0x3f;
        return std::string(reinterpret_cast<const char*>(this->data) + pos + 1, size);
    }

    void serialise(structured::Emitter& e) const override
    {
        auto n = this->name();
        e.add(std::string(n.data(), n.size()));
        e.add_string(as_string());
    }

    std::string to_string() const override { return quote_if_needed(as_string()); }
};

std::unique_ptr<Value> EncodedValue::decode(core::BinaryDecoder& dec)
{
    // Mark the location of the beginning of the memory buffer
    const uint8_t* begin = dec.buf;

    // Key length
    unsigned name_len = dec.pop_uint(1, "valuebag key length");

    // Key
    std::string name = dec.pop_string(name_len, "valuebag key");

    uint8_t lead = dec.pop_byte("valuebag value type");
    switch ((lead >> 6) & 0x3)
    {
        case ENC_SINT6:
            return std::unique_ptr<Value>(new EncodedSInt6(begin, decode_sint6(lead)));
        case ENC_NUMBER:
            return std::unique_ptr<Value>(new EncodedNumber(begin, decode_number(dec, lead)));
        case ENC_NAME:
            dec.skip(lead & 0x3f, "string value");
            return std::unique_ptr<Value>(new EncodedString(begin));
        case ENC_EXTENDED:
            throw std::runtime_error("cannot decode value: the encoded value has an extended type, but no extended type is currently implemented");
        default:
            throw std::runtime_error("cannot decode value: control flow should never reach here (" __FILE__ ":" + std::to_string(__LINE__) + "), but the compiler cannot easily know it.  This is here to silence a compiler warning.");
    }
}


/*
 * Values
 */

Values::~Values()
{
    for (auto& i: values)
        delete i;
}

Values::Values(const Values& vb)
{
    values.reserve(vb.values.size());
    for (const auto& v: vb.values)
        values.emplace_back(v->clone());
}

Values::Values(Values&& vb)
    : values(std::move(vb.values))
{
}

Values& Values::operator=(Values&& vb)
{
    // Handle the case a=a
    if (this == &vb) return *this;

    values = std::move(vb.values);

    return *this;
}

size_t Values::size() const { return values.size(); }

void Values::clear()
{
    for (auto* i: values)
        delete i;
    values.clear();
}

void Values::set(std::unique_ptr<Value> val)
{
    for (auto i = values.begin(); i != values.end(); ++i)
    {
        if ((*i)->name() == val->name())
        {
            delete *i;
            *i = val.release();
            return;
        } else if ((*i)->name() > val->name()) {
            values.emplace(i, val.release());
            return;
        }
    }

    values.emplace_back(val.release());
}


/*
 * ValueBagBuilder
 */

void ValueBagBuilder::add(std::unique_ptr<Value> value)
{
    values.set(std::move(value));
}

void ValueBagBuilder::add(const std::string& key, const std::string& val)
{
    std::unique_ptr<values::Value> v(new values::BuildValueString(key, val));
    values.set(std::move(v));
}

void ValueBagBuilder::add(const std::string& key, int val)
{
    std::unique_ptr<values::Value> v(new values::BuildValueInt(key, val));
    values.set(std::move(v));
}

ValueBag ValueBagBuilder::build() const
{
    // Encode into a buffer
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    for (const auto& v: values.values)
        v->encode(enc);

    // FIXME: it would be great to detatch the vector's buffer, but we cannot.
    // We need another copy instead.
    // https://stackoverflow.com/questions/3667580/can-i-detach-a-stdvectorchar-from-the-data-it-contains
    //   has a hack to do it
    std::unique_ptr<uint8_t[]> priv(new uint8_t[buf.size()]);
    memcpy(priv.get(), buf.data(), buf.size());

    // FIXME: valuebag at this point doesn't have the values vector populated
    return ValueBag(priv.release(), buf.size(), true);
}

}

/*
 * ValueBag
 */

ValueBag::ValueBag(const uint8_t* data, unsigned size, bool owned)
    : data(data), size(size), owned(owned)
{
}

ValueBag::ValueBag(const ValueBag& o)
    : data(new uint8_t[o.size]), size(o.size), owned(true)
{
    memcpy(const_cast<uint8_t*>(data), o.data, size);
}

ValueBag::ValueBag(ValueBag&& o)
    : data(o.data), size(o.size), owned(o.owned)
{
    o.data = nullptr;
    o.size = 0;
    o.owned = false;
}

ValueBag::~ValueBag()
{
    if (owned)
        delete[] data;
}

ValueBag& ValueBag::operator=(ValueBag&& vb)
{
    // Handle the case a=a
    if (this == &vb) return *this;

    if (owned)
        delete[] data;

    data = vb.data;
    size = vb.size;
    owned = vb.owned;

    vb.data = nullptr;
    vb.size = 0;
    vb.owned = false;

    return *this;
}

ValueBag::const_iterator::const_iterator(const core::BinaryDecoder& dec)
    : dec(dec)
{
    if (dec.size)
        value = values::EncodedValue::decode(this->dec).release();
}

ValueBag::const_iterator::const_iterator(const_iterator&& o)
    : dec(o.dec), value(o.value)
{
    o.value = nullptr;
}

ValueBag::const_iterator::~const_iterator()
{
    delete value;
}

ValueBag::const_iterator& ValueBag::const_iterator::operator=(const_iterator&& o)
{
    if (this == &o) return *this;
    dec = o.dec;
    delete value;
    value = o.value;
    o.value = nullptr;
    return *this;
}

ValueBag::const_iterator& ValueBag::const_iterator::operator++()
{
    delete value;
    value = nullptr;
    if (dec.size)
        value = values::EncodedValue::decode(dec).release();
    return *this;
}

bool ValueBag::const_iterator::operator==(const const_iterator& o) const
{
    return dec == o.dec && bool(value) == bool(o.value);
}

bool ValueBag::const_iterator::operator!=(const const_iterator& o) const
{
    return dec != o.dec || bool(value) != bool(o.value);
}

int ValueBag::compare(const ValueBag& vb) const
{
    auto a = begin();
    auto b = vb.begin();
    for ( ; a != end() && b != vb.end(); ++a, ++b)
        if (int res = a->compare(*b))
            return res;
    if (a == end() && b == vb.end())
        return 0;
    if (a == end())
        return -1;
    return 1;
}

bool ValueBag::operator==(const ValueBag& vb) const
{
    auto a = begin();
    auto b = vb.begin();
    for ( ; a != end() && b != vb.end(); ++a, ++b)
        if (*a != *b)
            return false;
    return a == end() && b == vb.end();
}

#if 0
void ValueBag::set(const std::string& key, int val) { values::Values::set(values::Value::create_integer(key, val)); }
void ValueBag::set(const std::string& key, const std::string& val) { values::Values::set(values::Value::create_string(key, val)); }
#endif

void ValueBag::encode(core::BinaryEncoder& enc) const
{
    for (const auto& v: *this)
        v.encode(enc);
}

std::string ValueBag::toString() const
{
    std::string res;
    bool first = true;
    for (const auto& i : *this)
    {
        if (first)
            first = false;
        else
            res += ", ";
        auto name = i.name();
        res.append(name.data(), name.size());
        // TODO: when we can have real string_view: res += i->name();
        res += '=';
        res += i.to_string();
    }
    return res;
}

void ValueBag::serialise(structured::Emitter& e) const
{
    e.start_mapping();
    for (const auto& i: *this)
        i.serialise(e);
    e.end_mapping();
}

ValueBag ValueBag::decode(core::BinaryDecoder& dec)
{
    std::unique_ptr<uint8_t[]> buf(new uint8_t[dec.size]);
    memcpy(buf.get(), dec.buf, dec.size);
    return ValueBag(buf.release(), dec.size, true);
}

ValueBag ValueBag::decode_reusing_buffer(core::BinaryDecoder& dec)
{
    return ValueBag(dec.buf, dec.size, false);
}

ValueBag ValueBag::parse(const std::string& str)
{
    values::ValueBagBuilder builder;
    size_t begin = 0;
    while (begin < str.size())
    {
        // Take until the next '='
        size_t cur = str.find('=', begin);
        // If there are no more '=', check that we are at the end
        if (cur == std::string::npos)
        {
			cur = skipSpaces(str, begin);
			if (cur != str.size())
				throw_consistency_error("parsing key=value list", "found invalid extra characters \""+str.substr(begin)+"\" at the end of the list");
			break;
		}

        // Read the key
        std::string key = str::strip(str.substr(begin, cur-begin));

		// Skip the '=' sign
		++cur;

		// Skip spaces after the '='
		cur = skipSpaces(str, cur);

        // Parse the value
        size_t lenParsed;
        auto val(values::BuildValue::parse(key, str.substr(cur), lenParsed));

        // Set the value
        if (val.get())
            builder.add(std::move(val));
        else
            throw_consistency_error("parsing key=value list", "cannot parse value at \""+str.substr(cur)+"\"");

		// Move on to the next one
		begin = cur + lenParsed;

		// Skip separators
		while (begin != str.size() && (isspace(str[begin]) || str[begin] == ','))
			++begin;
	}

    return builder.build();
}

ValueBag ValueBag::parse(const structured::Reader& reader)
{
    values::ValueBagBuilder builder;
    reader.items("values", [&](const std::string& key, const structured::Reader& val) {
        switch (val.type())
        {
            case structured::NodeType::NONE:
                break;
            case structured::NodeType::INT:
                builder.add(key, val.as_int("int value"));
                break;
            case structured::NodeType::STRING:
                builder.add(key, val.as_string("string value"));
                break;
            default:
                throw std::runtime_error("cannot decode value " + key + ": value is neither integer nor string");
        }
    });

    return builder.build();
}

#ifdef HAVE_LUA
ValueBag ValueBag::load_lua_table(lua_State* L, int idx)
{
    values::ValueBagBuilder vb;

	// Make the table index absolute
	if (idx < 0) idx = lua_gettop(L) + idx + 1;

	// Iterate the table
	lua_pushnil(L);
	while (lua_next(L, idx))
	{
        // Get key
        std::string key;
        switch (lua_type(L, -2))
        {
            case LUA_TNUMBER: key = std::to_string((int)lua_tonumber(L, -2)); break;
            case LUA_TSTRING: key = lua_tostring(L, -2); break;
            default:
            {
                char buf[256];
                snprintf(buf, 256, "cannot read Lua table: key has type %s but only ints and strings are supported",
                        lua_typename(L, lua_type(L, -2)));
                throw std::runtime_error(buf);
            }
        }
        // Get value
        switch (lua_type(L, -1))
        {
            case LUA_TNUMBER:
                vb.add(key, lua_tonumber(L, -1));
                break;
            case LUA_TSTRING:
                vb.add(key, lua_tostring(L, -1));
                break;
            default:
            {
                char buf[256];
                snprintf(buf, 256, "cannot read Lua table: value has type %s but only ints and strings are supported",
                            lua_typename(L, lua_type(L, -1)));
                throw std::runtime_error(buf);
            }
		}
		lua_pop(L, 1);
	}
	lua_pop(L, 1);

    return vb.build();
}
#endif

bool ValueBagMatcher::is_subset(const ValueBag& vb) const
{
    // Both a and b are sorted, so we can iterate them linearly together
    auto a = values.begin();
    auto b = vb.begin();

    while (b != vb.end())
    {
        // Nothing else wanted anymore
        if (a == values.end()) return true;

        if ((*a)->name() < b->name())
            // This value is wanted but we don't have it
            return false;

        if (b->name() < (*a)->name())
        {
            // This value is not in the match expression
            ++b;
            continue;
        }

        if ((*a)->compare_values(*b) != 0)
            // Same key, check if the value is the same
            return false;

        // If also the value is the same, move on to the next item
        ++a;
        ++b;
    }
    // We got to the end of a.  If there are still things in b, we don't
    // match.  If we are also to the end of b, then we matched everything
    return a == values.end();
}

std::string ValueBagMatcher::to_string() const
{
    std::string res;
    bool first = true;
    for (const auto& i : values)
    {
        if (first)
            first = false;
        else
            res += ", ";
        auto name = i->name();
        res.append(name.data(), name.size());
        // TODO: when we can have real string_view: res += i->name();
        res += '=';
        res += i->to_string();
    }
    return res;
}

#ifdef HAVE_LUA
void ValueBagMatcher::lua_push(lua_State* L) const
{
    lua_newtable(L);
    for (const auto& i: values)
    {
        auto name = i->name();
        lua_pushlstring(L, name.data(), name.size());
        switch (i->type_id())
        {
            case 1:
                lua_pushnumber(L, i->as_int());
                break;
            case 2:
            {
                std::string val = i->as_string();
                lua_pushlstring(L, val.data(), val.size());
                break;
            }
            default:
                throw std::runtime_error("unknown type_id found in lua_push");
        }
        // Set name = val in the table
        lua_settable(L, -3);
    }
    // Leave the table on the stack: we pushed it
}
#endif

ValueBagMatcher ValueBagMatcher::parse(const std::string& str)
{
    ValueBagMatcher res;
    size_t begin = 0;
    while (begin < str.size())
    {
        // Take until the next '='
        size_t cur = str.find('=', begin);
        // If there are no more '=', check that we are at the end
        if (cur == std::string::npos)
        {
            cur = skipSpaces(str, begin);
            if (cur != str.size())
                throw_consistency_error("parsing key=value list", "found invalid extra characters \""+str.substr(begin)+"\" at the end of the list");
            break;
        }

        // Read the key
        std::string key = str::strip(str.substr(begin, cur-begin));

        // Skip the '=' sign
        ++cur;

        // Skip spaces after the '='
        cur = skipSpaces(str, cur);

        // Parse the value
        size_t lenParsed;
        auto val(values::BuildValue::parse(key, str.substr(cur), lenParsed));

        // Set the value
        if (val.get())
            res.set(std::move(val));
        else
            throw_consistency_error("parsing key=value list", "cannot parse value at \""+str.substr(cur)+"\"");

        // Move on to the next one
        begin = cur + lenParsed;

        // Skip separators
        while (begin != str.size() && (isspace(str[begin]) || str[begin] == ','))
            ++begin;
    }

    return res;
}

}
}
