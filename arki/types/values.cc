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

#ifndef __cpp_lib_string_view

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
};

#endif


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


Value::Value(const uint8_t* data)
    : data(data)
{
}

Value::~Value() {}

values::string_view Value::name() const
{
    unsigned size = static_cast<unsigned>(data[0]);
    return values::string_view(reinterpret_cast<const char*>(data) + 1, size);
}

void Value::encode(core::BinaryEncoder& enc) const
{
    enc.add_raw(data, encoded_size());
}

bool Value::operator==(const Value& v) const
{
    if (type_id() != v.type_id()) return false;
    if (data[0] != v.data[0]) return false;
    if (memcmp(data + 1, v.data + 1, data[0]) != 0) return false;
    return value_equals(v);
}

int Value::compare(const Value& v) const
{
    // key: compare common string prefix
    if (int res = memcmp(data + 1, v.data + 1, std::min(data[0], v.data[0]))) return res;
    // key: if the prefix is the same, compare lengths
    if (int res = data[0] - v.data[0]) return res;

    if (int res = type_id() - v.type_id()) return res;
    return value_compare(v);
}

int Value::compare_values(const Value& v) const
{
    if (int res = type_id() - v.type_id()) return res;
    return value_compare(v);
}


/*
 * Copy allocator
 */

struct CopyAllocator
{
    static const uint8_t* create(const uint8_t* data, unsigned size)
    {
        uint8_t* tdata = new uint8_t[size];
        memcpy(tdata, data, size);
        return tdata;
    }

    static void remove(const uint8_t* data)
    {
        delete[] data;
    }
};

struct ReuseAllocator
{
    static const uint8_t* create(const uint8_t* data, unsigned size)
    {
        return data;
    }

    static void remove(const uint8_t* data)
    {
    }
};


/*
 * ValueBase
 */

template<typename Alloc>
struct ValueBase : public Value
{
    ValueBase(const uint8_t* data, unsigned size)
        : Value(Alloc::create(data, size))
    {
    }

    ~ValueBase()
    {
        Alloc::remove(data);
    }
};


/*
 * EncodedInt
 */

template<typename Alloc>
class EncodedInt : public ValueBase<Alloc>
{
protected:
    bool value_equals(const Value& v) const override
    {
        const EncodedInt* e = reinterpret_cast<const EncodedInt*>(&v);
        return value() == e->value();
    }

    int value_compare(const Value& v) const override
    {
        const EncodedInt* e = reinterpret_cast<const EncodedInt*>(&v);
        return value() - e->value();
    }

public:
    using ValueBase<Alloc>::ValueBase;

    unsigned type_id() const override { return 1; }

    virtual int value() const = 0;

    void serialise(structured::Emitter& e) const override
    {
        auto n = this->name();
        e.add(std::string(n.data(), n.size()));
        e.add_int(value());
    }

    std::string toString() const override
    {
        return std::to_string(value());
    }
};


template<typename Alloc>
class EncodedSInt6 : public EncodedInt<Alloc>
{
    unsigned encoded_size() const override
    {
        // Size of the key size and key, and one byte of type and number
        return this->data[0] + 2;
    }

public:
    using EncodedInt<Alloc>::EncodedInt;

    std::unique_ptr<Value> clone() const
    {
        return std::unique_ptr<Value>(new EncodedSInt6(this->data, encoded_size()));
    }

    int value() const override
    {
        uint8_t lead = this->data[this->data[0] + 1];
        if (lead & 0x20)
            return -((~(lead-1)) & 0x3f);
        else
            return lead & 0x3f;
    }
};


template<typename Alloc>
class EncodedNumber : public EncodedInt<Alloc>
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
    using EncodedInt<Alloc>::EncodedInt;

    std::unique_ptr<Value> clone() const
    {
        return std::unique_ptr<Value>(new EncodedNumber(this->data, encoded_size()));
    }

    int value() const override
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


template<typename Alloc>
class EncodedString : public ValueBase<Alloc>
{
protected:
    unsigned encoded_size() const override
    {
        return this->data[0] + 2 + (this->data[this->data[0] + 1] & 0x3f);
    }

    bool value_equals(const Value& v) const override
    {
        const EncodedString* e = reinterpret_cast<const EncodedString*>(&v);
        return value() == e->value();
    }

    int value_compare(const Value& v) const override
    {
        const EncodedString* e = reinterpret_cast<const EncodedString*>(&v);
        return value().compare(e->value());
    }

public:
    using ValueBase<Alloc>::ValueBase;

    std::unique_ptr<Value> clone() const
    {
        return std::unique_ptr<Value>(new EncodedString(this->data, encoded_size()));
    }

    unsigned type_id() const override { return 2; }

    // TODO: get rid of std::string. Use string_view or a local version if we don't have C++17
    std::string value() const
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
        e.add_string(value());
    }

    std::string toString() const override
    {
        auto val = value();
        int idummy;

        if (parsesAsNumber(val, idummy) || needsQuoting(val))
        {
            // If it is surrounded by double quotes or it parses as a number, we need to escape it
            return "\"" + str::encode_cstring(val) + "\"";
        } else {
            // Else, we can use the value as it is
            return val;
        }
    }
};

template<typename Alloc>
static std::unique_ptr<Value> _decode(core::BinaryDecoder& dec)
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
            return std::unique_ptr<Value>(new EncodedSInt6<Alloc>(begin, dec.buf - begin));
        case ENC_NUMBER: {
            switch ((lead >> 4) & 0x3)
            {
                case ENC_NUM_INTEGER:
                {
                    // Sign in the next bit.  Number of bytes in the next 3 bits.
                    unsigned nbytes = (lead & 0x7) + 1;
                    dec.skip(nbytes, "integer number value");
                    return std::unique_ptr<Value>(new EncodedNumber<Alloc>(begin, dec.buf - begin));
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
        }
        case ENC_NAME:
            dec.skip(lead & 0x3f, "string value");
            return std::unique_ptr<Value>(new EncodedString<Alloc>(begin, dec.buf - begin));
        case ENC_EXTENDED:
            throw std::runtime_error("cannot decode value: the encoded value has an extended type, but no extended type is currently implemented");
        default:
            throw std::runtime_error("cannot decode value: control flow should never reach here (" __FILE__ ":" + std::to_string(__LINE__) + "), but the compiler cannot easily know it.  This is here to silence a compiler warning.");
    }
}

std::unique_ptr<Value> Value::decode(core::BinaryDecoder& dec)
{
    return _decode<CopyAllocator>(dec);
}

std::unique_ptr<Value> Value::decode_reusing_buffer(core::BinaryDecoder& dec)
{
    return _decode<ReuseAllocator>(dec);
}

std::unique_ptr<Value> Value::parse(const std::string& name, const std::string& str)
{
    size_t dummy;
    return Value::parse(name, str, dummy);
}

std::unique_ptr<Value> Value::parse(const std::string& name, const std::string& str, size_t& lenParsed)
{
    size_t begin = skipSpaces(str, 0);

    // Handle the empty string
    if (begin == str.size())
    {
        lenParsed = begin;
        return create_string(name, std::string());
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
        return values::Value::create_string(name, res);
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
        return values::Value::create_integer(name, val);

    // Else return the string
    return values::Value::create_string(name, res);
}

std::unique_ptr<Value> Value::create_integer(const std::string& name, int val)
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);

    // Key length
    enc.add_unsigned(name.size(), 1);
    // Key
    enc.add_raw(name);

    if (val >= -32 && val < 31)
    {
        // If it's a small one, encode in the remaining 6 bits
        uint8_t encoded = { ENC_SINT6 << 6 };
        if (val < 0)
        {
            encoded |= ((~(-val) + 1) & 0x3f);
        } else {
            encoded |= (val & 0x3f);
        }
        enc.add_raw(&encoded, 1u);
        return std::unique_ptr<Value>(new values::EncodedSInt6<CopyAllocator>(buf.data(), buf.size()));
    }
    else
    {
        // Else, encode as an integer Number

        // Type
        uint8_t type = (ENC_NUMBER << 6) | (ENC_NUM_INTEGER << 4);
        // Value to encode
        unsigned int encoded;
        if (val < 0)
        {
            // Sign bit
            type |= 0x8;
            encoded = -val;
        }
        else
            encoded = val;
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
            throw std::runtime_error("cannot encode integer number: value " + std::to_string(val) + " is too large to be encoded");

        type |= (nbytes-1);
        enc.add_raw(&type, 1u);
        enc.add_unsigned(encoded, nbytes);

        return std::unique_ptr<Value>(new values::EncodedNumber<CopyAllocator>(buf.data(), buf.size()));
    }
}

std::unique_ptr<Value> Value::create_string(const std::string& name, const std::string& val)
{
    if (val.size() >= 64)
        throw std::runtime_error("cannot use string as a value: string '" + val + "' cannot be longer than 63 characters, but the string is " + std::to_string(val.size()) + " characters long");

    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);

    // Key length
    enc.add_unsigned(name.size(), 1);
    // Key
    enc.add_raw(name);
    // Type and value length
    uint8_t type = ENC_NAME << 6;
    type |= val.size() & 0x3f;
    enc.add_byte(type);
    // Value
    enc.add_raw(val);

    return std::unique_ptr<Value>(new values::EncodedString<CopyAllocator>(buf.data(), buf.size()));
}


/*
 * Values
 */

Values::Values() {}

Values::~Values()
{
    for (auto& i: values)
        delete i;
}

Values::Values(Values&& vb)
    : values(std::move(vb.values))
{
}

Values::Values(const Values& vb)
    : Values()
{
    for (const auto& v: vb.values)
        values.emplace_back(v->clone().release());
}

Values& Values::operator=(const Values& vb)
{
    // Handle the case a=a
    if (this == &vb) return *this;

    clear();
    for (const auto& v: vb.values)
        values.emplace_back(v->clone().release());

    return *this;
}

Values& Values::operator=(Values&& vb)
{
    // Handle the case a=a
    if (this == &vb) return *this;

    values = std::move(vb.values);

    return *this;
}

void Values::clear()
{
    for (auto* i: values)
        delete i;
    values.clear();
}

void Values::set(std::unique_ptr<values::Value> val)
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

}


int ValueBag::compare(const ValueBag& vb) const
{
    auto a = values.begin();
    auto b = vb.values.begin();
    for ( ; a != values.end() && b != vb.values.end(); ++a, ++b)
        if (int res = (*a)->compare(**b))
            return res;
    if (a == values.end() && b == vb.values.end())
        return 0;
    if (a == values.end())
        return -1;
    return 1;
}

#if 0
bool ValueBag::contains(const ValueBag& vb) const
{
    // Both a and b are sorted, so we can iterate them linearly together
    auto a = values.begin();
    auto b = vb.values.begin();

    while (a != values.end())
    {
        // Nothing else wanted anymore
        if (b == vb.values.end()) return true;

        if ((*a)->name() < (*b)->name())
        {
            // This value is not in the match expression
            ++a;
            continue;
        }

        if ((*b)->name() < (*a)->name())
            // This value is wanted but we don't have it
            return false;

        if ((*a)->compare_values(**b) != 0)
            // Same key, check if the value is the same
            return false;

        // If also the value is the same, move on to the next item
        ++a;
        ++b;
    }
    // We got to the end of a.  If there are still things in b, we don't
    // match.  If we are also to the end of b, then we matched everything
    return b == vb.values.end();
}
#endif

bool ValueBag::operator==(const ValueBag& vb) const
{
    auto a = values.begin();
    auto b = vb.values.begin();
    for ( ; a != values.end() && b != vb.values.end(); ++a, ++b)
        if (**a != **b)
            return false;
    return a == values.end() && b == vb.values.end();
}

const values::Value* ValueBag::get(const std::string& key) const
{
    for (const auto& i: values)
        if (i->name() == key)
            return i;
    return nullptr;
}

void ValueBag::update(const ValueBag& vb)
{
    for (const auto& v: vb.values)
        values::Values::set(v->clone());
}

void ValueBag::encode(core::BinaryEncoder& enc) const
{
    for (const auto& v: values)
        v->encode(enc);
}

std::string ValueBag::toString() const
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
        res += i->toString();
    }
    return res;
}

void ValueBag::serialise(structured::Emitter& e) const
{
    e.start_mapping();
    for (const auto& i: values)
        i->serialise(e);
    e.end_mapping();
}

ValueBag ValueBag::decode(core::BinaryDecoder& dec)
{
    ValueBag res;
    while (dec)
        res.set(values::Value::decode(dec));
    return res;
}

ValueBag ValueBag::decode_reusing_buffer(core::BinaryDecoder& dec)
{
    ValueBag res;
    while (dec)
        res.set(values::Value::decode_reusing_buffer(dec));
    return res;
}

ValueBag ValueBag::parse(const std::string& str)
{
	// Parsa key\s*=\s*val
	// Parsa \s*,\s*
	// Loop
	ValueBag res;
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
        std::unique_ptr<values::Value> val(values::Value::parse(key, str.substr(cur), lenParsed));

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

ValueBag ValueBag::parse(const structured::Reader& reader)
{
    ValueBag res;
    reader.items("values", [&](const std::string& key, const structured::Reader& val) {
        switch (val.type())
        {
            case structured::NodeType::NONE:
                break;
            case structured::NodeType::INT:
                res.set(key, val.as_int("int value"));
                break;
            case structured::NodeType::STRING:
                res.set(key, val.as_string("string value"));
                break;
            default:
                throw std::runtime_error("cannot decode value " + key + ": value is neither integer nor string");
        }
    });
    return res;
}

#ifdef HAVE_LUA
void ValueBag::load_lua_table(lua_State* L, int idx)
{
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
                set(key, lua_tonumber(L, -1));
                break;
            case LUA_TSTRING:
                set(key, lua_tostring(L, -1));
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
}
#endif

bool ValueBagMatcher::is_subset(const values::Values& vb) const
{
    // Both a and b are sorted, so we can iterate them linearly together
    auto a = values.begin();
    auto b = vb.values.begin();

    while (b != vb.values.end())
    {
        // Nothing else wanted anymore
        if (a == values.end()) return true;

        if ((*a)->name() < (*b)->name())
            // This value is wanted but we don't have it
            return false;

        if ((*b)->name() < (*a)->name())
        {
            // This value is not in the match expression
            ++b;
            continue;
        }

        if ((*a)->compare_values(**b) != 0)
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
        res += i->toString();
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
        // TODO: reorganise the hierarchy to have int/string accessors not yet templatized?
        if (auto vs = dynamic_cast<const values::EncodedInt<values::ReuseAllocator>*>(i))
        {
            lua_pushnumber(L, vs->value());
        } else if (auto vs = dynamic_cast<const values::EncodedString<values::ReuseAllocator>*>(i)) {
            std::string val = vs->toString();
            lua_pushlstring(L, val.data(), val.size());
        } else {
            std::string val = i->toString();
            lua_pushlstring(L, val.data(), val.size());
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
        std::unique_ptr<values::Value> val(values::Value::parse(key, str.substr(cur), lenParsed));

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
