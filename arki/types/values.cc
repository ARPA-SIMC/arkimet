#include "values.h"
#include "arki/libconfig.h"
#include "arki/exceptions.h"
#include "arki/core/binary.h"
#include "arki/utils/string.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include <memory>
#include <cstdlib>
#include <cctype>
#include <cstdio>

#ifdef HAVE_LUA
extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}
#endif

using namespace std;
using namespace arki::utils;

#if 0
static void dump(const char* name, const std::string& str)
{
	fprintf(stderr, "%s ", name);
	for (string::const_iterator i = str.begin(); i != str.end(); ++i)
	{
		fprintf(stderr, "%02x ", (int)(unsigned char)*i);
	}
	fprintf(stderr, "\n");
}
static void dump(const char* name, const unsigned char* str, int len)
{
	fprintf(stderr, "%s ", name);
	for (int i = 0; i < len; ++i)
	{
		fprintf(stderr, "%02x ", str[i]);
	}
	fprintf(stderr, "\n");
}
#endif

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
	if (str.find('\0', 0) != string::npos)
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


/**
 * Implementation of Value methods common to all types
 */
template<typename TYPE>
class Common : public Value
{
protected:
    TYPE m_val;

public:
    Common(const std::string& name, const TYPE& val) : Value(name), m_val(val) {}

    bool operator==(const Value& v) const override
    {
        if (name() != v.name()) return false;
        const Common<TYPE>* vi = dynamic_cast<const Common<TYPE>*>(&v);
        if (!vi) return false;
        return m_val == vi->m_val;
    }

    int compare(const Value& v) const override
    {
        if (name() < v.name()) return -1;
        if (name() > v.name()) return 1;
        return 0;
    }

    std::string toString() const override
    {
        stringstream ss;
        ss << m_val;
        return ss.str();
    }

    void encode(core::BinaryEncoder& enc) const override
    {
        // Key length
        enc.add_unsigned(m_name.size(), 1);
        // Key
        enc.add_raw(m_name);
    }
};

struct Integer : public Common<int>
{
    Integer(const std::string& name, const int& val) : Common<int>(name, val) {}

    int compare(const Value& v) const override
    {
        if (int res = Common<int>::compare(v)) return res;
        if (const Integer* v1 = dynamic_cast<const Integer*>(&v))
            return m_val - v1->m_val;
        else
            return sortKey() - v.sortKey();
    }

    bool value_equals(const Value& v) const override
    {
        if (const Integer* v1 = dynamic_cast<const Integer*>(&v))
            return m_val == v1->m_val;
        else
            return false;
    }

    int sortKey() const override { return 1; }

    int toInt() const { return m_val; }

    void encode(core::BinaryEncoder& enc) const override
    {
        Common<int>::encode(enc);
        if (m_val >= -32 && m_val < 31)
        {
            // If it's a small one, encode in the remaining 6 bits
            uint8_t encoded = { ENC_SINT6 << 6 };
            if (m_val < 0)
            {
                encoded |= ((~(-m_val) + 1) & 0x3f);
            } else {
                encoded |= (m_val & 0x3f);
            }
            enc.add_raw(&encoded, 1u);
        }
        else 
        {
            // Else, encode as an integer Number

            // Type
            uint8_t type = (ENC_NUMBER << 6) | (ENC_NUM_INTEGER << 4);
            // Value to encode
            unsigned int val;
			if (m_val < 0)
			{
				// Sign bit
				type |= 0x8;
				val = -m_val;
			}
			else
				val = m_val;
			// Number of bytes
			unsigned nbytes;
			// TODO: add bits for 64 bits here if it's ever needed
			if (val & 0xff000000)
				nbytes = 4;
			else if (val & 0x00ff0000)
				nbytes = 3;
			else if (val & 0x0000ff00)
				nbytes = 2;
			else if (val & 0x000000ff)
				nbytes = 1;
            else
                throw std::runtime_error("cannot encode integer number: value " + to_string(val) + " is too large to be encoded");

            type |= (nbytes-1);
            enc.add_raw(&type, 1u);
            enc.add_unsigned(val, nbytes);
        }
    }

    static Integer* parse(const std::string& name, const std::string& str);

    void serialise(structured::Emitter& e) const override
    {
        e.add_int(m_val);
    }

    Value* clone() const override { return new Integer(m_name, m_val); }
};

Integer* Integer::parse(const std::string& name, const std::string& str)
{
    return new Integer(name, atoi(str.c_str()));
}


struct String : public Common<std::string>
{
    String(const std::string& name, const std::string& val) : Common<std::string>(name, val) {}

    int sortKey() const override { return 2; }

    int compare(const Value& v) const override
    {
        if (int res = Common<std::string>::compare(v)) return res;
        if (const String* v1 = dynamic_cast<const String*>(&v))
        {
			if (m_val < v1->m_val) return -1;
			if (m_val > v1->m_val) return 1;
			return 0;
		}
		else
			return sortKey() - v.sortKey();
    }

    bool value_equals(const Value& v) const override
    {
        if (const String* v1 = dynamic_cast<const String*>(&v))
            return m_val == v1->m_val;
        else
            return false;
    }

    void encode(core::BinaryEncoder& enc) const override
    {
        Common<std::string>::encode(enc);
        if (m_val.size() < 64)
        {
            uint8_t type = ENC_NAME << 6;
            type |= m_val.size() & 0x3f;
            enc.add_raw(&type, 1u);
            enc.add_raw(m_val);
        }
        else
            // TODO: if needed, here we implement another string encoding type
            throw_consistency_error("encoding short string", "string '"+m_val+"' is too long: the maximum length is 63 characters, but the string is " + to_string(m_val.size()) + " characters long");
    }

    std::string toString() const override
    {
		int idummy;

		if (parsesAsNumber(m_val, idummy) || needsQuoting(m_val))
		{
			// If it is surrounded by double quotes or it parses as a number, we need to escape it
			return "\"" + str::encode_cstring(m_val) + "\"";
		} else {
			// Else, we can use the value as it is
			return m_val;
		}
	}

    void serialise(structured::Emitter& e) const override
    {
        e.add_string(m_val);
    }

    Value* clone() const override { return new String(m_name, m_val); }
};

Value* Value::decode(const std::string& name, core::BinaryDecoder& dec)
{
    uint8_t lead = dec.pop_byte("valuebag value type");
    switch ((lead >> 6) & 0x3)
    {
        case ENC_SINT6:
            if (lead & 0x20)
                return new Integer(name, -((~(lead-1)) & 0x3f));
            else
                return new Integer(name, lead & 0x3f);
        case ENC_NUMBER: {
            switch ((lead >> 4) & 0x3)
            {
                case ENC_NUM_INTEGER: {
                    // Sign in the next bit.  Number of bytes in the next 3 bits.
                    unsigned nbytes = (lead & 0x7) + 1;
                    unsigned val = dec.pop_uint(nbytes, "integer number value");
                    return new Integer(name, (lead & 0x8) ? -val : val);
                }
                case ENC_NUM_FLOAT:
                    throw std::runtime_error("cannot decode value: the number value to decode is a floating point number, but decoding floating point numbers is not currently implemented");
                case ENC_NUM_UNUSED:
                    throw std::runtime_error("cannot decode value: the number value to decode has an unknown type");
                case ENC_NUM_EXTENDED:
                    throw std::runtime_error("cannot decode value: the number value to decode has an extended type, but no extended type is currently implemented");
                default:
                    throw std::runtime_error("cannot decode value: control flow should never reach here (" __FILE__ ":" + to_string(__LINE__) + "), but the compiler cannot easily know it.  This is here to silence a compiler warning.");
            }
        }
        case ENC_NAME: {
            unsigned size = lead & 0x3f;
            return new String(name, dec.pop_string(size, "valuebag string value"));
        }
        case ENC_EXTENDED:
            throw std::runtime_error("cannot decode value: the encoded value has an extended type, but no extended type is currently implemented");
        default:
            throw std::runtime_error("cannot decode value: control flow should never reach here (" __FILE__ ":" + to_string(__LINE__) + "), but the compiler cannot easily know it.  This is here to silence a compiler warning.");
    }
}

Value* Value::parse(const std::string& name, const std::string& str)
{
	size_t dummy;
	return Value::parse(name, str, dummy);
}

Value* Value::parse(const std::string& name, const std::string& str, size_t& lenParsed)
{
    size_t begin = skipSpaces(str, 0);

    // Handle the empty string
    if (begin == str.size())
    {
        lenParsed = begin;
        return new String(name, string());
    }

	// Handle the quoted string
	if (str[begin] == '"')
	{
		// Skip the first double quote
		++begin;

		// Unescape the string
		size_t parsed;
		string res = str::decode_cstring(str.substr(begin), parsed);

        lenParsed = skipSpaces(str, begin + parsed);
        return new String(name, res);
    }

	// No quoted string, so we can terminate the token at the next space, ',' or ';'
	size_t end = begin;
	while (end != str.size() && !isspace(str[end]) && str[end] != ',' && str[end] != ';')
		++end;
	string res = str.substr(begin, end-begin);
	lenParsed = skipSpaces(str, end);

    // If it can be parsed as a number, with maybe leading and trailing
    // spaces, return the number
    int val;
    if (parsesAsNumber(res, val))
        return new Integer(name, val);

    // Else return the string
    return new String(name, res);
}

Value* Value::create_integer(const std::string& name, int val) { return new Integer(name, val); }
Value* Value::create_string(const std::string& name, const std::string& val) { return new String(name, val); }

}


ValueBag::ValueBag() {}

ValueBag::~ValueBag()
{
    for (auto& i: values)
        delete i;
}

ValueBag::ValueBag(ValueBag&& vb)
    : values(std::move(vb.values))
{
}

ValueBag::ValueBag(const ValueBag& vb)
    : ValueBag()
{
    for (const auto& v: vb.values)
        values.emplace_back(v->clone());
}

ValueBag& ValueBag::operator=(const ValueBag& vb)
{
    // Handle the case a=a
    if (this == &vb) return *this;

    clear();
    for (const auto& v: vb.values)
        values.emplace_back(v->clone());

    return *this;
}

ValueBag& ValueBag::operator=(ValueBag&& vb)
{
    // Handle the case a=a
    if (this == &vb) return *this;

    values = std::move(vb.values);

    return *this;
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

        if (!(*a)->value_equals(**b))
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

bool ValueBag::operator==(const ValueBag& vb) const
{
    auto a = values.begin();
    auto b = vb.values.begin();
    for ( ; a != values.end() && b != vb.values.end(); ++a, ++b)
        if (**a != **b)
            return false;
    return a == values.end() && b == vb.values.end();
}

void ValueBag::clear()
{
    for (auto* i: values)
        delete i;
    values.clear();
}

const values::Value* ValueBag::get(const std::string& key) const
{
    for (const auto& i: values)
        if (i->name() == key)
            return i;
    return nullptr;
}

void ValueBag::set(values::Value* val)
{
    for (auto i = values.begin(); i != values.end(); ++i)
    {
        if ((*i)->name() == val->name())
        {
            delete *i;
            *i = val;
            return;
        } else if ((*i)->name() > val->name()) {
            values.emplace(i, val);
            return;
        }
    }

    values.emplace_back(val);
}

void ValueBag::update(const ValueBag& vb)
{
    for (const auto& v: vb.values)
        set(v->clone());
}

void ValueBag::encode(core::BinaryEncoder& enc) const
{
    for (const auto& v: values)
    {
        // Value
        v->encode(enc);
    }
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
        res += i->name();
        res += '=';
        res += i->toString();
    }
    return res;
}

void ValueBag::serialise(structured::Emitter& e) const
{
    e.start_mapping();
    for (const auto& i: values)
    {
        e.add(i->name());
        i->serialise(e);
    }
    e.end_mapping();
}

/**
 * Decode from compact binary representation
 */
ValueBag ValueBag::decode(core::BinaryDecoder& dec)
{
    ValueBag res;
    while (dec)
    {
        // Key length
        unsigned key_len = dec.pop_uint(1, "valuebag key length");

        // Key
        string key = dec.pop_string(key_len, "valuebag key");

        // Value
        res.set(values::Value::decode(key, dec));
    }
    return res;
}

/**
 * Parse from a string representation
 */
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
		if (cur == string::npos)
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
        unique_ptr<values::Value> val(values::Value::parse(key, str.substr(cur), lenParsed));

        // Set the value
        if (val.get())
            res.set(val.release());
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
void ValueBag::lua_push(lua_State* L) const
{
    lua_newtable(L);
    for (const auto& i: values)
    {
        const string& name = i->name();
        lua_pushlstring(L, name.data(), name.size());
        if (const values::Integer* vs = dynamic_cast<const values::Integer*>(i))
        {
            lua_pushnumber(L, vs->toInt());
        } else if (const values::String* vs = dynamic_cast<const values::String*>(i)) {
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

void ValueBag::load_lua_table(lua_State* L, int idx)
{
	// Make the table index absolute
	if (idx < 0) idx = lua_gettop(L) + idx + 1;

	// Iterate the table
	lua_pushnil(L);
	while (lua_next(L, idx))
	{
        // Get key
        string key;
        switch (lua_type(L, -2))
        {
            case LUA_TNUMBER: key = to_string((int)lua_tonumber(L, -2)); break;
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

}
}
