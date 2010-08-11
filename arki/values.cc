/*
 * values - Dynamic type system used by some types of arkimet metadata
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

#include <wibble/exception.h>
#include <arki/values.h>
#include <arki/utils/codec.h>
#include <cstdlib>
#include <cctype>
#include <cstdio>
#include "config.h"

#ifdef HAVE_LUA
extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils;
using namespace arki::utils::codec;

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

static std::string escape(const std::string& str)
{
	string res;
	for (string::const_iterator i = str.begin(); i != str.end(); ++i)
		if (*i == '\n')
			res += "\\n";
		else if (*i == '\t')
			res += "\\t";
		else if (*i == 0 || iscntrl(*i))
		{
			char buf[5];
			snprintf(buf, 5, "\\x%02x", (unsigned int)*i);
			res += buf;
		}
		else if (*i == '"' || *i == '\\')
		{
			res += "\\";
			res += *i;
		}
		else
			res += *i;
	return res;
}

static std::string unescape(const std::string& str, size_t& lenParsed)
{
	string res;
	string::const_iterator i = str.begin();
	for ( ; i != str.end() && *i != '"'; ++i)
		if (*i == '\\' && (i+1) != str.end())
		{
			switch (*(i+1))
			{
				case 'n': res += '\n'; break;
				case 't': res += '\t'; break;
				case 'x': {
					char buf[5] = "0x\0\0";
					// Read up to 2 extra hex digits
					for (size_t j = 0; j < 2 && i+2+j != str.end() && isxdigit(*(i+2+j)); ++j)
						buf[2+j] = *(i+2+j);
					res += (char)atoi(buf);
					break;
				}
				default:
					res += *(i+1);
					break;
			}
			++i;
		} else
			res += *i;
	if (i != str.end() && *i == '"')
		++i;
	lenParsed = i - str.begin();
	return res;
}

static inline size_t skipSpaces(const std::string& str, size_t cur)
{
	while (cur < str.size() && isspace(str[cur]))
		++cur;
	return cur;
}


namespace arki {
namespace value {

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
	Common(const TYPE& val) : m_val(val) {}

	virtual bool operator==(const Value& v) const
	{
		const Common<TYPE>* vi = dynamic_cast<const Common<TYPE>*>(&v);
		if (vi == 0)
			return false;
		return m_val == vi->m_val;
	}
	virtual bool operator<(const Value& v) const
	{
		const Common<TYPE>* vi = dynamic_cast<const Common<TYPE>*>(&v);
		if (vi == 0)
			return false;
		return m_val < vi->m_val;
	}
	virtual std::string toString() const
	{
		return str::fmt(m_val);
	}
};

struct Integer : public Common<int>
{
	Integer(const int& val) : Common<int>(val) {}

	virtual int compare(const Value& v) const
	{
		if (const Integer* v1 = dynamic_cast< const Integer* >(&v))
			return m_val - v1->m_val;
		else
			return sortKey() - v.sortKey();
	}

	virtual int sortKey() const { return 1; }

	int toInt() const { return m_val; }

	/**
	 * Encode into a compact binary representation
	 */
	virtual void encode(Encoder& enc) const
	{
		if (m_val >= -32 && m_val < 31)
		{
			// If it's a small one, encode in the remaining 6 bits
			unsigned char encoded = { ENC_SINT6 << 6 };
			if (m_val < 0)
			{
				encoded |= ((~(-m_val) + 1) & 0x3f);
			} else {
				encoded |= (m_val & 0x3f);
			}
			enc.addString((const char*)&encoded, 1u);
		}
		else 
		{
			// Else, encode as an integer Number

			// Type
			unsigned char type = (ENC_NUMBER << 6) | (ENC_NUM_INTEGER << 4);
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
				throw wibble::exception::Consistency("encoding integer number", "value " + str::fmt(val) + " is too large to be encoded");
				
			type |= (nbytes-1);
			enc.addString((const char*)&type, 1u);
			enc.addUInt(val, nbytes);
		}
	}

	static Integer* parse(const std::string& str);

	Value* clone() const { return new Integer(m_val); }
};

Integer* Integer::parse(const std::string& str)
{
	return new Integer(atoi(str.c_str()));
}


struct String : public Common<std::string>
{
	String(const std::string& val) : Common<std::string>(val) {}

	virtual int sortKey() const { return 2; }

	virtual int compare(const Value& v) const
	{
		if (const String* v1 = dynamic_cast< const String* >(&v))
		{
			if (m_val < v1->m_val) return -1;
			if (m_val > v1->m_val) return 1;
			return 0;
		}
		else
			return sortKey() - v.sortKey();
	}

	virtual void encode(Encoder& enc) const
	{
		if (m_val.size() < 64)
		{
			unsigned char type = ENC_NAME << 6;
			type |= m_val.size() & 0x3f;
			enc.addString((const char*)&type, 1u);
			enc.addString(m_val);
		}
		else
			// TODO: if needed, here we implement another string encoding type
			throw wibble::exception::Consistency("encoding short string", "string '"+m_val+"' is too long: the maximum length is 63 characters, but the string is " + str::fmt(m_val.size()) + " characters long");
	}

	virtual std::string toString() const
	{
		int idummy;

		if (parsesAsNumber(m_val, idummy) || needsQuoting(m_val))
		{
			// If it is surrounded by double quotes or it parses as a number, we need to escape it
			return "\"" + escape(m_val) + "\"";
		} else {
			// Else, we can use the value as it is
			return m_val;
		}
	}

	Value* clone() const { return new String(m_val); }
};

}

Value* Value::decode(const void* buf, size_t len, size_t& used)
{
	using namespace codec;

	unsigned char* s = (unsigned char*)buf;

	ensureSize(len, 1, "value type");
	switch ((*s >> 6) & 0x3)
	{
		case value::ENC_SINT6:
			used = 1;
			if (*s & 0x20)
				return new value::Integer(-((~(*s-1)) & 0x3f));
			else
				return new value::Integer(*s & 0x3f);
		case value::ENC_NUMBER: {
			switch ((*s >> 4) & 0x3)
			{
				case value::ENC_NUM_INTEGER: {
					// Sign in the next bit.  Number of bytes in the next 3 bits.
					unsigned nbytes = (*s & 0x7) + 1;
					ensureSize(len, 1+nbytes, "integer number value");
					unsigned val = decodeUInt(s+1, nbytes);
					used = 1+nbytes;
					return new value::Integer((*s & 0x8) ? -val : val);
				}
				case value::ENC_NUM_FLOAT:
					throw wibble::exception::Consistency("decoding value", "the number value to decode is a floating point number, but decoding floating point numbers is not currently implemented");
				case value::ENC_NUM_UNUSED:
					throw wibble::exception::Consistency("decoding value", "the number value to decode has an unknown type");
				case value::ENC_NUM_EXTENDED:
					throw wibble::exception::Consistency("decoding value", "the number value to decode has an extended type, but no extended type is currently implemented");
				default:
					throw wibble::exception::Consistency("decoding value", "control flow should never reach here (" __FILE__ ":" + str::fmt(__LINE__) + "), but the compiler cannot easily know it.  This is here to silence a compiler warning.");
			}
		}
		case value::ENC_NAME: {
			unsigned size = *s & 0x3f;
			ensureSize(len, 1+size, "string value");
			used = 1+size;
			return new value::String(string((char*)s+1, size));
		}
		case value::ENC_EXTENDED:
			throw wibble::exception::Consistency("decoding value", "the encoded value has an extended type, but no extended type is currently implemented");
		default:
			throw wibble::exception::Consistency("decoding value", "control flow should never reach here (" __FILE__ ":" + str::fmt(__LINE__) + "), but the compiler cannot easily know it.  This is here to silence a compiler warning.");
	}
}

Value* Value::parse(const std::string& str)
{
	size_t dummy;
	return Value::parse(str, dummy);
}

Value* Value::parse(const std::string& str, size_t& lenParsed)
{
	size_t begin = skipSpaces(str, 0);

	// Handle the empty string
	if (begin == str.size())
	{
		lenParsed = begin;
		return new value::String(string());
	}

	// Handle the quoted string
	if (str[begin] == '"')
	{
		// Skip the first double quote
		++begin;

		// Unescape the string
		size_t parsed;
		string res = unescape(str.substr(begin), parsed);

		lenParsed = skipSpaces(str, begin + parsed);
		return new value::String(res);
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
		return new value::Integer(val);

	// Else return the string
	return new value::String(res);
}

Value* Value::createInteger(int val)
{
	return new value::Integer(val);
}
Value* Value::createString(const std::string& val)
{
	return new value::String(val);
}

ValueBag::ValueBag() {}

ValueBag::~ValueBag()
{
	// Deallocate all the Value pointers
	for (iterator i = begin(); i != end(); ++i)
		if (i->second)
			delete i->second;
}

ValueBag::ValueBag(const ValueBag& vb)
{
	iterator inspos = begin();
	for (ValueBag::const_iterator i = vb.begin(); i != vb.end(); ++i)
		inspos = insert(inspos, make_pair(i->first, i->second->clone()));
}

ValueBag& ValueBag::operator=(const ValueBag& vb)
{
	// Handle the case a=a
	if (this == &vb)
		return *this;

	clear();
	
	// Fill up again with the new values
	iterator inspos = begin();
	for (ValueBag::const_iterator i = vb.begin(); i != vb.end(); ++i)
		inspos = insert(inspos, make_pair(i->first, i->second->clone()));
	return *this;
}

bool ValueBag::operator<(const ValueBag& vb) const
{
	const_iterator a = begin();
	const_iterator b = vb.begin();
	for ( ; a != end() && b != vb.end(); ++a, ++b)
	{
		if (a->first < b->first)
			return true;
		if (a->first != b->first)
			return false;
		if (*a->second < *b->second)
			return true;
		if (*a->second != *b->second)
			return false;
	}
	if (a == end() && b == vb.end())
		return false;
	if (a == end())
		return true;
	return false;
}

int ValueBag::compare(const ValueBag& vb) const
{
	const_iterator a = begin();
	const_iterator b = vb.begin();
	for ( ; a != end() && b != vb.end(); ++a, ++b)
	{
		if (a->first < b->first)
			return -1;
		if (a->first > b->first)
			return 1;
		if (int res = a->second->compare(*b->second)) return res;
	}
	if (a == end() && b == vb.end())
		return 0;
	if (a == end())
		return -1;
	return 1;
}

bool ValueBag::contains(const ValueBag& vb) const
{
	// Both a and b are sorted, so we can iterate them linearly together

	ValueBag::const_iterator a = begin();
	ValueBag::const_iterator b = vb.begin();

	while (a != end())
	{
		// Nothing else wanted anymore
		if (b == vb.end())
			return true;
		if (a->first < b->first)
			// This value is not in the match expression
			++a;
		else if (b->first < a->first)
			// This value is wanted but we don't have it
			return false;
		else if (*a->second != *b->second)
			// Same key, check if the value is the same
			return false;
		else
		{
			// If also the value is the same, move on to the next item
			++a;
			++b;
		}
	}
	// We got to the end of a.  If there are still things in b, we don't
	// match.  If we are also to the end of b, then we matched everything
	return b == vb.end();
}

bool ValueBag::operator==(const ValueBag& vb) const
{
	const_iterator a = begin();
	const_iterator b = vb.begin();
	for ( ; a != end() && b != vb.end(); ++a, ++b)
		if (a->first != b->first || *a->second != *b->second)
			return false;
	return a == end() && b == vb.end();
}

void ValueBag::clear()
{
	// Deallocate all the Value pointers
	for (iterator i = begin(); i != end(); ++i)
		if (i->second)
			delete i->second;

	// Empty the map
	map<string, Value*>::clear();
}

const Value* ValueBag::get(const std::string& key) const
{
	const_iterator i = find(key);
	if (i == end())
		return 0;
	return i->second;
}

void ValueBag::set(const std::string& key, Value* val)
{
	iterator i = find(key);
	if (i == end())
		insert(make_pair(key, val));
	else
	{
		if (i->second)
			delete i->second;
		i->second = val;
	}
}

void ValueBag::update(const ValueBag& vb)
{
	for (ValueBag::const_iterator i = vb.begin();
			i != vb.end(); ++i)
		set(i->first, i->second->clone());
}

void ValueBag::encode(Encoder& enc) const
{
	for (const_iterator i = begin(); i != end(); ++i)
	{
		// Key length
		enc.addUInt(i->first.size(), 1);
		// Key
		enc.addString(i->first);
		// Value
		i->second->encode(enc);
	}
}

std::string ValueBag::toString() const
{
	string res;
	for (const_iterator i = begin(); i != end(); ++i)
	{
		if (i != begin())
			res += ", ";
		res += i->first;
		res += '=';
		res += i->second->toString();
	}
	return res;
}

/**
 * Decode from compact binary representation
 */
ValueBag ValueBag::decode(const void* buf, size_t len)
{
	using namespace codec;

	ValueBag res;
	const unsigned char* start = (const unsigned char*)buf;
	for (const unsigned char* cur = start; cur < start+len; )
	{
		// Key length
		ensureSize(len, cur-start+1, "Key");
		unsigned keyLen = decodeUInt(cur, 1);
		cur += 1;

		// Key
		ensureSize(len, cur-start+keyLen, "Key");
		string key((const char*)cur, keyLen);
		cur += keyLen;

		// Value
		size_t used;
		res.set(key, Value::decode(cur, len-(cur-start), used));
		cur += used;
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
				throw wibble::exception::Consistency("parsing key=value list", "found invalid extra characters \""+str.substr(begin)+"\" at the end of the list");
			break;
		}
			
		// Read the key
		string key = str::trim(str.substr(begin, cur-begin));

		// Skip the '=' sign
		++cur;

		// Skip spaces after the '='
		cur = skipSpaces(str, cur);

		// Parse the value
		size_t lenParsed;
		auto_ptr<Value> val(Value::parse(str.substr(cur), lenParsed));

		// Set the value
		if (val.get())
			res.set(key, val.release());
		else
			throw wibble::exception::Consistency("parsing key=value list", "cannot parse value at \""+str.substr(cur)+"\"");

		// Move on to the next one
		begin = cur + lenParsed;

		// Skip separators
		while (begin != str.size() && (isspace(str[begin]) || str[begin] == ','))
			++begin;
	}

	return res;
}

#ifdef HAVE_LUA
void ValueBag::lua_push(lua_State* L) const
{
	lua_newtable(L);
	for (const_iterator i = begin(); i != end(); ++i)
	{
		string name = i->first;
		lua_pushlstring(L, name.data(), name.size());
		if (const value::Integer* vs = dynamic_cast<const value::Integer*>(i->second))
		{
			lua_pushnumber(L, vs->toInt());
		} else if (const value::String* vs = dynamic_cast<const value::String*>(i->second)) {
			string val = vs->toString();
			lua_pushlstring(L, val.data(), val.size());
		} else {
			string val = i->second->toString();
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
			case LUA_TNUMBER: key = str::fmt(lua_tointeger(L, -2)); break;
			case LUA_TSTRING: key = lua_tostring(L, -2); break;
			default:
				throw wibble::exception::Consistency("reading Lua table",
						str::fmtf("key has type %s but only ints and strings are supported",
							lua_typename(L, lua_type(L, -2))));
		}
		// Get value
		switch (lua_type(L, -1))
		{
			case LUA_TNUMBER: 
				set(key, Value::createInteger(lua_tointeger(L, -1)));
				break;
			case LUA_TSTRING:
				set(key, Value::createString(lua_tostring(L, -1)));
				break;
			default:
				throw wibble::exception::Consistency("reading Lua table",
						str::fmtf("value has type %s but only ints and strings are supported",
							lua_typename(L, lua_type(L, -1))));
		}
		lua_pop(L, 1);
	}
	lua_pop(L, 1);
}
#endif

#if 0

template<typename Base>
struct DataKeyVal : Base
{
	virtual bool match(const std::map<std::string, int>& wanted) const
	{
		// Both a and b are sorted, so we can iterate them linerly together

		values_t::const_iterator a = values.begin();
		std::map<std::string, int>::const_iterator b = wanted.begin();

		while (a != values.end())
		{
			// Nothing else wanted anymore
			if (b == wanted.end())
				return true;
			if (a->first < b->first)
				// This value is not in the match expression
				++a;
			else if (b->first < a->first)
				// This value is wanted but we don't have it
				return false;
			else if (a->second != b->second)
				// Same key, check if the value is the same
				return false;
			else
			{
				// If also the value is the same, move on to the next item
				++a;
				++b;
			}
		}
		// We got to the end of a.  If there are still things in b, we don't
		// match.  If we are also to the end of b, then we matched everything
		return b == wanted.end();
	}
};
#endif

}
// vim:set ts=4 sw=4:
