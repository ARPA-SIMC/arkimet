#ifndef ARKI_VALUES_H
#define ARKI_VALUES_H

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

#include <string>
#include <map>
#include <iosfwd>

struct lua_State;

namespace arki {
namespace utils {
namespace codec {
struct Encoder;
}
}

/**
 * Base class for generic scalar values.
 *
 * This is needed in order to store arbitrary types for the key=value kind of
 * types (like Area and Ensemble).
 *
 * In practice, we have to create a dynamic type system.
 */
class Value
{
public:
	virtual ~Value() {}

	virtual bool operator==(const Value& v) const = 0;
	virtual bool operator!=(const Value& v) const { return !operator==(v); }
	virtual bool operator<(const Value& v) const = 0;
	virtual int compare(const Value& v) const = 0;

	// Provide a unique sorting key to allow sorting of values of different
	// types.
	virtual int sortKey() const = 0;

	virtual Value* clone() const = 0;

	/**
	 * Encode into a compact binary representation
	 */
	virtual void encode(utils::codec::Encoder& enc) const = 0;

	/**
	 * Decode from compact binary representation.
	 *
	 * @retval used The number of bytes decoded.
	 */
	static Value* decode(const void* buf, size_t len, size_t& used);

	/**
	 * Encode into a string representation
	 */
	virtual std::string toString() const = 0;

	/**
	 * Parse from a string representation
	 */
	static Value* parse(const std::string& str);

	/**
	 * Parse from a string representation
	 */
	static Value* parse(const std::string& str, size_t& lenParsed);

	static Value* createInteger(int val);
	static Value* createString(const std::string& val);
};

struct ValueBag : public std::map<std::string, Value*>
{
	ValueBag();
	ValueBag(const ValueBag& vb);
	~ValueBag();

	ValueBag& operator=(const ValueBag& vb);
	bool operator<(const ValueBag& vb) const;
	bool operator==(const ValueBag& vb) const;
	bool operator!=(const ValueBag& vb) const { return !operator==(vb); }
	int compare(const ValueBag& vb) const;

	bool contains(const ValueBag& vb) const;

	void clear();

	/**
	 * Gets a value.
	 *
	 * It returns 0 if the value is not found.
	 */
	const Value* get(const std::string& key) const;

	/**
	 * Sets a value.
	 *
	 * It takes ownership of the Value pointer.
	 */
	void set(const std::string& key, Value* val);

	/**
	 * Encode into a compact binary representation
	 */
	void encode(utils::codec::Encoder& enc) const;

	/**
	 * Decode from compact binary representation
	 */
	static ValueBag decode(const void* buf, size_t len);

	/**
	 * Encode into a string representation
	 */
	std::string toString() const;

	/**
	 * Parse from a string representation
	 */
	static ValueBag parse(const std::string& str);

	/// Push to the LUA stack a table with the data of this ValueBag
	void lua_push(lua_State* L) const;

private:
	// Disable modifying subscription, because it'd be hard to deallocate the
	// old value
	Value*& operator[](const std::string& str);
};

static inline std::ostream& operator<<(std::ostream& o, const Value& v)
{
	return o << v.toString();
}
static inline std::ostream& operator<<(std::ostream& o, const ValueBag& v)
{
	return o << v.toString();
}

}

// vim:set ts=4 sw=4:
#endif
