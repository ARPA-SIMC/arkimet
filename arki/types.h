#ifndef ARKI_TYPES_H
#define ARKI_TYPES_H

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

#include <wibble/exception.h>
#include <arki/refcounted.h>

#include <string>
#include <iosfwd>
#include <map>

struct lua_State;

namespace wibble {
namespace sys {
struct Buffer;
}
}

namespace arki {
namespace utils {
namespace codec {
struct Encoder;
}
}

namespace types {
struct Type;

/// Identifier codes used for binary serialisation
enum Code
{
	TYPE_INVALID = 0,
	TYPE_ORIGIN = 1,
	TYPE_PRODUCT = 2,
	TYPE_LEVEL = 3,
	TYPE_TIMERANGE = 4,
	TYPE_REFTIME = 5,
	TYPE_NOTE = 6,
	TYPE_SOURCE = 7,
	TYPE_ASSIGNEDDATASET = 8,
	TYPE_AREA = 9,
	TYPE_ENSEMBLE = 10,
	TYPE_SUMMARYITEM = 11,
	TYPE_SUMMARYSTATS = 12,
	TYPE_TIME = 13,
	TYPE_BBOX = 14,
	TYPE_RUN = 15,
	TYPE_MAXCODE
};

Code parseCodeName(const std::string& name);
std::string formatCode(const Code& c);
static inline std::ostream& operator<<(std::ostream& o, const Code& c) { return o << formatCode(c); }

}

/**
 * Arkimet metadata item.
 *
 * All metadata items are immutable, and can only be assigned to.
 */
template< typename TYPE = types::Type >
struct UItem : public refcounted::Pointer<TYPE>
{
	UItem() : refcounted::Pointer<TYPE>() {}
	UItem(TYPE* ptr) : refcounted::Pointer<TYPE>(ptr) {}
	UItem(const UItem<TYPE>& val) : refcounted::Pointer<TYPE>(val) {}
	template<typename TYPE1>
	UItem(const UItem<TYPE1>& val) : refcounted::Pointer<TYPE>(val) {}
	~UItem() {}

	/// Check if we have a value or if we are undefined
	bool defined() const { return this->m_ptr != 0; }

	/// Set the pointer to 0
	void clear()
	{
		if (this->m_ptr && this->m_ptr->unref()) delete this->m_ptr;
		this->m_ptr = 0;
	}

	/// Comparison
	int compare(const UItem<TYPE>& o) const
	{
		if (this->m_ptr == 0 && o.m_ptr != 0) return -1;
		if (this->m_ptr == 0 && o.m_ptr == 0) return 0;
		if (this->m_ptr != 0 && o.m_ptr == 0) return 1;
		return this->m_ptr->compare(*o.m_ptr);
	}
	bool operator<(const UItem<TYPE>& o) const { return compare(o) < 0; }
	bool operator<=(const UItem<TYPE>& o) const { return compare(o) <= 0; }
	bool operator>(const UItem<TYPE>& o) const { return compare(o) > 0; }
	bool operator>=(const UItem<TYPE>& o) const { return compare(o) >= 0; }

	/// Equality
	bool operator==(const UItem<TYPE>& o) const
	{
		if (this->m_ptr == 0 && o.m_ptr == 0)
			return true;
		if (this->m_ptr == 0 || o.m_ptr == 0)
			return false;
		return *this->m_ptr == *o.m_ptr;
	}
	bool operator!=(const UItem<TYPE>& o) const { return !operator==(o); }

	/// Encoding to compact binary representation
	std::string encode() const
	{
		if (this->m_ptr)
			return this->m_ptr->encodeWithEnvelope();
		else
			return std::string();
	}

	/// Checked conversion to a more specific Item
	template<typename TYPE1>
	UItem<TYPE1> upcast() const
	{
		if (!this->m_ptr) return UItem<TYPE1>();

		return UItem<TYPE1>(this->m_ptr->upcast<TYPE1>());
	}
};

/// Write as a string to an output stream
template<typename T>
std::ostream& operator<<(std::ostream& o, const UItem<T>& i)
{
	if (i.defined())
		return i->writeToOstream(o);
	else
		return o << std::string("(none)");
}

/**
 * Same as UItem, but it cannot contain an undefined value.
 */
template< typename TYPE = types::Type >
struct Item : public UItem<TYPE>
{
	Item(TYPE* ptr) : UItem<TYPE>(ptr) {}
	Item(const Item<TYPE>& val) : UItem<TYPE>(val) {}
	Item(const UItem<TYPE>& val) : UItem<TYPE>(val) { this->ensureValid(); }
	template<typename TYPE1>
	Item(const Item<TYPE1>& val) : UItem<TYPE>(val) {}
	template<typename TYPE1>
	Item(const UItem<TYPE1>& val) : UItem<TYPE>(val) { this->ensureValid(); }

protected:
	Item() : UItem<TYPE>() {}
};

/// Write as a string to an output stream
template<typename T>
std::ostream& operator<<(std::ostream& o, const Item<T>& i)
{
	return i->writeToOstream(o);
}


namespace types {

/**
 * Base class for implementing arkimet metadata types
 */
struct Type : public refcounted::Base
{
	virtual ~Type() {}

	/// Comparison (<0 if <, 0 if =, >0 if >)
	virtual int compare(const Type& o) const;

	/// Equality
	virtual bool operator==(const Type& o) const = 0;

	/**
	 * Tag to identify this metadata item.
	 *
	 * This is used, for example, when creating error messages, or to identify
	 * the item when serialising to YAML.
	 */
	virtual std::string tag() const = 0;

	/// Serialisation code
	virtual types::Code serialisationCode() const = 0;

	/// Length in bytes of the size field when serialising
	virtual size_t serialisationSizeLength() const = 0;

	/**
	 * Encoding to compact binary representation, without identification
	 * envelope
	 */
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const = 0;

	/**
	 * Encode to compact binary representation, without identification
	 * envelope
	 */
	virtual std::string encodeWithEnvelope() const;

	/// Write as a string to an output stream
	virtual std::ostream& writeToOstream(std::ostream& o) const = 0;

	/**
	 * Return a matcher query (without the metadata type prefix) that
	 * exactly matches this metadata item
	 */
	virtual std::string exactQuery() const;

	/// Push to the LUA stack a userdata to access this item
	virtual void lua_push(lua_State* L) const = 0;
};

/**
 * This class is used to register types with the arkimet metadata type system.
 *
 * Registration is done by declaring a static RegisterItem object, passing the
 * metadata details in the constructor.
 */
struct MetadataType
{
	typedef Item<Type> (*item_decoder)(const unsigned char* start, size_t len);
	typedef Item<Type> (*string_decoder)(const std::string& val);

	types::Code serialisationCode;
	int serialisationSizeLen;
	std::string tag;
	item_decoder decode_func;
	string_decoder string_decode_func;
	
	MetadataType(
		types::Code serialisationCode,
		int serialisationSizeLen,
		const std::string& tag,
		item_decoder decode_func,
		string_decoder string_decode_func);
	~MetadataType();

	// Get information about the given metadata
	static const MetadataType* get(types::Code);
};

/// Decode an item encoded in binary representation
Item<> decode(const unsigned char* buf, size_t len);
/**
 * Decode the item envelope in buf:len
 *
 * Return the inside of the envelope start in buf and the inside length in len
 *
 * After the function returns, the start of the next envelope is at buf+len
 */
types::Code decodeEnvelope(const unsigned char*& buf, size_t& len);
Item<> decodeInner(types::Code, const unsigned char* buf, size_t len);
Item<> decodeString(types::Code, const std::string& val);
std::string tag(types::Code);

/**
 * Read a data bundle from a POSIX file descriptor, returning the signature
 * string, the version number and the data in a buffer.
 *
 * @return
 *   true if a data bundle was read, false on end of file
 */
bool readBundle(int fd, const std::string& filename, wibble::sys::Buffer& buf, std::string& signature, unsigned& version);

/**
 * Read a data bundle from a file, returning the signature string, the version
 * number and the data in a buffer.
 *
 * @return
 *   true if a data bundle was read, false on end of file
 */
bool readBundle(std::istream& in, const std::string& filename, wibble::sys::Buffer& buf, std::string& signature, unsigned& version);

/**
 * Decode the header of a data bundle from a memory buffer, returning the
 * signature string, the version number and a pointer to the data inside the
 * buffer.
 *
 * @return
 *   true if a data bundle was found, false on end of buffer
 */
bool readBundle(const unsigned char*& buf, size_t& len, const std::string& filename, const unsigned char*& obuf, size_t& olen, std::string& signature, unsigned& version);

}

}

// vim:set ts=4 sw=4:
#endif
