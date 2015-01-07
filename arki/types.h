#ifndef ARKI_TYPES_H
#define ARKI_TYPES_H

/*
 * types - arkimet metadata type system
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include <wibble/exception.h>
#include <string>
#include <iosfwd>
#include <memory>

#ifndef HAVE_CXX11
#define override
#endif

struct lua_State;

namespace wibble {
namespace sys {
struct Buffer;
}
}

namespace arki {
struct Emitter;
struct Formatter;

/// dynamic cast between two auto_ptr
template<typename B, typename A>
std::auto_ptr<B> downcast(std::auto_ptr<A> orig)
{
    if (!orig.get()) return std::auto_ptr<B>();

    // Cast, but leave ownership to orig
    B* dst = dynamic_cast<B*>(orig.get());

    // If we fail here, orig will clean it
    if (!dst) throw wibble::exception::BadCastExt<A, B>("cannot cast smart pointer");

    // Release ownership from orig: we still have the pointer in dst
    orig.release();

    // Transfer ownership to the new auto_ptr and return it
    return std::auto_ptr<B>(dst);
}

/// upcast between two auto_ptr
template<typename B, typename A>
std::auto_ptr<B> upcast(std::auto_ptr<A> orig)
{
    return std::auto_ptr<B>(orig.release());
}

namespace emitter {
namespace memory {
struct Mapping;
}
}

namespace utils {
namespace codec {
struct Encoder;
struct Decoder;
}
}

namespace types {
struct Type;

/// Identifier codes used for binary serialisation
enum Code
{
    TYPE_INVALID         =  0,
    TYPE_ORIGIN          =  1,
    TYPE_PRODUCT         =  2,
    TYPE_LEVEL           =  3,
    TYPE_TIMERANGE       =  4,
    TYPE_REFTIME         =  5,
    TYPE_NOTE            =  6,
    TYPE_SOURCE          =  7,
    TYPE_ASSIGNEDDATASET =  8,
    TYPE_AREA            =  9,
    TYPE_PRODDEF         = 10,
    TYPE_SUMMARYITEM     = 11,
    TYPE_SUMMARYSTATS    = 12,
    TYPE_TIME            = 13,
    TYPE_BBOX            = 14,
    TYPE_RUN             = 15,
    TYPE_TASK            = 16, // utilizzato per OdimH5 /how.task
    TYPE_QUANTITY        = 17, // utilizzato per OdimH5 /what.quantity
    TYPE_VALUE           = 18,
    TYPE_MAXCODE
};

// Parse name into a type code, returning TYPE_INVALID if it does not match
Code checkCodeName(const std::string& name);
// Parse name into a type code, throwing an exception if it does not match
Code parseCodeName(const std::string& name);
std::string formatCode(const Code& c);
static inline std::ostream& operator<<(std::ostream& o, const Code& c) { return o << formatCode(c); }

}

namespace types {

template<typename T>
class traits;

/**
 * Base class for implementing arkimet metadata types
 */
struct Type
{
	virtual ~Type() {}

    /**
     * Make a copy of this type. The caller will own the newly created object
     * returned by the function.
     *
     * This does not return an auto_ptr to allow to use covariant return types
     * in implementations. Use cloneType() for a version with explicit memory
     * ownership of the result.
     */
    virtual Type* clone() const = 0;

    /// Make a copy of this type
    std::auto_ptr<Type> cloneType() const { return std::auto_ptr<Type>(clone()); }

    /// Comparison (<0 if <, 0 if =, >0 if >)
    virtual int compare(const Type& o) const;
    bool operator<(const Type& o) const { return compare(o) < 0; }
    bool operator<=(const Type& o) const { return compare(o) <= 0; }
    bool operator>(const Type& o) const { return compare(o) > 0; }
    bool operator>=(const Type& o) const { return compare(o) >= 0; }

    /// Equality
    virtual bool equals(const Type& t) const = 0;
    bool operator==(const Type& o) const { return equals(o); }
    bool operator!=(const Type& o) const { return !equals(o); }

    /// Compare with a stringified version, useful for testing
    bool operator==(const std::string& o) const;

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
     * Encode to compact binary representation, with identification envelope
     */
    virtual std::string encodeBinary() const;

	/// Write as a string to an output stream
	virtual std::ostream& writeToOstream(std::ostream& o) const = 0;

    /// Serialise using an emitter
    virtual void serialise(Emitter& e, const Formatter* f=0) const;
    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const = 0;

	/**
	 * Return a matcher query (without the metadata type prefix) that
	 * exactly matches this metadata item
	 */
	virtual std::string exactQuery() const;

    /// Push to the LUA stack a userdata with a copy of this item
    void lua_push(lua_State* L) const;

	/**
	 * Lookup members by name and push them in the Lua stack
	 *
	 * @return true if name matched a member and an element was pushed on
	 *         stack, else false
	 */
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	/**
	 * Return the lua type name (i.e. arki.type.<something>)
	 * used to register the metatable for this type
	 */
	virtual const char* lua_type_name() const = 0;

	/**
	 * Given a metatable on top of the stack, register methods for this
	 * object on it
	 */
	virtual void lua_register_methods(lua_State* L) const;

    /**
     * Check that the element at \a idx is a Type userdata
     *
     * @return the Type element, or undefined if the check failed
     */
    static Type* lua_check(lua_State* L, int idx, const char* prefix = "arki.types");

    template<typename T>
    static T* lua_check(lua_State* L, int idx)
    {
        return dynamic_cast<T*>(lua_check(L, idx, traits<T>::type_lua_tag));
    }

    static void lua_loadlib(lua_State* L);

    /**
     * Return true of either both pointers are null, or if they are both
     * non-null and the two types test for equality.
     */
    static inline bool nullable_equals(const Type* a, const Type* b)
    {
        if (!a && !b) return true;
        if (!a || !b) return false;
        return a->equals(*b);
    }
    /**
     * Return the comparison value of a and b, assuming that a null pointer
     * tests smaller than any type.
     */
    static inline int nullable_compare(const Type* a, const Type* b)
    {
        if (!a && !b) return 0;
        if (!a) return -1;
        if (!b) return 1;
        return a->compare(*b);
    }
};

/// Write as a string to an output stream
inline std::ostream& operator<<(std::ostream& o, const Type& t)
{
    return t.writeToOstream(o);
}



template<typename BASE>
struct CoreType : public Type
{
    types::Code serialisationCode() const override { return traits<BASE>::type_code; }
    size_t serialisationSizeLength() const override { return traits<BASE>::type_sersize_bytes; }
    std::string tag() const override { return traits<BASE>::type_tag; }
    const char* lua_type_name() const override { return traits<BASE>::type_lua_tag; }
    static void lua_loadlib(lua_State* L);
};

template<typename BASE>
struct StyledType : public CoreType<BASE>
{
	typedef typename traits<BASE>::Style Style;

	// Get the element style
	virtual Style style() const = 0;

    // Default implementations of Type methods
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    int compare(const Type& o) const override;
    virtual int compare_local(const BASE& o) const = 0;

    virtual void serialiseLocal(Emitter& e, const Formatter* f=0) const;

	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

    static Style style_from_mapping(const emitter::memory::Mapping& m);
};


/// Decode an item encoded in binary representation
std::auto_ptr<Type> decode(const unsigned char* buf, size_t len);

/**
 * Decode an item encoded in binary representation with envelope, from a
 * decoder
 */
std::auto_ptr<Type> decode(utils::codec::Decoder& dec);
/**
 * Decode the item envelope in buf:len
 *
 * Return the inside of the envelope start in buf and the inside length in len
 *
 * After the function returns, the start of the next envelope is at buf+len
 */
types::Code decodeEnvelope(const unsigned char*& buf, size_t& len);
std::auto_ptr<Type> decodeInner(types::Code, const unsigned char* buf, size_t len);
std::auto_ptr<Type> decodeString(types::Code, const std::string& val);
std::auto_ptr<Type> decodeMapping(const emitter::memory::Mapping& m);
/// Same as decodeMapping, but does not look for the item type in the mapping
std::auto_ptr<Type> decodeMapping(types::Code, const emitter::memory::Mapping& m);
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

#endif
