#ifndef ARKI_TYPES_H
#define ARKI_TYPES_H

/// arkimet metadata type system
#include <arki/defs.h>
#include <arki/core/fwd.h>
#include <arki/types/fwd.h>
#include <arki/emitter/fwd.h>
#include <string>
#include <vector>
#include <memory>

struct lua_State;

namespace arki {
struct BinaryEncoder;
struct BinaryDecoder;

/// dynamic cast between two unique_ptr
template<typename B, typename A>
std::unique_ptr<B> downcast(std::unique_ptr<A> orig)
{
    if (!orig.get()) return std::unique_ptr<B>();

    // Cast, but leave ownership to orig
    B* dst = dynamic_cast<B*>(orig.get());

    // If we fail here, orig will clean it
    if (!dst)
    {
        std::string msg("cannot cast smart pointer from ");
        msg += typeid(A).name();
        msg += " to ";
        msg += typeid(B).name();
        throw std::runtime_error(std::move(msg));
    }

    // Release ownership from orig: we still have the pointer in dst
    orig.release();

    // Transfer ownership to the new unique_ptr and return it
    return std::unique_ptr<B>(dst);
}

/// upcast between two unique_ptr
template<typename B, typename A>
std::unique_ptr<B> upcast(std::unique_ptr<A> orig)
{
    return std::unique_ptr<B>(orig.release());
}

namespace types {

// Parse name into a type code, returning TYPE_INVALID if it does not match
Code checkCodeName(const std::string& name);
// Parse name into a type code, throwing an exception if it does not match
Code parseCodeName(const std::string& name);
std::string formatCode(const Code& c);
static inline std::ostream& operator<<(std::ostream& o, const Code& c) { return o << formatCode(c); }

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
     * This does not return an unique_ptr to allow to use covariant return types
     * in implementations. Use cloneType() for a version with explicit memory
     * ownership of the result.
     */
    virtual Type* clone() const = 0;

    /// Make a copy of this type
    std::unique_ptr<Type> cloneType() const { return std::unique_ptr<Type>(clone()); }

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
	virtual types::Code type_code() const = 0;

	/// Length in bytes of the size field when serialising
	virtual size_t serialisationSizeLength() const = 0;

	/**
	 * Encoding to compact binary representation, without identification
	 * envelope
	 */
	virtual void encodeWithoutEnvelope(BinaryEncoder& enc) const = 0;

    /**
     * Version of encode_without_envelope without redundant data, used for
     * indexing
     */
    virtual void encode_for_indexing(BinaryEncoder& enc) const;

    /// Encode to compact binary representation, with identification envelope
    void encodeBinary(BinaryEncoder& enc) const;

    /// Encode to compact binary representation, with identification envelope
    std::vector<uint8_t> encodeBinary() const;

    /// Write as a string to an output stream
    virtual std::ostream& writeToOstream(std::ostream& o) const = 0;

    std::string to_string() const;

    /// Serialise using an emitter
    virtual void serialise(Emitter& e, const emitter::Keys& keys, const Formatter* f=0) const;
    virtual void serialise_local(Emitter& e, const emitter::Keys& keys, const Formatter* f=0) const = 0;

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
    template<typename A, typename B>
    static inline bool nullable_equals(const std::unique_ptr<A>& a, const std::unique_ptr<B>& b)
    {
        return nullable_equals(a.get(), b.get());
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
    template<typename A, typename B>
    static inline bool nullable_compare(const std::unique_ptr<A>& a, const std::unique_ptr<B>& b)
    {
        return nullable_compare(a.get(), b.get());
    }
};

/// Write as a string to an output stream
inline std::ostream& operator<<(std::ostream& o, const Type& t)
{
    return t.writeToOstream(o);
}


/**
 * Decode an item encoded in binary representation with envelope, from a
 * decoder
 */
std::unique_ptr<Type> decode(BinaryDecoder& dec);

std::unique_ptr<Type> decodeInner(types::Code, BinaryDecoder& dec);
std::unique_ptr<Type> decodeString(types::Code, const std::string& val);
std::unique_ptr<Type> decodeMapping(const emitter::memory::Mapping& m);
/// Same as decodeMapping, but does not look for the item type in the mapping
std::unique_ptr<Type> decodeMapping(types::Code, const emitter::memory::Mapping& m);
std::unique_ptr<Type> decode_structure(const emitter::Keys& keys, const emitter::Reader& reader);
std::unique_ptr<Type> decode_structure(const emitter::Keys& keys, types::Code code, const emitter::Reader& reader);
std::string tag(types::Code);

}

}

#endif
