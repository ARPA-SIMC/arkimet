#ifndef ARKI_VALUES_H
#define ARKI_VALUES_H

#include <arki/core/fwd.h>
#include <arki/structured/fwd.h>
#include <string>
#include <map>
#include <iosfwd>

struct lua_State;

namespace arki {
namespace types {

namespace values {
/**
 * Base class for generic scalar values.
 *
 * This is needed in order to store arbitrary types for the key=value kind of
 * types (like Area and Proddef).
 *
 * In practice, we have to create a dynamic type system.
 */
class Value
{
protected:
    std::string m_name;

public:
    Value(const std::string& name) : m_name(name) {}
    virtual ~Value() {}

    const std::string& name() const { return m_name; }

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
    virtual void encode(core::BinaryEncoder& enc) const = 0;

    /**
     * Decode from compact binary representation.
     *
     * @retval used The number of bytes decoded.
     */
    static Value* decode(const std::string& name, core::BinaryDecoder& dec);

	/**
	 * Encode into a string representation
	 */
	virtual std::string toString() const = 0;

    /// Send contents to an emitter
    virtual void serialise(structured::Emitter& e) const = 0;

    /**
     * Parse from a string representation
     */
    static Value* parse(const std::string& name, const std::string& str);

    /**
     * Parse from a string representation
     */
    static Value* parse(const std::string& name, const std::string& str, size_t& lenParsed);

    static Value* create_integer(const std::string& name, int val);
    static Value* create_string(const std::string& name, const std::string& val);
};
}

struct ValueBag : public std::map<std::string, values::Value*>
{
public:
	ValueBag();
	ValueBag(const ValueBag& vb);
	~ValueBag();

	ValueBag& operator=(const ValueBag& vb);
	bool operator==(const ValueBag& vb) const;
	bool operator!=(const ValueBag& vb) const { return !operator==(vb); }
	int compare(const ValueBag& vb) const;

	bool contains(const ValueBag& vb) const;

	void update(const ValueBag& vb);

	void clear();

    /**
     * Gets a value.
     *
     * It returns nullptr if the value is not found.
     */
    const values::Value* get(const std::string& key) const;

    /**
     * Sets a value.
     *
     * It takes ownership of the Value pointer.
     */
    void set(const std::string& key, values::Value* val);

    /// Set an integer value
    void set(const std::string& key, int val) { set(key, values::Value::create_integer(key, val)); }

    /// Set a string value
    void set(const std::string& key, const std::string& val) { set(key, values::Value::create_string(key, val)); }

    /**
     * Encode into a compact binary representation
     */
    void encode(core::BinaryEncoder& enc) const;

    /**
     * Decode from compact binary representation
     */
    static ValueBag decode(core::BinaryDecoder& dec);

	/**
	 * Encode into a string representation
	 */
	std::string toString() const;

	/**
	 * Parse from a string representation
	 */
	static ValueBag parse(const std::string& str);

    /// Send contents to an emitter
    void serialise(structured::Emitter& e) const;

    /// Parse from structured data
    static ValueBag parse(const structured::Reader& reader);

    // Lua functions are still here because they are needed by arki::utils::vm2::find_*
    // and can be removed otherwise

	/// Push to the LUA stack a table with the data of this ValueBag
	void lua_push(lua_State* L) const;

    // Fill in the ValueBag from the Lua table on top of the stack.
    // The values can be string or integer (numbers will be truncated).
	void load_lua_table(lua_State* L, int idx = -1);

private:
    // Disable modifying subscription, because it'd be hard to deallocate the
    // old value
    values::Value*& operator[](const std::string& str);
};

static inline std::ostream& operator<<(std::ostream& o, const values::Value& v)
{
    return o << v.toString();
}
static inline std::ostream& operator<<(std::ostream& o, const ValueBag& v)
{
    return o << v.toString();
}

}
}

#endif
