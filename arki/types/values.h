#ifndef ARKI_VALUES_H
#define ARKI_VALUES_H

#include <arki/core/fwd.h>
#include <arki/structured/fwd.h>
#include <string>
#ifdef __cpp_lib_string_view
#include <string_view>
#endif
#include <vector>
#include <memory>
#include <iosfwd>

struct lua_State;

namespace arki {
namespace types {
class ValueBag;

namespace values {
class Values;

#ifdef __cpp_lib_string_view
typedef std::string_view string_view;
#else
class string_view;
#endif

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
    const uint8_t* data = nullptr;

    /// Size of the data buffer
    virtual unsigned encoded_size() const = 0;

    /**
     * Implementation identifier, used for sorting and downcasting
     */
    virtual unsigned type_id() const = 0;

    /**
     * Check if two values are the same.
     *
     * When this function is called, it can be assumed that
     * type_id() == v.type_id()
     */
    virtual bool value_equals(const Value& v) const = 0;

    /**
     * Compare two items, returning <0 ==0 or >0 depending on results.
     *
     * When this function is called, it can be assumed that
     * type_id() == v.type_id()
     */
    virtual int value_compare(const Value& v) const = 0;

    values::string_view name() const;

public:
    Value(const uint8_t* data);
    Value(const Value&) = delete;
    Value(Value&&) = delete;
    virtual ~Value();
    Value& operator=(const Value&) = delete;
    Value& operator=(Value&&) = delete;

    bool operator==(const Value& v) const;
    bool operator!=(const Value& v) const { return !operator==(v); }
    int compare(const Value& v) const;
    int compare_values(const Value& v) const;

    virtual std::unique_ptr<Value> clone() const = 0;

    /**
     * Encode into a compact binary representation
     */
    void encode(core::BinaryEncoder& enc) const;

    /**
     * Decode from compact binary representation.
     */
    static std::unique_ptr<Value> decode(core::BinaryDecoder& dec);

    /**
     * Decode from compact binary representation, reusing the data from the
     * buffer in dec.
     *
     * The buffer must remain valid during the whole lifetime of the ValueBag
     * object.
     */
    static std::unique_ptr<Value> decode_reusing_buffer(core::BinaryDecoder& dec);

    /**
     * Encode into a string representation
     */
    virtual std::string toString() const = 0;

    /// Send contents to an emitter
    virtual void serialise(structured::Emitter& e) const = 0;

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
};

/**
 * Sorted container for values.
 *
 * Values are sorted by name. Values with the same name are replaced, meaning
 * that names are kept unique.
 */
class Values
{
protected:
    std::vector<values::Value*> values;

    /**
     * Sets a value.
     *
     * It takes ownership of the Value pointer.
     */
    void set(std::unique_ptr<values::Value> val);

public:
    Values();
    Values(const Values& vb);
    Values(Values&& vb);
    ~Values();
    Values& operator=(const Values& vb);
    Values& operator=(Values&& vb);

    bool empty() const { return values.empty(); }
    size_t size() const { return values.size(); }
    void clear();
};

}

struct ValueBag : public values::Values
{
protected:
    using values::Values::set;

public:
    using values::Values::Values;

	bool operator==(const ValueBag& vb) const;
	bool operator!=(const ValueBag& vb) const { return !operator==(vb); }
	int compare(const ValueBag& vb) const;

	bool contains(const ValueBag& vb) const;

	void update(const ValueBag& vb);

    /**
     * Gets a value.
     *
     * It returns nullptr if the value is not found.
     */
    const values::Value* get(const std::string& key) const;

    /// Set an integer value
    void set(const std::string& key, int val) { values::Values::set(values::Value::create_integer(key, val)); }

    /// Set a string value
    void set(const std::string& key, const std::string& val) { values::Values::set(values::Value::create_string(key, val)); }

    /**
     * Encode into a compact binary representation
     */
    void encode(core::BinaryEncoder& enc) const;

    /**
     * Decode from compact binary representation
     */
    static ValueBag decode(core::BinaryDecoder& dec);

    /**
     * Decode from compact binary representation, reusing the data from the
     * buffer in dec.
     *
     * The buffer must remain valid during the whole lifetime of the ValueBag
     * object.
     */
    static ValueBag decode_reusing_buffer(core::BinaryDecoder& dec);

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

struct ValueBagMatcher
{
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
