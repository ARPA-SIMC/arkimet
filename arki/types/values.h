#ifndef ARKI_VALUES_H
#define ARKI_VALUES_H

#include <arki/core/fwd.h>
#include <arki/structured/fwd.h>
#include <string>
#include <vector>
#include <memory>
#include <iosfwd>

struct lua_State;

namespace arki {
namespace types {
class ValueBag;
class ValueBagMatcher;

namespace values {
class Values;
class Value;

void encode_int(core::BinaryEncoder& enc, int value);
int decode_sint6(uint8_t lead);
int decode_number(core::BinaryDecoder& dec, uint8_t lead);
int decode_int(core::BinaryDecoder& dec, uint8_t lead);

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

    size_t size() const { return values.size(); }
    void clear();

public:
    Values();
    Values(const Values& vb) = delete;
    Values(Values&& vb);
    ~Values();
    Values& operator=(const Values& vb) = delete;
    Values& operator=(Values&& vb);

    bool empty() const { return values.empty(); }

    friend class arki::types::ValueBagMatcher;
};

}

struct ValueBag : public values::Values
{
protected:
    using values::Values::set;

    /// Set an integer value
    void set(const std::string& key, int val);

public:
    using values::Values::Values;

    typedef std::vector<values::Value*>::const_iterator const_iterator;
    const_iterator begin() const { return values.begin(); }
    const_iterator end() const { return values.end(); }

	bool operator==(const ValueBag& vb) const;
	bool operator!=(const ValueBag& vb) const { return !operator==(vb); }
	int compare(const ValueBag& vb) const;

    /// Set a string value (currently needed by bufr scanner)
    void set(const std::string& key, const std::string& val);

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

    // Fill in the ValueBag from the Lua table on top of the stack.
    // The values can be string or integer (numbers will be truncated).
	void load_lua_table(lua_State* L, int idx = -1);

    friend class ValueBagMatcher;

private:
    // Disable modifying subscription, because it'd be hard to deallocate the
    // old value
    values::Value*& operator[](const std::string& str);
};

struct ValueBagMatcher : public values::Values
{
    using values::Values::Values;

    bool is_subset(const values::Values& vb) const;

    /**
     * Encode into a string representation
     */
    std::string to_string() const;

    /// Push to the LUA stack a table with the data of this ValueBag
    void lua_push(lua_State* L) const;

    /**
     * Parse from a string representation
     */
    static ValueBagMatcher parse(const std::string& str);
};

static inline std::ostream& operator<<(std::ostream& o, const ValueBag& v)
{
    return o << v.toString();
}

}
}

#endif
