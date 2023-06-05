#ifndef ARKI_VALUES_H
#define ARKI_VALUES_H

#include <arki/core/fwd.h>
#include <arki/structured/fwd.h>
#include <arki/core/binary.h>
#include <string>
#include <vector>
#include <memory>
#include <iosfwd>
#include <cstdint>

struct lua_State;

namespace arki {
namespace types {
class ValueBag;
class ValueBagMatcher;

namespace values {
class Value;
class ValueBagBuilder;

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
    std::vector<Value*> values;

    /**
     * Sets a value.
     *
     * It takes ownership of the Value pointer.
     */
    void set(std::unique_ptr<Value> val);

    size_t size() const;
    void clear();

public:
    Values() = default;
    Values(const Values& vb);
    Values(Values&& vb);
    ~Values();
    Values& operator=(const Values& vb) = delete;
    Values& operator=(Values&& vb);

    bool empty() const { return values.empty(); }

    friend class arki::types::ValueBagMatcher;
    friend class arki::types::ValueBag;
    friend class arki::types::values::ValueBagBuilder;
};

class ValueBagBuilder
{
protected:
    values::Values values;

public:
    void add(std::unique_ptr<Value> value);
    void add(const std::string& key, const std::string& val);
    void add(const std::string& key, int val);

    ValueBag build() const;
};

}


class ValueBag
{
protected:
    const uint8_t* data = nullptr;
    unsigned size = 0;
    bool owned = false;

public:
    ValueBag() = default;
    ValueBag(const uint8_t* data, unsigned size, bool owned);
    ValueBag(const ValueBag& o);
    ValueBag(ValueBag&& o);
    ~ValueBag();
    ValueBag& operator=(ValueBag&& vb);

    struct const_iterator
    {
        typedef values::Value value_type;
        typedef ptrdiff_t difference_type;
        typedef value_type *pointer;
        typedef value_type &reference;
        typedef std::forward_iterator_tag iterator_category;

        core::BinaryDecoder dec;
        const values::Value* value = nullptr;

        const_iterator(const core::BinaryDecoder& dec);
        const_iterator(const const_iterator& o) = delete;
        const_iterator(const_iterator&& o);
        ~const_iterator();
        const_iterator& operator=(const const_iterator& o) = delete;
        const_iterator& operator=(const_iterator&& o);
        const_iterator& operator++();
        bool operator==(const const_iterator& o) const;
        bool operator!=(const const_iterator& o) const;
        const values::Value& operator*() const { return *value; }
        const values::Value* operator->() const { return value; }
    };

    const_iterator begin() const { return const_iterator(core::BinaryDecoder(data, size)); }
    const_iterator end() const { return const_iterator(core::BinaryDecoder(data + size, 0)); }

    bool operator==(const ValueBag& vb) const;
    bool operator!=(const ValueBag& vb) const { return !operator==(vb); }
    bool empty() const { return size == 0; }
    int compare(const ValueBag& vb) const;

    /**
     * Encode into a compact binary representation
     */
    void encode(core::BinaryEncoder& enc) const;

	/**
	 * Encode into a string representation
	 */
	std::string toString() const;

    /// Send contents to an emitter
    void serialise(structured::Emitter& e) const;

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
     * Parse from a string representation
     */
    static ValueBag parse(const std::string& str);

    /// Parse from structured data
    static ValueBag parse(const structured::Reader& reader);

    // Lua functions are still here because they are needed by arki::utils::vm2::find_*
    // and can be removed otherwise

    // Fill in the ValueBag from the Lua table on top of the stack.
    // The values can be string or integer (numbers will be truncated).
    static ValueBag load_lua_table(lua_State* L, int idx = -1);

    friend class ValueBagMatcher;

private:
    // Disable modifying subscription, because it'd be hard to deallocate the
    // old value
    values::Value*& operator[](const std::string& str);
};

class ValueBagMatcher : public values::Values
{
public:
    using values::Values::Values;

    bool is_subset(const ValueBag& vb) const;

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
