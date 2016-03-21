#ifndef ARKI_VALUES_H
#define ARKI_VALUES_H

#include <string>
#include <map>
#include <iosfwd>

struct lua_State;

namespace arki {
struct BinaryEncoder;
struct BinaryDecoder;
struct Emitter;

namespace emitter {
namespace memory {
struct Mapping;
struct Node;
}
}

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
    virtual void encode(BinaryEncoder& enc) const = 0;

    /**
     * Decode from compact binary representation.
     *
     * @retval used The number of bytes decoded.
     */
    static Value* decode(BinaryDecoder& dec);

	/**
	 * Encode into a string representation
	 */
	virtual std::string toString() const = 0;

    /// Send contents to an emitter
    virtual void serialise(Emitter& e) const = 0;

	/**
	 * Parse from a string representation
	 */
	static Value* parse(const std::string& str);

	/**
	 * Parse from a string representation
	 */
	static Value* parse(const std::string& str, size_t& lenParsed);

    /// Parse from structured data
    static Value* parse(const emitter::memory::Node& m);

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

	void update(const ValueBag& vb);

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
    void encode(BinaryEncoder& enc) const;

    /**
     * Decode from compact binary representation
     */
    static ValueBag decode(BinaryDecoder& dec);

	/**
	 * Encode into a string representation
	 */
	std::string toString() const;

	/**
	 * Parse from a string representation
	 */
	static ValueBag parse(const std::string& str);

    /// Send contents to an emitter
    void serialise(Emitter& e) const;

    /// Parse from structured data
    static ValueBag parse(const emitter::memory::Mapping& m);

	/// Push to the LUA stack a table with the data of this ValueBag
	void lua_push(lua_State* L) const;

	/// Fill in the ValueBag from the Lua table on top of the stack
	void load_lua_table(lua_State* L, int idx = -1);

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
