#ifndef ARKI_TYPES_QUANTITY_H
#define ARKI_TYPES_QUANTITY_H

#include <arki/types/core.h>
#include <set>
#include <string>

struct lua_State;

namespace arki {
namespace types {

template<> struct traits<Quantity>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
    static const char* type_lua_tag;
};

/**
 * Quantity informations
 */
struct Quantity : public CoreType<Quantity>
{
	std::set<std::string> values;

	Quantity(const std::set<std::string>& values) : values(values) {}

    int compare(const Type& o) const override;
    bool equals(const Type& o) const override;

    /// CODEC functions
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    static std::unique_ptr<Quantity> decode(BinaryDecoder& dec);
    static std::unique_ptr<Quantity> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    Quantity* clone() const override;

    /// Create a task
    static std::unique_ptr<Quantity> create(const std::string& values);
    static std::unique_ptr<Quantity> create(const std::set<std::string>& values);
    static std::unique_ptr<Quantity> decode_structure(const structured::Keys& keys, const structured::Reader& val);

	static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();
};

}
}
#endif
