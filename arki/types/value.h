#ifndef ARKI_TYPES_VALUE_H
#define ARKI_TYPES_VALUE_H

#include <arki/types.h>
#include <string>

struct lua_State;

namespace arki {
namespace types {

struct Value;

template<>
struct traits<Value>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The run of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
struct Value : public types::CoreType<Value>
{
    std::string buffer;

    bool equals(const Type& o) const override;
    int compare(const Type& o) const override;
    void encodeWithoutEnvelope(BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;

    /// CODEC functions
    static std::unique_ptr<Value> decode(BinaryDecoder& dec);
    static std::unique_ptr<Value> decodeString(const std::string& val);
    static std::unique_ptr<Value> decodeMapping(const emitter::memory::Mapping& val);

    Value* clone() const override;
    static std::unique_ptr<Value> create(const std::string& buf);

    static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();
};

}
}
#endif
