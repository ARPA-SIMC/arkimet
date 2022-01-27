#ifndef ARKI_TYPES_VALUE_H
#define ARKI_TYPES_VALUE_H

#include <arki/types.h>
#include <string>

namespace arki {
namespace types {

struct Value;

template<>
struct traits<Value>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
};

struct Value : public Type
{
    constexpr static const char* doc = R"(
The value of very short data encoded as part of the metadata

This is currently used to encode the non-metadata part of VM2 data so that
it can be extracted from metadata or dataset indices and completed using the
rest of metadata values, avoiding disk lookips
)";
    std::string buffer;

    types::Code type_code() const override { return traits<Value>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Value>::type_sersize_bytes; }
    std::string tag() const override { return traits<Value>::type_tag; }

    bool equals(const Type& o) const override;
    int compare(const Type& o) const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    /// CODEC functions
    static std::unique_ptr<Value> decode(core::BinaryDecoder& dec, bool reuse_buffer);
    static std::unique_ptr<Value> decodeString(const std::string& val);
    static std::unique_ptr<Value> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    Value* clone() const override;
    static std::unique_ptr<Value> create(const std::string& buf);

    static void write_documentation(stream::Text& out, unsigned heading_level);

    // Register this type tree with the type system
    static void init();
};

}
}
#endif
