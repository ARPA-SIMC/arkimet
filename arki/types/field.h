#ifndef ARKI_TYPES_FIELD_H
#define ARKI_TYPES_FIELD_H

#include <arki/types/encoded.h>

namespace arki::types {

template <> struct traits<Field>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef unsigned char Style;
};

/**
 * A Field annotation
 */
class Field : public Encoded
{
public:
    using Encoded::Encoded;

    types::Code type_code() const override { return traits<Field>::type_code; }
    size_t serialisationSizeLength() const override
    {
        return traits<Field>::type_sersize_bytes;
    }
    std::string tag() const override { return traits<Field>::type_tag; }

    std::string get() const;

    int compare(const Type& o) const override;

    /// CODEC functions
    static std::unique_ptr<Field> decode(core::BinaryDecoder& dec,
                                         bool reuse_buffer);
    static std::unique_ptr<Field> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys,
                         const Formatter* f = 0) const override;

    Field* clone() const override;

    /// Create a task
    static std::unique_ptr<Field> create(const std::string& value);
    static std::unique_ptr<Field>
    decode_structure(const structured::Keys& keys,
                     const structured::Reader& val);

    static void write_documentation(stream::Text& out, unsigned heading_level);

    // Register this type tree with the type system
    static void init();
};

} // namespace arki::types
#endif
