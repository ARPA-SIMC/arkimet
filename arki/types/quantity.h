#ifndef ARKI_TYPES_QUANTITY_H
#define ARKI_TYPES_QUANTITY_H

#include <arki/types/encoded.h>
#include <set>
#include <string>

namespace arki {
namespace types {

template<> struct traits<Quantity>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
};

class Quantity : public Encoded
{
    constexpr static const char* doc = R"(
Representation of ODIM quantity as a list of one or more string values.

.. note::
   TODO: find meanings of ODIMh5 values
)";

public:
    using Encoded::Encoded;

    types::Code type_code() const override { return traits<Quantity>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Quantity>::type_sersize_bytes; }
    std::string tag() const override { return traits<Quantity>::type_tag; }

    // TODO: use a sorted vector instead of a std::set, since we always store few values
    std::set<std::string> get() const;

    int compare(const Type& o) const override;

    /// CODEC functions
    static std::unique_ptr<Quantity> decode(core::BinaryDecoder& dec, bool reuse_buffer);
    static std::unique_ptr<Quantity> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    Quantity* clone() const override;

    /// Create a task
    static std::unique_ptr<Quantity> create(const std::string& values);
    static std::unique_ptr<Quantity> create(const std::set<std::string>& values);
    static std::unique_ptr<Quantity> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    static void write_documentation(stream::Text& out, unsigned heading_level);

    // Register this type tree with the type system
    static void init();
};

}
}
#endif
