#ifndef ARKI_TYPES_BBOX_H
#define ARKI_TYPES_BBOX_H

/**
 * WARNING
 * This metadata type is discontinued, and it exists only to preserve
 * compatibility with existing saved data
 */


#include <arki/types/encoded.h>

namespace arki {
namespace types {

namespace bbox {

/// Style values
enum class Style: unsigned char {
    INVALID = 1,
    POINT = 2,
    BOX = 3,
    HULL = 4,
};

}

class BBox;

template<>
struct traits<BBox>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef bbox::Style Style;
};

/**
 * The bbox of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
class BBox : public types::Encoded
{
public:
    using Encoded::Encoded;

    typedef bbox::Style Style;

    types::Code type_code() const override { return traits<BBox>::type_code; }
    size_t serialisationSizeLength() const override { return traits<BBox>::type_sersize_bytes; }
    std::string tag() const override { return traits<BBox>::type_tag; }

    BBox* clone() const override { return new BBox(data, size); }

    bool equals(const Type& o) const override;
    int compare(const Type& o) const override;

    // Get the element style
    bbox::Style style() const;

    /// Convert a string into a style
    static Style parseStyle(const std::string& str);
    /// Convert a style into its string representation
    static std::string formatStyle(Style s);

    /// CODEC functions
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    static std::unique_ptr<BBox> decode(core::BinaryDecoder& dec, bool reuse_buffer);
    static std::unique_ptr<BBox> decodeString(const std::string& val);
    static std::unique_ptr<BBox> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    // Register this type tree with the type system
    static void init();

    static std::unique_ptr<BBox> createInvalid();
};

}
}
#endif
