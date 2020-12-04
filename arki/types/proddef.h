#ifndef ARKI_TYPES_PRODDEF_H
#define ARKI_TYPES_PRODDEF_H

#include <arki/types/encoded.h>
#include <arki/types/values.h>

namespace arki {
namespace types {

namespace proddef {

/// Style values
enum class Style: unsigned char {
    GRIB = 1,
};

}

struct Proddef;

template<>
struct traits<Proddef>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef proddef::Style Style;
};

/**
 * The vertical proddef or layer of some data
 *
 * It can contain information like proddef type and proddef value.
 */
struct Proddef : public Encoded
{
    using Encoded::Encoded;

    typedef proddef::Style Style;

    types::Code type_code() const override { return traits<Proddef>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Proddef>::type_sersize_bytes; }
    std::string tag() const override { return traits<Proddef>::type_tag; }

    Proddef* clone() const override = 0;

    int compare(const Type& o) const override;

    static proddef::Style style(const uint8_t* data, unsigned size);
    static ValueBag get_GRIB(const uint8_t* data, unsigned size);

    proddef::Style style() const { return style(data, size); }
    ValueBag get_GRIB() const { return get_GRIB(data, size); }

    /// Convert a string into a style
    static Style parseStyle(const std::string& str);
    /// Convert a style into its string representation
    static std::string formatStyle(Style s);

    /// CODEC functions
    static std::unique_ptr<Proddef> decode(core::BinaryDecoder& dec, bool reuse_buffer);
    static std::unique_ptr<Proddef> decodeString(const std::string& val);
    static std::unique_ptr<Proddef> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    // Register this type tree with the type system
    static void init();

    static std::unique_ptr<Proddef> createGRIB(const ValueBag& values);
};

namespace proddef {

inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Proddef::formatStyle(s); }


class GRIB : public Proddef
{
public:
    using Proddef::Proddef;
    virtual ~GRIB();

    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    int compare_local(const GRIB& o) const;

    GRIB* clone() const override { return new GRIB(data, size); }
};

}

}
}

#endif
