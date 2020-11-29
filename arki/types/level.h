#ifndef ARKI_TYPES_LEVEL_H
#define ARKI_TYPES_LEVEL_H

#include <arki/types/encoded.h>
#include <stdint.h>

namespace arki {
namespace types {

namespace level {

/// Style values
enum class Style: unsigned char {
    GRIB1 = 1,
    GRIB2S = 2,
    GRIB2D = 3,
    ODIMH5 = 4,
};

}

template<>
struct traits<Level>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
    typedef level::Style Style;
};

/**
 * The vertical level or layer of some data
 *
 * It can contain information like leveltype and level value.
 */
class Level : public types::Encoded
{
public:
    static const uint8_t GRIB2_MISSING_TYPE;
    static const uint8_t GRIB2_MISSING_SCALE;
    static const uint32_t GRIB2_MISSING_VALUE;
    static unsigned GRIB1_type_vals(unsigned char type);

    using Encoded::Encoded;

    typedef level::Style Style;

    types::Code type_code() const override { return traits<Level>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Level>::type_sersize_bytes; }
    std::string tag() const override { return traits<Level>::type_tag; }

    Level* clone() const override;

    int compare(const Type& o) const override;

    // Get the element style
    level::Style style() const;
    void get_GRIB1(unsigned& type, unsigned& l1, unsigned& l2) const;
    void get_GRIB2S(unsigned& type, unsigned& scale, unsigned& value) const;
    void get_GRIB2D(unsigned& type1, unsigned& scale1, unsigned& value1, unsigned& type2, unsigned& scale2, unsigned& value2) const;
    void get_ODIMH5(double& min, double& max) const;

    /// Convert a string into a style
    static Style parseStyle(const std::string& str);
    /// Convert a style into its string representation
    static std::string formatStyle(Style s);

    /// CODEC functions
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    std::string exactQuery() const override;

    static std::unique_ptr<Level> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<Level> decodeString(const std::string& val);
    static std::unique_ptr<Level> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    // Register this type tree with the type system
    static void init();

    static std::unique_ptr<Level> createGRIB1(unsigned char type);
    static std::unique_ptr<Level> createGRIB1(unsigned char type, unsigned short l1);
    static std::unique_ptr<Level> createGRIB1(unsigned char type, unsigned short l1, unsigned char l2);
    static std::unique_ptr<Level> createGRIB2S(uint8_t type, uint8_t scale, uint32_t val);
    static std::unique_ptr<Level> createGRIB2D(uint8_t type1, uint8_t scale1, uint32_t val1,
                                             uint8_t type2, uint8_t scale2, uint32_t val2);
    static std::unique_ptr<Level> createODIMH5(double value);
    static std::unique_ptr<Level> createODIMH5(double min, double max);
};

namespace level {
inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Level::formatStyle(s); }
}

}
}
#endif
