#ifndef ARKI_TYPES_ORIGIN_H
#define ARKI_TYPES_ORIGIN_H

#include <arki/types/encoded.h>

namespace arki {
namespace types {

namespace origin {

/// Style values
enum class Style: unsigned char {
    GRIB1 = 1,
    GRIB2 = 2,
    BUFR = 3,
    ODIMH5 = 4,
};

}

template<>
struct traits<Origin>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef origin::Style Style;
};

/**
 * The origin of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
class Origin : public types::Encoded
{
public:
    using Encoded::Encoded;

    typedef origin::Style Style;

    types::Code type_code() const override { return traits<Origin>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Origin>::type_sersize_bytes; }
    std::string tag() const override { return traits<Origin>::type_tag; }

    Origin* clone() const override { return new Origin(data, size); }

    int compare(const Type& o) const override;

    // Get the element style
    origin::Style style() const;
    void get_GRIB1(unsigned& centre, unsigned& subcentre, unsigned& process) const;
    void get_GRIB2(unsigned& centre, unsigned& subcentre, unsigned& processtype, unsigned& bgprocessid, unsigned& processid) const;
    void get_BUFR(unsigned& centre, unsigned& subcentre) const;
    void get_ODIMH5(std::string& WMO, std::string& RAD, std::string& PLC) const;

    /// Convert a string into a style
    static origin::Style parseStyle(const std::string& str);
    /// Convert a style into its string representation
    static std::string formatStyle(origin::Style s);

    /// CODEC functions
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    std::string exactQuery() const override;

    static std::unique_ptr<Origin> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<Origin> decodeString(const std::string& val);
    static std::unique_ptr<Origin> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    // Register this type tree with the type system
    static void init();

    static std::unique_ptr<Origin> createGRIB1(unsigned char centre, unsigned char subcentre, unsigned char process);
    static std::unique_ptr<Origin> createGRIB2(unsigned short centre, unsigned short subcentre,
                                               unsigned char processtype, unsigned char bgprocessid, unsigned char processid);
    static std::unique_ptr<Origin> createBUFR(unsigned char centre, unsigned char subcentre);
    static std::unique_ptr<Origin> createODIMH5(const std::string& wmo, const std::string& rad, const std::string& plc);
};

namespace origin {
inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Origin::formatStyle(s); }
}

}
}
#endif
