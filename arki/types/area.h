#ifndef ARKI_TYPES_AREA_H
#define ARKI_TYPES_AREA_H

#include <memory>
#include <arki/types/encoded.h>
#include <arki/utils/geos.h>

namespace arki {
namespace types {

namespace area {

/// Style values
enum class Style: unsigned char {
    GRIB = 1,
    ODIMH5 = 2,
    VM2 = 3,
};

}

template<>
struct traits<Area>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
    typedef area::Style Style;
};

class Area : public types::Encoded
{
protected:
    mutable arki::utils::geos::Geometry cached_bbox;

    constexpr static const char* doc = R"(
Geographical area relative to a data element.

When possible, Area values should be convertible to coordinate polygons,
allowing matching using geospatial primitives (contains, intersects, ...).
)";

public:
    using Encoded::Encoded;

    ~Area();

    typedef area::Style Style;

    types::Code type_code() const override { return traits<Area>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Area>::type_sersize_bytes; }
    std::string tag() const override { return traits<Area>::type_tag; }

    Area* clone() const override = 0;

    int compare(const Type& o) const override;

    // Get the element style
    static area::Style style(const uint8_t* data, unsigned size);
    static ValueBag get_GRIB(const uint8_t* data, unsigned size);
    static ValueBag get_ODIMH5(const uint8_t* data, unsigned size);
    static unsigned get_VM2(const uint8_t* data, unsigned size);

    area::Style style() const;
    ValueBag get_GRIB() const;
    ValueBag get_ODIMH5() const;
    unsigned get_VM2() const;

    /// Convert a string into a style
    static Style parseStyle(const std::string& str);
    /// Convert a style into its string representation
    static std::string formatStyle(Style s);

    /// CODEC functions
    static std::unique_ptr<Area> decode(core::BinaryDecoder& dec, bool reuse_buffer);
    static std::unique_ptr<Area> decodeString(const std::string& val);
    static std::unique_ptr<Area> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    /// Return the geographical bounding box
    const arki::utils::geos::Geometry& bbox() const;

    static void write_documentation(stream::Text& out, unsigned heading_level);

    // Register this type tree with the type system
    static void init();
};

namespace area {

inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Area::formatStyle(s); }


class GRIB : public Area
{
public:
    constexpr static const char* name = "GRIB";
    constexpr static const char* doc = R"(
Collection of key-value pairs, interpreted in the context of GRIB grid
definitions.
)";

    using Area::Area;
    ~GRIB();

    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    int compare_local(const GRIB& o) const;
    GRIB* clone() const override { return new GRIB(data, size); }

    static std::unique_ptr<Area> create(const ValueBag& values);
};

class ODIMH5 : public Area
{
public:
    constexpr static const char* name = "ODIMH5";
    constexpr static const char* doc = R"(
Collection of key-value pairs, interpreted in the context of ODIM area
information.
)";

    using Area::Area;
    ~ODIMH5();

    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    int compare_local(const ODIMH5& o) const;
    ODIMH5* clone() const override { return new ODIMH5(data, size); }

    static std::unique_ptr<Area> create(const ValueBag& values);
};

class VM2 : public Area
{
public:
    constexpr static const char* name = "VM2";
    constexpr static const char* doc = R"(
Area information as an integer VM2 station identifier.
)";
    using Area::Area;
    ~VM2();

    bool equals(const Type& o) const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    void encode_for_indexing(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    int compare_local(const VM2& o) const;
    VM2* clone() const override { return new VM2(data, size); }

    ValueBag derived_values() const;

    static std::unique_ptr<Area> create(unsigned station_id);
};


}
}
}
#endif
