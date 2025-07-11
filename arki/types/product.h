#ifndef ARKI_TYPES_PRODUCT_H
#define ARKI_TYPES_PRODUCT_H

#include <arki/types/encoded.h>
#include <arki/types/values.h>
#include <cstdint>
#include <memory>

namespace arki::types {

namespace product {

/// Style values
enum class Style : unsigned char {
    GRIB1  = 1,
    GRIB2  = 2,
    BUFR   = 3,
    ODIMH5 = 4,
    VM2    = 5,
};

} // namespace product

template <> struct traits<Product>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;
    typedef product::Style Style;
};

/**
 * Information on what product (variable measured, variable forecast, ...) is
 * contained in the data.
 */
class Product : public types::Encoded
{
    constexpr static const char* doc = R"(
Defines what kind of variable is measured or forecast or otherwise represented
in the data.
)";

public:
    using Encoded::Encoded;

    typedef product::Style Style;

    types::Code type_code() const override
    {
        return traits<Product>::type_code;
    }
    size_t serialisationSizeLength() const override
    {
        return traits<Product>::type_sersize_bytes;
    }
    std::string tag() const override { return traits<Product>::type_tag; }

    static Style style(const uint8_t* data, unsigned size);
    static void get_GRIB1(const uint8_t* data, unsigned size, unsigned& origin,
                          unsigned& table, unsigned& product);
    static void get_GRIB2(const uint8_t* data, unsigned size, unsigned& centre,
                          unsigned& discipline, unsigned& category,
                          unsigned& number, unsigned& table_version,
                          unsigned& local_table_version);
    static void get_BUFR(const uint8_t* data, unsigned size, unsigned& type,
                         unsigned& subtype, unsigned& localsubtype,
                         ValueBag& values);
    static void get_ODIMH5(const uint8_t* data, unsigned size, std::string& obj,
                           std::string& prod);
    static void get_VM2(const uint8_t* data, unsigned size,
                        unsigned& variable_id);

    Style style() const { return style(data, size); }
    void get_GRIB1(unsigned& origin, unsigned& table, unsigned& product) const
    {
        get_GRIB1(data, size, origin, table, product);
    }
    void get_GRIB2(unsigned& centre, unsigned& discipline, unsigned& category,
                   unsigned& number, unsigned& table_version,
                   unsigned& local_table_version) const
    {
        get_GRIB2(data, size, centre, discipline, category, number,
                  table_version, local_table_version);
    }
    void get_BUFR(unsigned& type, unsigned& subtype, unsigned& localsubtype,
                  ValueBag& values) const
    {
        get_BUFR(data, size, type, subtype, localsubtype, values);
    }
    void get_ODIMH5(std::string& obj, std::string& prod) const
    {
        get_ODIMH5(data, size, obj, prod);
    }
    void get_VM2(unsigned& variable_id) const
    {
        get_VM2(data, size, variable_id);
    }

    int compare(const Type& o) const override;

    /// Convert a string into a style
    static Style parseStyle(const std::string& str);
    /// Convert a style into its string representation
    static std::string formatStyle(Style s);

    /// CODEC functions

    static std::unique_ptr<Product> decode(core::BinaryDecoder& dec,
                                           bool reuse_buffer);
    static std::unique_ptr<Product> decodeString(const std::string& val);
    static std::unique_ptr<Product>
    decode_structure(const structured::Keys& keys,
                     const structured::Reader& val);

    static void init();

    static std::unique_ptr<Product> createGRIB1(unsigned char origin,
                                                unsigned char table,
                                                unsigned char product);
    static std::unique_ptr<Product>
    createGRIB2(unsigned short centre, unsigned char discipline,
                unsigned char category, unsigned char number,
                unsigned char table_version       = 4,
                unsigned char local_table_version = 255);
    static std::unique_ptr<Product> createBUFR(unsigned char type,
                                               unsigned char subtype,
                                               unsigned char localsubtype);
    static std::unique_ptr<Product> createBUFR(unsigned char type,
                                               unsigned char subtype,
                                               unsigned char localsubtype,
                                               const ValueBag& name);
    static std::unique_ptr<Product> createODIMH5(const std::string& obj,
                                                 const std::string& prod);
    static std::unique_ptr<Product> createVM2(unsigned variable_id);

    static void write_documentation(stream::Text& out, unsigned heading_level);
};

namespace product {

inline std::ostream& operator<<(std::ostream& o, Style s)
{
    return o << Product::formatStyle(s);
}

class GRIB1 : public Product
{
public:
    constexpr static const char* name = "GRIB1";
    constexpr static const char* doc  = R"(
Product defined the same as product definition in GRIB version 1:

* Origin
* Table number
* Product

.. note::
   TODO: add references to manual of codes
)";
    using Product::Product;

    GRIB1* clone() const override { return new GRIB1(data, size); }

    int compare_local(const GRIB1& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys,
                         const Formatter* f = 0) const override;
    std::string exactQuery() const override;
};

class GRIB2 : public Product
{
public:
    constexpr static const char* name = "GRIB2";
    constexpr static const char* doc  = R"(
Product defined the same as in GRIB version 2:

* Centre
* Discipline
* Category
* Number
* Table version
* Local table version

.. note::
   TODO: add references to manual of codes
)";
    using Product::Product;

    GRIB2* clone() const override { return new GRIB2(data, size); }

    int compare_local(const GRIB2& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys,
                         const Formatter* f = 0) const override;
    std::string exactQuery() const override;
};

class BUFR : public Product
{
public:
    constexpr static const char* name = "BUFR";
    constexpr static const char* doc  = R"(
Product defined the same as in BUFR:

* Type
* Subtype
* Local subtype

.. note::
   TODO: add references to manual of codes

Optionally one can add a number of arbitrary key-value pairs to represent
information extracted from the BUFR contents that is significant for archival
purposes.
)";
    using Product::Product;

    BUFR* clone() const override { return new BUFR(data, size); }

    int compare_local(const BUFR& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys,
                         const Formatter* f = 0) const override;
    std::string exactQuery() const override;
};

class ODIMH5 : public Product
{
public:
    constexpr static const char* name = "ODIMH5";
    constexpr static const char* doc  = R"(
Product defined as in ODIM:

* Obj
* Prod

.. note::
   TODO: find meanings of ODIMh5 values
)";
    using Product::Product;

    ODIMH5* clone() const override { return new ODIMH5(data, size); }

    int compare_local(const ODIMH5& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys,
                         const Formatter* f = 0) const override;
    std::string exactQuery() const override;
};

class VM2 : public Product
{
public:
    constexpr static const char* name = "VM2";
    constexpr static const char* doc  = R"(
Product defined as a VM2 variable ID
)";
    using Product::Product;

    VM2* clone() const override { return new VM2(data, size); }

    bool equals(const Type& o) const override;
    int compare_local(const VM2& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys,
                         const Formatter* f = 0) const override;
    std::string exactQuery() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    void encode_for_indexing(core::BinaryEncoder& enc) const override;
    ValueBag derived_values() const;
};

} // namespace product

} // namespace arki::types

#endif
