#ifndef ARKI_TYPES_PRODUCT_H
#define ARKI_TYPES_PRODUCT_H

#include <arki/types/encoded.h>
#include <arki/types/values.h>
#include <memory>

namespace arki {
namespace types {

namespace product {

/// Style values
enum class Style: unsigned char {
    GRIB1 = 1,
    GRIB2 = 2,
    BUFR = 3,
    ODIMH5 = 4,
    VM2 = 5,
};

}

template<>
struct traits<Product>
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
public:
    using Encoded::Encoded;

    typedef product::Style Style;

    types::Code type_code() const override { return traits<Product>::type_code; }
    size_t serialisationSizeLength() const override { return traits<Product>::type_sersize_bytes; }
    std::string tag() const override { return traits<Product>::type_tag; }

    Style style() const;
    void get_GRIB1(unsigned& origin, unsigned& table, unsigned& product) const;
    void get_GRIB2(unsigned& centre, unsigned& discipline, unsigned& category, unsigned& number, unsigned& table_version, unsigned& local_table_version) const;
    void get_BUFR(unsigned& type, unsigned& subtype, unsigned& localsubtype, ValueBag& values) const;
    void get_ODIMH5(std::string& obj, std::string& prod) const;
    void get_VM2(unsigned& variable_id) const;

    int compare(const Type& o) const override;

    /// Convert a string into a style
    static Style parseStyle(const std::string& str);
    /// Convert a style into its string representation
    static std::string formatStyle(Style s);

    /// CODEC functions

    static std::unique_ptr<Product> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<Product> decodeString(const std::string& val);
    static std::unique_ptr<Product> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    static void init();

    static std::unique_ptr<Product> createGRIB1(unsigned char origin, unsigned char table, unsigned char product);
    static std::unique_ptr<Product> createGRIB2(
            unsigned short centre,
            unsigned char discipline,
            unsigned char category,
            unsigned char number,
            unsigned char table_version=4,
            unsigned char local_table_version=255);
    static std::unique_ptr<Product> createBUFR(unsigned char type, unsigned char subtype, unsigned char localsubtype);
    static std::unique_ptr<Product> createBUFR(unsigned char type, unsigned char subtype, unsigned char localsubtype, const ValueBag& name);
    static std::unique_ptr<Product> createODIMH5(const std::string& obj, const std::string& prod);
    static std::unique_ptr<Product> createVM2(unsigned variable_id);
};

namespace product {

inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Product::formatStyle(s); }

struct GRIB1 : public Product
{
public:
    using Product::Product;

    GRIB1* clone() const override { return new GRIB1(data, size); }

    int compare_local(const GRIB1& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;
};

struct GRIB2 : public Product
{
public:
    using Product::Product;

    GRIB2* clone() const override { return new GRIB2(data, size); }

    int compare_local(const GRIB2& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;
};

struct BUFR : public Product
{
public:
    using Product::Product;

    BUFR* clone() const override { return new BUFR(data, size); }

    int compare_local(const BUFR& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;
};

struct ODIMH5 : public Product
{
public:
    using Product::Product;

    ODIMH5* clone() const override { return new ODIMH5(data, size); }

    int compare_local(const ODIMH5& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;
};

struct VM2 : public Product
{
public:
    using Product::Product;

    VM2* clone() const override { return new VM2(data, size); }

    bool equals(const Type& o) const override;
    int compare_local(const VM2& o) const;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    void encode_for_indexing(core::BinaryEncoder& enc) const override;
    ValueBag derived_values() const;
};


}

}
}

#endif
