#ifndef ARKI_TYPES_PRODUCT_H
#define ARKI_TYPES_PRODUCT_H

#include <arki/types/styled.h>
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
struct Product : public types::StyledType<Product>
{
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


class GRIB1 : public Product
{
protected:
	unsigned char m_origin;
	unsigned char m_table;
	unsigned char m_product;

public:
    void get_GRIB1(unsigned& origin, unsigned& table, unsigned& product) const
    {
        origin = m_origin;
        table = m_table;
        product = m_product;
    }

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    int compare_local(const Product& o) const override;
    bool equals(const Type& o) const override;

    GRIB1* clone() const override;
    static std::unique_ptr<GRIB1> create(unsigned char origin, unsigned char table, unsigned char product);
    static std::unique_ptr<GRIB1> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

class GRIB2 : public Product
{
protected:
	unsigned short m_centre;
	unsigned char m_discipline;
	unsigned char m_category;
	unsigned char m_number;
    unsigned char m_table_version;
    unsigned char m_local_table_version;

public:
    void get_GRIB2(unsigned& centre, unsigned& discipline, unsigned& category, unsigned& number, unsigned& table_version, unsigned& local_table_version) const
    {
        centre = m_centre;
        discipline = m_discipline;
        category = m_category;
        number = m_number;
        table_version = m_table_version;
        local_table_version = m_local_table_version;
    }

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    int compare_local(const Product& o) const override;
    bool equals(const Type& o) const override;

    GRIB2* clone() const override;
    static std::unique_ptr<GRIB2> create(
            unsigned short centre,
            unsigned char discipline,
            unsigned char category,
            unsigned char number,
            unsigned char table_version=4,
            unsigned char local_table_version=255);
    static std::unique_ptr<GRIB2> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

class BUFR : public Product
{
protected:
	unsigned char m_type;
	unsigned char m_subtype;
	unsigned char m_localsubtype;
	ValueBag m_values;

public:
    void get_BUFR(unsigned& type, unsigned& subtype, unsigned& localsubtype, ValueBag& values) const
    {
        type = m_type;
        subtype = m_subtype;
        localsubtype = m_localsubtype;
        values = m_values;
    }

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    int compare_local(const Product& o) const override;
    bool equals(const Type& o) const override;

    BUFR* clone() const override;
    static std::unique_ptr<BUFR> create(unsigned char type, unsigned char subtype, unsigned char localsubtype);
    static std::unique_ptr<BUFR> create(unsigned char type, unsigned char subtype, unsigned char localsubtype, const ValueBag& name);
    static std::unique_ptr<BUFR> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

class ODIMH5 : public Product
{
protected:
	std::string 	m_obj;		/* attribute /what.object */
	std::string 	m_prod;		/* attribute /dataset/what.product */

	/* NOTE: sometimes /dataset/product requires /dataset/prodpar, but we store prodpar values into other metadata */
	/* REMOVED: double 		m_prodpar1;	 attribute /dataset/what.prodpar */
	/* REMOVED: double 		m_prodpar2;	 attribute /dataset/what.prodpar BIS */

public:
    void get_ODIMH5(std::string& obj, std::string& prod) const
    {
        obj = m_obj;
        prod = m_prod;
    }

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    int compare_local(const Product& o) const override;
    bool equals(const Type& o) const override;

    ODIMH5* clone() const override;
    static std::unique_ptr<ODIMH5> create(const std::string& obj, const std::string& prod
            /*REMOVED:, double prodpar1, double prodpar2*/
    );
    static std::unique_ptr<ODIMH5> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    // Deprecated functions
    std::vector<int> toIntVector() const override;
};

class VM2 : public Product
{
protected:
    unsigned m_variable_id;
    mutable std::unique_ptr<ValueBag> m_derived_values;

public:
    virtual ~VM2() {}

    void get_VM2(unsigned& variable_id) const
    {
        variable_id = m_variable_id;
    }

    const ValueBag& derived_values() const;

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    int compare_local(const Product& o) const override;
    bool equals(const Type& o) const override;

    VM2* clone() const override;
    static std::unique_ptr<VM2> create(unsigned variable_id);
    static std::unique_ptr<VM2> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

}

}
}

#endif
