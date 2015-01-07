#ifndef ARKI_TYPES_PRODUCT_H
#define ARKI_TYPES_PRODUCT_H

/*
 * types/product - Product metadata item
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <memory>

#include <arki/types.h>
#include <arki/values.h>

struct lua_State;

namespace arki {
namespace types {

struct Product;

template<>
struct traits<Product>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * Information on what product (variable measured, variable forecast, ...) is
 * contained in the data.
 */
struct Product : public types::StyledType<Product>
{
	/// Style values
	//static const unsigned char NONE = 0;
	static const unsigned char GRIB1 = 1;
	static const unsigned char GRIB2 = 2;
	static const unsigned char BUFR = 3;
	static const unsigned char ODIMH5 	= 4;
    static const unsigned char VM2 = 5;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /// CODEC functions
    static std::auto_ptr<Product> decode(const unsigned char* buf, size_t len);
    static std::auto_ptr<Product> decodeString(const std::string& val);
    static std::auto_ptr<Product> decodeMapping(const emitter::memory::Mapping& val);

	// Deprecated functions
	virtual std::vector<int> toIntVector() const = 0;
	static int getMaxIntCount();

	static void lua_loadlib(lua_State* L);

	static void init();

    static std::auto_ptr<Product> createGRIB1(unsigned char origin, unsigned char table, unsigned char product);
    static std::auto_ptr<Product> createGRIB2(
            unsigned short centre,
            unsigned char discipline,
            unsigned char category,
            unsigned char number,
            unsigned char table_version=4,
            unsigned char local_table_version=255);
    static std::auto_ptr<Product> createBUFR(unsigned char type, unsigned char subtype, unsigned char localsubtype);
    static std::auto_ptr<Product> createBUFR(unsigned char type, unsigned char subtype, unsigned char localsubtype, const ValueBag& name);
    static std::auto_ptr<Product> createODIMH5(const std::string& obj, const std::string& prod);
    static std::auto_ptr<Product> createVM2(unsigned variable_id);
};


namespace product {

class GRIB1 : public Product
{
protected:
	unsigned char m_origin;
	unsigned char m_table;
	unsigned char m_product;

public:
	unsigned origin() const { return m_origin; }
	unsigned table() const { return m_table; }
	unsigned product() const { return m_product; }

    Style style() const override;
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;

    int compare_local(const Product& o) const override;
    bool equals(const Type& o) const override;

    bool lua_lookup(lua_State* L, const std::string& name) const;

    GRIB1* clone() const override;
    static std::auto_ptr<GRIB1> create(unsigned char origin, unsigned char table, unsigned char product);
    static std::auto_ptr<GRIB1> decodeMapping(const emitter::memory::Mapping& val);

    // Deprecated functions
    std::vector<int> toIntVector() const override;
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
	unsigned centre() const { return m_centre; }
	unsigned discipline() const { return m_discipline; }
	unsigned category() const { return m_category; }
	unsigned number() const { return m_number; }
    unsigned table_version() const { return m_table_version; }
    unsigned local_table_version() const { return m_local_table_version; }

    Style style() const override;
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;

    int compare_local(const Product& o) const override;
    bool equals(const Type& o) const override;

    bool lua_lookup(lua_State* L, const std::string& name) const override;

    GRIB2* clone() const override;
    static std::auto_ptr<GRIB2> create(
            unsigned short centre,
            unsigned char discipline,
            unsigned char category,
            unsigned char number,
            unsigned char table_version=4,
            unsigned char local_table_version=255);
    static std::auto_ptr<GRIB2> decodeMapping(const emitter::memory::Mapping& val);

    // Deprecated functions
    std::vector<int> toIntVector() const override;
};

class BUFR : public Product
{
protected:
	unsigned char m_type;
	unsigned char m_subtype;
	unsigned char m_localsubtype;
	ValueBag m_values;

public:
	unsigned type() const { return m_type; }
	unsigned subtype() const { return m_subtype; }
	unsigned localsubtype() const { return m_localsubtype; }
	const ValueBag& values() const { return m_values; }

    // Add/replace these key,value pairs into m_values
    void addValues(const ValueBag& newvalues);

    Style style() const override;
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;

    int compare_local(const Product& o) const override;
    bool equals(const Type& o) const override;

    void lua_register_methods(lua_State* L) const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    BUFR* clone() const override;
    static std::auto_ptr<BUFR> create(unsigned char type, unsigned char subtype, unsigned char localsubtype);
    static std::auto_ptr<BUFR> create(unsigned char type, unsigned char subtype, unsigned char localsubtype, const ValueBag& name);
    static std::auto_ptr<BUFR> decodeMapping(const emitter::memory::Mapping& val);

    // Deprecated functions
    std::vector<int> toIntVector() const override;
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
	inline std::string obj() 	const { return m_obj; }
	inline std::string prod() 	const { return m_prod; }

	/* REMOVED: inline double prodpar1() 	const { return m_prodpar1; } */
	/* REMOVED: inline double prodpar2() 	const { return m_prodpar2; } */

    Style style() const override;
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;

    int compare_local(const Product& o) const override;
    bool equals(const Type& o) const override;

    void lua_register_methods(lua_State* L) const override;
    bool lua_lookup(lua_State* L, const std::string& name) const override;

    ODIMH5* clone() const override;
    static std::auto_ptr<ODIMH5> create(const std::string& obj, const std::string& prod
            /*REMOVED:, double prodpar1, double prodpar2*/
    );
    static std::auto_ptr<ODIMH5> decodeMapping(const emitter::memory::Mapping& val);

    // Deprecated functions
    std::vector<int> toIntVector() const override;
};

class VM2 : public Product
{
protected:
    unsigned m_variable_id;
    mutable std::auto_ptr<ValueBag> m_derived_values;

public:
    virtual ~VM2() {}

    unsigned variable_id() const { return m_variable_id; }
    const ValueBag& derived_values() const;

    Style style() const override;
    void encodeWithoutEnvelope(utils::codec::Encoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialiseLocal(Emitter& e, const Formatter* f=0) const override;
    std::string exactQuery() const override;
    const char* lua_type_name() const override;

    int compare_local(const Product& o) const override;
    bool equals(const Type& o) const override;

    bool lua_lookup(lua_State* L, const std::string& name) const override;

    VM2* clone() const override;
    static std::auto_ptr<VM2> create(unsigned variable_id);
    static std::auto_ptr<VM2> decodeMapping(const emitter::memory::Mapping& val);

    std::vector<int> toIntVector() const override;
};

}

}
}

// vim:set ts=4 sw=4:
#endif
