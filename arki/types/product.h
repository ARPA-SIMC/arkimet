#ifndef ARKI_TYPES_PRODUCT_H
#define ARKI_TYPES_PRODUCT_H

/*
 * types/product - Product metadata item
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types.h>

struct lua_State;

namespace arki {
namespace types {

/**
 * Information on what product (variable measured, variable forecast, ...) is
 * contained in the data.
 */
struct Product : public types::Type
{
	typedef unsigned char Style;

	/// Style values
	//static const unsigned char NONE = 0;
	static const unsigned char GRIB1 = 1;
	static const unsigned char GRIB2 = 2;
	static const unsigned char BUFR = 3;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);
	/// Product style
	virtual Style style() const = 0;

	virtual int compare(const Type& o) const;
	virtual int compare(const Product& o) const;

	virtual std::string tag() const;

	/// CODEC functions
	virtual types::Code serialisationCode() const;
	virtual size_t serialisationSizeLength() const;
	virtual std::string encodeWithoutEnvelope() const;
	static Item<Product> decode(const unsigned char* buf, size_t len);
	static Item<Product> decodeString(const std::string& val);

	// LUA functions
	/// Push to the LUA stack a userdata to access this Origin
	virtual void lua_push(lua_State* L) const;
	/// Callback used for the __index function of the Origin LUA object
	static int lua_lookup(lua_State* L);

	// Deprecated functions
	virtual std::vector<int> toIntVector() const = 0;
	static int getMaxIntCount();
};


namespace product {

struct GRIB1 : public Product
{
	unsigned char origin;
	unsigned char table;
	unsigned char product;

	virtual Style style() const;
	virtual std::string encodeWithoutEnvelope() const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	virtual int compare(const Product& o) const;
	virtual int compare(const GRIB1& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB1> create(unsigned char origin, unsigned char table, unsigned char product);

	// Deprecated functions
	virtual std::vector<int> toIntVector() const;
};

struct GRIB2 : public Product
{
	unsigned short centre;
	unsigned char discipline;
	unsigned char category;
	unsigned char number;

	virtual Style style() const;
	virtual std::string encodeWithoutEnvelope() const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	virtual int compare(const Product& o) const;
	virtual int compare(const GRIB2& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB2> create(unsigned short centre, unsigned char discipline, unsigned char category, unsigned char number);

	// Deprecated functions
	virtual std::vector<int> toIntVector() const;
};

struct BUFR : public Product
{
	unsigned char type;
	unsigned char subtype;
	unsigned char localsubtype;

	virtual Style style() const;
	virtual std::string encodeWithoutEnvelope() const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	virtual int compare(const Product& o) const;
	virtual int compare(const BUFR& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<BUFR> create(unsigned char type, unsigned char subtype, unsigned char localsubtype);

	// Deprecated functions
	virtual std::vector<int> toIntVector() const;
};

}

}
}

// vim:set ts=4 sw=4:
#endif
