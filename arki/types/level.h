#ifndef ARKI_TYPES_LEVEL_H
#define ARKI_TYPES_LEVEL_H

/*
 * types/level - Vertical level or layer
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
 * The vertical level or layer of some data
 *
 * It can contain information like leveltype and level value.
 */
struct Level : public types::Type
{
	typedef unsigned char Style;

	/// Style values
	static const Style GRIB1 = 1;
	static const Style GRIB2S = 2;
	static const Style GRIB2D = 3;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);
	/// Level style
	virtual Style style() const = 0;

	virtual int compare(const Type& o) const;
	virtual int compare(const Level& o) const;

	virtual std::string tag() const;

	/// CODEC functions
	virtual types::Code serialisationCode() const;
	virtual size_t serialisationSizeLength() const;
	virtual std::string encodeWithoutEnvelope() const;
	static Item<Level> decode(const unsigned char* buf, size_t len);
	static Item<Level> decodeString(const std::string& val);

	// LUA functions
	/// Push to the LUA stack a userdata to access this Level
	virtual void lua_push(lua_State* L) const;
	/// Callback used for the __index function of the Level LUA object
	static int lua_lookup(lua_State* L);
};

namespace level {

struct GRIB1 : public Level
{
    unsigned char type;
	unsigned short l1;
	unsigned char l2;

	virtual Style style() const;
	virtual std::string encodeWithoutEnvelope() const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	/**
	 * Get information on how l1 and l2 should be treated:
	 *
	 * \l 0 means 'l1 and l2 should be ignored'
	 * \l 1 means 'level, only l1 is to be considered'
	 * \l 2 means 'layer from l1 to l2'
	 */
	int valType() const;

	virtual int compare(const Level& o) const;
	virtual int compare(const GRIB1& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB1> create(unsigned char type);
	static Item<GRIB1> create(unsigned char type, unsigned short l1);
	static Item<GRIB1> create(unsigned char type, unsigned char l1, unsigned char l2);

	static int getValType(unsigned char type);
};

struct GRIB2S : public Level
{
    unsigned char type;
    unsigned char scale;
	unsigned long value;

	virtual Style style() const;
	virtual std::string encodeWithoutEnvelope() const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	/**
	 * Get information on how l1 and l2 should be treated:
	 *
	 * \l 0 means 'l1 and l2 should be ignored'
	 * \l 1 means 'level, only l1 is to be considered'
	 * \l 2 means 'layer from l1 to l2'
	 */
	int valType() const;

	virtual int compare(const Level& o) const;
	virtual int compare(const GRIB2S& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB2S> create(unsigned char type, unsigned char scale, unsigned long val);
};

struct GRIB2D : public Level
{
    unsigned char type1;
    unsigned char scale1;
	unsigned long value1;
    unsigned char type2;
    unsigned char scale2;
	unsigned long value2;

	virtual Style style() const;
	virtual std::string encodeWithoutEnvelope() const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	virtual int compare(const Level& o) const;
	virtual int compare(const GRIB2D& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB2D> create(unsigned char type1, unsigned char scale1, unsigned long val1,
                               unsigned char type2, unsigned char scale2, unsigned long val2);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
