#ifndef ARKI_TYPES_LEVEL_H
#define ARKI_TYPES_LEVEL_H

/*
 * types/level - Vertical level or layer
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

struct Level;

template<>
struct traits<Level>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The vertical level or layer of some data
 *
 * It can contain information like leveltype and level value.
 */
struct Level : public types::StyledType<Level>
{
	/// Style values
	static const Style GRIB1 = 1;
	static const Style GRIB2S = 2;
	static const Style GRIB2D = 3;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

	/// CODEC functions
	static Item<Level> decode(const unsigned char* buf, size_t len);
	static Item<Level> decodeString(const std::string& val);

	static void lua_loadlib(lua_State* L);
};

namespace level {

class GRIB1 : public Level
{
protected:
	unsigned char m_type;
	unsigned short m_l1;
	unsigned char m_l2;

public:
	unsigned type() const { return m_type; }
	unsigned l1() const { return m_l1; }
	unsigned l2() const { return m_l2; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	/**
	 * Get information on how l1 and l2 should be treated:
	 *
	 * \l 0 means 'l1 and l2 should be ignored'
	 * \l 1 means 'level, only l1 is to be considered'
	 * \l 2 means 'layer from l1 to l2'
	 */
	int valType() const;

	virtual int compare_local(const Level& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB1> create(unsigned char type);
	static Item<GRIB1> create(unsigned char type, unsigned short l1);
	static Item<GRIB1> create(unsigned char type, unsigned char l1, unsigned char l2);

	static int getValType(unsigned char type);
};

struct GRIB2 : public Level
{
	/**
	 * Get information on how l1 and l2 should be treated:
	 *
	 * \l 0 means 'l1 and l2 should be ignored'
	 * \l 1 means 'level, only l1 is to be considered'
	 * \l 2 means 'layer from l1 to l2'
	 */
	//int valType() const;
};

class GRIB2S : public GRIB2
{
protected:
	unsigned char m_type;
	unsigned char m_scale;
	unsigned long m_value;

public:
	unsigned type() const { return m_type; }
	unsigned scale() const { return m_scale; }
	unsigned value() const { return m_value; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Level& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB2S> create(unsigned char type, unsigned char scale, unsigned long val);
};

class GRIB2D : public GRIB2
{
protected:
	unsigned char m_type1;
	unsigned char m_scale1;
	unsigned long m_value1;
	unsigned char m_type2;
	unsigned char m_scale2;
	unsigned long m_value2;

public:
	unsigned type1() const { return m_type1; }
	unsigned scale1() const { return m_scale1; }
	unsigned value1() const { return m_value1; }
	unsigned type2() const { return m_type2; }
	unsigned scale2() const { return m_scale2; }
	unsigned value2() const { return m_value2; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Level& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB2D> create(unsigned char type1, unsigned char scale1, unsigned long val1,
                               unsigned char type2, unsigned char scale2, unsigned long val2);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
