#ifndef ARKI_TYPES_TIMERANGE_H
#define ARKI_TYPES_TIMERANGE_H

/*
 * types/timerange - Time span information
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

/**
 * The time span information of the data
 *
 * It can contain information such as accumulation time, or validity of
 * forecast.
 */
struct Timerange : public types::Type
{
	typedef unsigned char Style;

	/// Style values
	static const Style GRIB1 = 1;
	static const Style GRIB2 = 2;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);
	/// Timerange style
	virtual Style style() const = 0;

	virtual int compare(const Type& o) const;
	virtual int compare(const Timerange& o) const;

	virtual std::string tag() const;

	/// CODEC functions
	virtual types::Code serialisationCode() const;
	virtual size_t serialisationSizeLength() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	static Item<Timerange> decode(const unsigned char* buf, size_t len);
	static Item<Timerange> decodeString(const std::string& val);
	static types::Code typecode();

	// LUA functions
	/// Push to the LUA stack a userdata to access this Timerange
	virtual void lua_push(lua_State* L) const;
	/// Callback used for the __index function of the Timerange LUA object
	static int lua_lookup(lua_State* L);
};

namespace timerange {

struct GRIB1 : public Timerange
{
protected:
	std::ostream& writeNumbers(std::ostream& o) const;

	unsigned char m_type, m_unit;
	unsigned char m_p1, m_p2;

public:
	unsigned type() const { return m_type; }
	unsigned unit() const { return m_unit; }
	unsigned p1() const { return m_p1; }
	unsigned p2() const { return m_p2; }

	enum Unit {
		SECOND = 0,
		MONTH = 1
	};

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;

	virtual int compare(const Timerange& o) const;
	virtual int compare(const GRIB1& o) const;
	virtual bool operator==(const Type& o) const;

	void getNormalised(int& type, Unit& unit, int& p1, int& p2) const;

	static Item<GRIB1> create(unsigned char type, unsigned char unit, unsigned char p1, unsigned char p2);
};

class GRIB2 : public Timerange
{
protected:
	unsigned char m_type, m_unit;
	unsigned long m_p1, m_p2;

public:
	unsigned type() const { return m_type; }
	unsigned unit() const { return m_unit; }
	unsigned p1() const { return m_p1; }
	unsigned p2() const { return m_p2; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;

	virtual int compare(const Timerange& o) const;
	virtual int compare(const GRIB2& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB2> create(unsigned char type, unsigned char unit, unsigned long p1, unsigned long p2);
};

}

}
}

// vim:set ts=4 sw=4:
#endif
