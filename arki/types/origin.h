#ifndef ARKI_TYPES_ORIGIN_H
#define ARKI_TYPES_ORIGIN_H

/*
 * types/origin - Originating centre metadata item
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

struct Origin;

template<>
struct traits<Origin>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The origin of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
struct Origin : public types::StyledType<Origin>
{
	/// Style values
	//static const Style NONE = 0;
	static const Style GRIB1 = 1;
	static const Style GRIB2 = 2;
	static const Style BUFR = 3;
	static const Style ODIMH5 = 4;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

	/// CODEC functions
	static Item<Origin> decode(const unsigned char* buf, size_t len);
	static Item<Origin> decodeString(const std::string& val);

	// Deprecated functions
	virtual std::vector<int> toIntVector() const = 0;
	static int getMaxIntCount();

	static void lua_loadlib(lua_State* L);
};

namespace origin {

class GRIB1 : public Origin
{
protected:
	unsigned char m_centre;
	unsigned char m_subcentre;
	unsigned char m_process;

public:
	virtual ~GRIB1();

	unsigned centre() const { return m_centre; }
	unsigned subcentre() const { return m_subcentre; }
	unsigned process() const { return m_process; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Origin& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB1> create(unsigned char centre, unsigned char subcentre, unsigned char process);

	// Deprecated functions
	virtual std::vector<int> toIntVector() const;
};

class GRIB2 : public Origin
{
protected:
	unsigned short m_centre;
	unsigned short m_subcentre;
	unsigned char m_processtype;
	unsigned char m_bgprocessid;
	unsigned char m_processid;

public:
	virtual ~GRIB2();

	unsigned centre() const { return m_centre; }
	unsigned subcentre() const { return m_subcentre; }
	unsigned processtype() const { return m_processtype; }
	unsigned bgprocessid() const { return m_bgprocessid; }
	unsigned processid() const { return m_processid; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Origin& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB2> create(unsigned short centre, unsigned short subcentre,
				  unsigned char processtype, unsigned char bgprocessid, unsigned char processid);

	// Deprecated functions
	virtual std::vector<int> toIntVector() const;
};

class BUFR : public Origin
{
protected:
	unsigned char m_centre;
	unsigned char m_subcentre;

public:
	virtual ~BUFR();

	unsigned centre() const { return m_centre; }
	unsigned subcentre() const { return m_subcentre; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Origin& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<BUFR> create(unsigned char centre, unsigned char subcentre);

	// Deprecated functions
	virtual std::vector<int> toIntVector() const;
};

class ODIMH5 : public Origin
{
protected:
	std::string m_WMO;
	std::string m_RAD;
	std::string m_PLC;

public:
	virtual ~ODIMH5();

	const std::string& getWMO() const { return m_WMO; }
	const std::string& getRAD() const { return m_RAD; }
	const std::string& getPLC() const { return m_PLC; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Origin& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<ODIMH5> create(const std::string& wmo, const std::string& rad, const std::string& plc);

	// Deprecated functions
	virtual std::vector<int> toIntVector() const;
};

}

}

}

// vim:set ts=4 sw=4:
#endif
