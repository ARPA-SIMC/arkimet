#ifndef ARKI_TYPES_AREA_H
#define ARKI_TYPES_AREA_H

/*
 * types/area - Geographical area
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
#include <arki/values.h>
#include <arki/utils/geosfwd.h>

struct lua_State;

namespace arki {
namespace types {

struct Area;

template<>
struct traits<Area>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The vertical area or layer of some data
 *
 * It can contain information like areatype and area value.
 */
struct Area : public types::StyledType<Area>
{
	mutable ARKI_GEOS_GEOMETRY* cached_bbox;

	/// Style values
	static const Style GRIB = 1;

	Area();

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

	/// CODEC functions
	static Item<Area> decode(const unsigned char* buf, size_t len);
	static Item<Area> decodeString(const std::string& val);

	/// Return the geographical bounding box
	const ARKI_GEOS_GEOMETRY* bbox() const;
};

namespace area {

class GRIB : public Area
{
protected:
	ValueBag m_values;

public:
	virtual ~GRIB();

	const ValueBag& values() const { return m_values; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Area& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB> create(const ValueBag& values);
};

}

}
}

// vim:set ts=4 sw=4:
#endif
