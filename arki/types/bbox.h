#ifndef ARKI_TYPES_BBOX_H
#define ARKI_TYPES_BBOX_H

/*
 * types/bbox - Bounding box metadata item
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
#include <arki/utils/geosfwd.h>

struct lua_State;

namespace arki {
namespace types {

/**
 * The bbox of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
struct BBox : public types::Type
{
	typedef unsigned char Style;

	/// Style values
	//static const Style NONE = 0;
	static const Style INVALID = 1;
	static const Style POINT = 2;
	static const Style BOX = 3;
	static const Style HULL = 4;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);
	/// BBox style
	virtual Style style() const = 0;

	virtual int compare(const Type& o) const;
	virtual int compare(const BBox& o) const;

	virtual std::string tag() const;

	/// CODEC functions
	virtual types::Code serialisationCode() const;
	virtual size_t serialisationSizeLength() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	static Item<BBox> decode(const unsigned char* buf, size_t len);
	static Item<BBox> decodeString(const std::string& val);

	// LUA functions
	/// Push to the LUA stack a userdata to access this BBox
	virtual void lua_push(lua_State* L) const;
	/// Callback used for the __index function of the BBox LUA object
	static int lua_lookup(lua_State* L);

	// GEOS functions
	/**
	 * Return a new GEOS Geometry for this bounding box.
	 *
	 * The object will need to be deallocated by the caller.
	 *
	 * Invalid bounding boxes can return 0 here.
	 */
	virtual ARKI_GEOS_GEOMETRY* geometry(const ARKI_GEOS_GEOMETRYFACTORY& gf) const = 0;
};

namespace bbox {

struct INVALID : public BBox
{
	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	virtual int compare(const BBox& o) const;
	virtual int compare(const INVALID& o) const;
	virtual bool operator==(const Type& o) const;

	ARKI_GEOS_GEOMETRY* geometry(const ARKI_GEOS_GEOMETRYFACTORY& gf) const;

	static Item<INVALID> create();
};

struct POINT : public BBox
{
	float lat;
	float lon;

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	virtual int compare(const BBox& o) const;
	virtual int compare(const POINT& o) const;
	virtual bool operator==(const Type& o) const;

	ARKI_GEOS_GEOMETRY* geometry(const ARKI_GEOS_GEOMETRYFACTORY& gf) const;

	static Item<POINT> create(float lat, float lon);
};

struct BOX : public BBox
{
	float latmin;
	float latmax;
	float lonmin;
	float lonmax;

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	virtual int compare(const BBox& o) const;
	virtual int compare(const BOX& o) const;
	virtual bool operator==(const Type& o) const;

	ARKI_GEOS_GEOMETRY* geometry(const ARKI_GEOS_GEOMETRYFACTORY& gf) const;

	static Item<BOX> create(
				  float latmin, float latmax,
				  float lonmin, float lonmax);
};

struct HULL : public BBox
{
	std::vector< std::pair<float, float> > points;

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;

	virtual int compare(const BBox& o) const;
	virtual int compare(const HULL& o) const;
	virtual bool operator==(const Type& o) const;

	ARKI_GEOS_GEOMETRY* geometry(const ARKI_GEOS_GEOMETRYFACTORY& gf) const;

	static Item<HULL> create(const std::vector< std::pair<float, float> >& points);
};

}

}
}

// vim:set ts=4 sw=4:
#endif
