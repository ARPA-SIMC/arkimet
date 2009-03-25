#ifndef ARKI_BBOX_H
#define ARKI_BBOX_H

/*
 * arki/bbox - Compute bounding boxes
 *
 * Copyright (C) 2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <string>
#include <arki/bbox.h>
#include <arki/types/area.h>
#include <arki/utils-lua.h>
#include <arki/config.h>

#ifdef HAVE_GEOS
#if GEOS_VERSION < 3
#include <geos/geom.h>
#ifdef INLINE
#undef INLINE
#endif

namespace geos {
struct Geometry;
struct GeometryFactory;
}

typedef DefaultCoordinateSequence CoordinateArraySequence;
#define ARKI_GEOS_NS geos
#else
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Point.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Polygon.h>
#ifdef INLINE
#undef INLINE
#endif

namespace geos {
namespace geom {
struct Geometry;
struct GeometryFactory;
}
}
#define ARKI_GEOS_NS geos::geom
#endif
#define ARKI_GEOS_GEOMETRY ARKI_GEOS_NS::Geometry
#define ARKI_GEOS_GEOMETRYFACTORY ARKI_GEOS_NS::GeometryFactory
#endif



namespace arki {

class BBox
{
protected:
	Lua *L;
	ARKI_GEOS_GEOMETRYFACTORY* gf;
	unsigned funcCount;

public:
	BBox(const std::string& code = std::string());
	virtual ~BBox();

	/**
	 * Compute the bounding box for an area.
	 *
	 * The lua code must produce a table called 'bbox' that contains a
	 * vector of (lat, lon) pairs.
	 *
	 * @return the Geometry object with the bounding box, or 0 if the
	 * computation is unsupported for this area.
	 */
	virtual ARKI_GEOS_GEOMETRY* operator()(const Item<types::Area>& v) const;
};

}

// vim:set ts=4 sw=4:
#endif
