#ifndef ARKI_UTILS_GEOSFWD_H
#define ARKI_UTILS_GEOSFWD_H

/*
 * utils-geosfwd - libgeos forward definitions
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

#include <arki/config.h>


#ifdef HAVE_GEOS

#if GEOS_VERSION < 3
namespace geos {
struct Geometry;
struct GeometryFactory;
}
#define ARKI_GEOS_NS geos
#else
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

#else

struct DummyGeos {}
#define ARKI_GEOS_GEOMETRY DummyGeos
#define ARKI_GEOS_GEOMETRYFACTORY DummyGeos

#endif

// vim:set ts=4 sw=4:
#endif
