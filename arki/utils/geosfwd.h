#ifndef ARKI_UTILS_GEOSFWD_H
#define ARKI_UTILS_GEOSFWD_H

/*
 * utils-geosfwd - libgeos forward definitions
 *
 * Copyright (C) 2009--2011  ARPAE-SIMC <simc-urp@arpae.it>
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

#include <arki/libconfig.h>

/*
 * The C++ interface of GEOS is not guaranteed to be stable.
 *
 * The C interface of GEOS is guaranteed to be stable, but it requires
 * reimplementing extensive resource management, which would mean re-wrapping
 * the C GEOS API in C++ wrappers that do proper RAII resource management.
 *
 * At the moment it seems easier to use a local thin adapter layer to make the
 * C++ GEOS API changes manageable. This may change in the future depending on
 * how extensive future GEOS API breaks are going to be.
 */

// Forward-declare geos components
#ifdef HAVE_GEOS
#include <geos/version.h>

namespace geos {
#if GEOS_VERSION_MAJOR < 3
struct Geometry;
struct GeometryFactory;
struct Coordinate;
struct CoordinateArraySequence;
struct LinearRing;
struct WKTReader;
#else
namespace geom {
struct Geometry;
struct GeometryFactory;
struct Coordinate;
struct CoordinateArraySequence;
struct LinearRing;
}
namespace io {
struct WKTReader;
}
#endif
}
#endif

// Bring geos components into a local, stable namespace
namespace arki {
namespace utils {
namespace geos {

#ifdef HAVE_GEOS

#if GEOS_VERSION_MAJOR < 3
using ::geos::Geometry;
using ::geos::GeometryFactory;
using ::geos::Coordinate;
using ::geos::CoordinateArraySequence;
using ::geos::LinearRing;
using ::geos::WKTReader;
#else
using ::geos::geom::Geometry;
using ::geos::geom::GeometryFactory;
using ::geos::geom::Coordinate;
using ::geos::geom::CoordinateArraySequence;
using ::geos::geom::LinearRing;
using ::geos::io::WKTReader;
#endif

#else

struct Geometry {};
struct GeometryFactory {};
struct Coordinate {};

#endif

}
}
}
#endif
