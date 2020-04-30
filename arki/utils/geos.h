#ifndef ARKI_UTILS_GEOSDEF_H
#define ARKI_UTILS_GEOSDEF_H

/*
 * utils-geosdef - libgeos definitions
 *
 * Copyright (C) 2009  ARPAE-SIMC <simc-urp@arpae.it>
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

#include <arki/utils/geosfwd.h>
#include <memory>

#ifdef HAVE_GEOS
#include <geos/version.h>

#if GEOS_VERSION < 3

#include <geos/geom.h>
#include <geos/io.h>

#ifdef INLINE
#undef INLINE
#endif

#else

#define USE_UNSTABLE_GEOS_CPP_API
#include <geos/geom/Coordinate.h>
#include <geos/geom/CoordinateArraySequence.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/GeometryCollection.h>
#include <geos/geom/Point.h>
#include <geos/geom/LinearRing.h>
#include <geos/geom/Polygon.h>
#include <geos/io/WKTReader.h>
#undef USE_UNSTABLE_GEOS_CPP_API

#ifdef INLINE
#undef INLINE
#endif

#endif


namespace arki {
namespace utils {
namespace geos {

}
}
}

#endif

#endif
