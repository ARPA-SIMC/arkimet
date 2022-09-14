#ifndef ARKI_UTILS_GEOSFWD_H
#define ARKI_UTILS_GEOSFWD_H

/// libgeos forward definitions

#include <arki/libconfig.h>

/*
 * The C++ interface of GEOS is not guaranteed to be stable.
 *
 * The C interface of GEOS is guaranteed to be stable, but it requires
 * re-wrapping in C++ to get proper RAII resource management.
 *
 * We used a thin layer of compatibility glue to deal with the C++ API.
 * However, due to packaging issues with the C++ APIs (see
 * https://github.com/ARPA-SIMC/arkimet/issues/291), we now have a C++ wrapper
 * to the C wrapper to the C++ GEOS API.
 */

namespace arki {
namespace utils {
namespace geos {

class GeometryVector;
class Geometry;
class WKTReader;
class WKTWriter;

}
}
}
#endif
