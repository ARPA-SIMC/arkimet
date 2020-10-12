#ifndef ARKI_SCAN_NETCDF_H
#define ARKI_SCAN_NETCDF_H

#include <arki/scan.h>
#include <string>
#include <vector>

namespace arki {
class Metadata;

namespace scan {
struct Validator;
class MockEngine;

namespace netcdf {
const Validator& validator();
}

}
}

#endif

