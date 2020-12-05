#ifndef ARKI_TYPES_FWD_H
#define ARKI_TYPES_FWD_H

#include <arki/defs.h>

namespace arki {
namespace types {

struct Type;
typedef TypeCode Code;
template<typename T>
class traits;

struct Source;
namespace source {
struct Blob;
struct Inline;
struct Url;
}

struct Note;

struct Reftime;
namespace reftime {
struct Position;
struct Period;
}

struct Origin;
namespace origin {
class GRIB1;
class GRIB2;
class BUFR;
class ODIMH5;
}

struct Product;
namespace product {
class GRIB1;
class GRIB2;
class BUFR;
class ODIMH5;
class VM2;
}

struct Level;
namespace level {
class GRIB1;
class GRIB2;
class GRIB2S;
class GRIB2D;
class ODIMH5;
}

struct Run;
namespace run {
class Minute;
}

struct Timerange;
namespace timerange {
class Timedef;
class GRIB1;
class GRIB2;
class BUFR;
}

struct Area;
namespace area {
class GRIB;
class ODIMH5;
class VM2;
}

struct Quantity;
struct Task;

class ItemSet;
class ValueBag;

}

struct Formatter;

}
#endif
