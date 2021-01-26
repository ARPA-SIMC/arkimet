#ifndef ARKI_TYPES_FWD_H
#define ARKI_TYPES_FWD_H

#include <arki/defs.h>

namespace arki {
namespace types {

class Type;
typedef TypeCode Code;
template<typename T>
struct traits;

class Source;
namespace source {
class Blob;
class Inline;
class Url;
}

class Note;

class Reftime;
namespace reftime {
class Position;
class Period;
}

class Origin;
namespace origin {
class GRIB1;
class GRIB2;
class BUFR;
class ODIMH5;
}

class Product;
namespace product {
class GRIB1;
class GRIB2;
class BUFR;
class ODIMH5;
class VM2;
}

class Level;
namespace level {
class GRIB1;
class GRIB2;
class GRIB2S;
class GRIB2D;
class ODIMH5;
}

class Run;
namespace run {
class Minute;
}

class Timerange;
namespace timerange {
class Timedef;
class GRIB1;
class GRIB2;
class BUFR;
}

class Area;
namespace area {
class GRIB;
class ODIMH5;
class VM2;
}

class Quantity;
class Task;

class ItemSet;
class ValueBag;

}

class Formatter;

}
#endif
