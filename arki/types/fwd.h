#ifndef ARKI_TYPES_FWD_H
#define ARKI_TYPES_FWD_H

#include <arki/defs.h>

namespace arki {
namespace types {

class Type;
typedef TypeCode Code;
template <typename T> struct traits;

class Source;
namespace source {
class Blob;
class Inline;
class Url;
} // namespace source

class Note;

class Reftime;
namespace reftime {
class Position;
class Period;
} // namespace reftime

class Origin;
namespace origin {
class GRIB1;
class GRIB2;
class BUFR;
class ODIMH5;
} // namespace origin

class Product;
namespace product {
class GRIB1;
class GRIB2;
class BUFR;
class ODIMH5;
class VM2;
} // namespace product

class Level;
namespace level {
class GRIB1;
class GRIB2;
class GRIB2S;
class GRIB2D;
class ODIMH5;
} // namespace level

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
} // namespace timerange

class Area;
namespace area {
class GRIB;
class ODIMH5;
class VM2;
} // namespace area

class Quantity;
class Task;

class ItemSet;
class ValueBag;

} // namespace types

class Formatter;

} // namespace arki
#endif
