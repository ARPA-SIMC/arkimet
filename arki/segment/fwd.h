#ifndef ARKI_SEGMENT_FWD_H
#define ARKI_SEGMENT_FWD_H

#include <arki/defs.h>
#include <arki/segment/data/fwd.h>
#include <arki/segment/defs.h>

namespace arki {
class Segment;

namespace segment {
class Reporter;
class Session;
class Reader;
struct WriterConfig;
class Writer;
class Checker;
class Fixer;
struct RepackConfig;

namespace metadata {
class Reader;
}

namespace scan {
class Reader;
}

namespace iseg {
class Segment;
class Session;
class Reader;
} // namespace iseg

} // namespace segment

} // namespace arki

#endif
