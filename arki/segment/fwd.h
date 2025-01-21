#ifndef ARKI_SEGMENT_FWD_H
#define ARKI_SEGMENT_FWD_H

#include <arki/defs.h>
#include <arki/segment/defs.h>
#include <arki/segment/data/fwd.h>

namespace arki {
class Segment;

namespace segment {
class Reporter;
class Session;
class Reader;
class Checker;
class Fixer;
class Data;

namespace data {
class Reader;
class Writer;
struct WriterConfig;
class Checker;
struct RepackConfig;
}

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
}

}

}

#endif

