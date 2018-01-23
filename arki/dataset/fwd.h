#ifndef ARKI_DATASET_FWD_H
#define ARKI_DATASET_FWD_H

#include <arki/defs.h>

namespace arki {
namespace dataset {

struct DataQuery;
struct ByteQuery;
struct Config;
struct Reader;
struct Writer;
struct Checker;
struct Reporter;

struct Segment;
namespace segment {
struct State;
struct WriterBatchElement;
struct Writer;
struct Checker;
struct Manager;
}

}
}

#endif
