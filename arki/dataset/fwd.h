#ifndef ARKI_DATASET_FWD_H
#define ARKI_DATASET_FWD_H

#include <arki/defs.h>

namespace arki {
namespace dataset {

class Session;
class Pool;
struct DataQuery;
struct ByteQuery;
class Dataset;
class Reader;
class Writer;
struct WriterBatchElement;
class WriterBatch;
class Checker;
class Reporter;
class QueryProgress;
struct SessionTime;
struct SessionTimeOverride;

namespace local {
class Dataset;
class Reader;
class Writer;
class Checker;
}

namespace archive {
class Reader;
class Checker;
}

namespace segmented {
class Reader;
class Writer;
class Checker;
}

namespace simple {
class Reader;
class Writer;
class Checker;
}

namespace iseg {
class Reader;
class Writer;
class Checker;
}

}
}

#endif
