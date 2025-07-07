#ifndef ARKI_DATASET_FWD_H
#define ARKI_DATASET_FWD_H

#include <arki/defs.h>
#include <arki/metadata/fwd.h>
#include <arki/query/fwd.h>

namespace arki {
namespace dataset {

class Session;
class Pool;
class Dataset;
class Reader;
class Writer;
class Checker;
class Reporter;
struct SessionTime;
struct SessionTimeOverride;
struct Step;

namespace local {
class Dataset;
class Reader;
class Writer;
class Checker;
} // namespace local

namespace archive {
class Reader;
class Checker;
} // namespace archive

namespace segmented {
class Reader;
class Writer;
class Checker;
} // namespace segmented

namespace simple {
class Reader;
class Writer;
class Checker;
} // namespace simple

namespace iseg {
class Reader;
class Writer;
class Checker;
} // namespace iseg

} // namespace dataset
} // namespace arki

#endif
