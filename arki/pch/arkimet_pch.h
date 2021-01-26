// List of headers to precompile.
// See https://github.com/mesonbuild/meson/blob/master/docs/markdown/Precompiled-headers.md
//
// Hint: git grep -h '#include <'| sort | uniq -c | sort -n
//
// If you wish to compile your project without precompiled headers, you can
// change the value of the pch option by passing -Db_pch=false argument to
// Meson at configure time or later with meson configure.
//
// Note that adding a header here will have it always included when compiling.
// Turn off precompiled headers to check include consistency.
//
// Note also that changing one of the included headers will trigger a rebuild
// of everything.

#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include <cstring>
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdexcept>
#include <set>
#include <sys/stat.h>
#include <map>
#include <system_error>
#include <functional>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <iomanip>
#include <ctime>
#include <sys/uio.h>
#include <sys/time.h>
#include <signal.h>
#include <iostream>
#include <iosfwd>
#include <sys/fcntl.h>
#include <ostream>
#include <cstddef>
#include <cctype>

// #include "arki/core/cfg.h"
// #include "arki/core/file.h"
// #include "arki/core/fwd.h"
// #include "arki/core/time.h"
// #include "arki/core/transaction.h"
// #include "arki/dataset.h"
// #include "arki/dataset/fwd.h"
// #include "arki/dataset/impl.h"
// #include "arki/defs.h"
// #include "arki/libconfig.h"
// #include "arki/matcher.h"
// #include "arki/matcher/fwd.h"
// #include "arki/matcher/utils.h"
// #include "arki/metadata.h"
// #include "arki/metadata/collection.h"
// #include "arki/metadata/fwd.h"
// #include "arki/segment.h"
// #include "arki/segment/base.h"
// #include "arki/segment/fwd.h"
// #include "arki/structured/fwd.h"
// #include "arki/summary.h"
// #include "arki/types.h"
// #include "arki/types/encoded.h"
// #include "arki/types/fwd.h"
// #include "arki/utils/string.h"
