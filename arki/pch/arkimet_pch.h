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

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <fcntl.h>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iosfwd>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sysexits.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <system_error>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <type_traits>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utime.h>
#include <vector>


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
