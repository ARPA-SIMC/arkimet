#ifndef ARKI_CORE_FWD_H
#define ARKI_CORE_FWD_H

namespace arki {

namespace utils {
namespace sys {
struct NamedFileDescriptor;
struct File;
}
}

namespace core {

using arki::utils::sys::NamedFileDescriptor;
using arki::utils::sys::File;

struct Time;
struct FuzzyTime;
struct LineReader;

}
}

#endif
