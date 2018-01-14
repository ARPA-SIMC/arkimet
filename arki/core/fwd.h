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
struct LineReader;
struct Lock;
namespace lock {
struct Policy;
extern const Policy* policy_null;
extern const Policy* policy_ofd;
}

struct Time;
struct FuzzyTime;

}
}

#endif
