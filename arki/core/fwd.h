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
namespace lock {
struct Policy;
struct NullPolicy;
struct OFDPolicy;
extern const Policy* policy_null;
extern const Policy* policy_ofd;
}

struct Time;
struct FuzzyTime;

}
}

#endif
