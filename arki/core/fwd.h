#ifndef ARKI_CORE_FWD_H
#define ARKI_CORE_FWD_H

namespace arki {

namespace utils {
namespace sys {
class NamedFileDescriptor;
class File;
}
}

namespace core {

using arki::utils::sys::NamedFileDescriptor;
using arki::utils::sys::File;

struct Stdin;
struct Stdout;
struct Stderr;
struct AbstractInputFile;
struct AbstractOutputFile;

struct BufferedReader;
class LineReader;
class Lock;

namespace lock {
struct Policy;
extern const Policy* policy_null;
extern const Policy* policy_ofd;
}

class Time;
struct Interval;
class FuzzyTime;

namespace cfg {
class Section;
class Sections;
}

struct BinaryEncoder;
struct BinaryDecoder;

class Transaction;
class Pending;

}
}

#endif
