#ifndef ARKI_CORE_FWD_H
#define ARKI_CORE_FWD_H

namespace arki {

namespace utils::sys {
class NamedFileDescriptor;
class File;
} // namespace utils::sys

namespace core {

using arki::utils::sys::File;
using arki::utils::sys::NamedFileDescriptor;

struct Stdin;
struct Stdout;
struct Stderr;
struct AbstractInputFile;

struct BufferedReader;
class LineReader;
class Lock;
class ReadLock;
class AppendLock;
class CheckLock;
class CheckWriteLock;

namespace lock {
struct Policy;
extern const Policy* policy_null;
extern const Policy* policy_ofd;
} // namespace lock

class Time;
struct Interval;
class FuzzyTime;

namespace cfg {
class Section;
class Sections;
} // namespace cfg

struct BinaryEncoder;
struct BinaryDecoder;

class Transaction;
class Pending;

} // namespace core
} // namespace arki

#endif
