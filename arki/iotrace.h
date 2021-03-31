/// iotrace - I/O profiling

#ifndef ARKI_IOTRACE_H
#define ARKI_IOTRACE_H

/** @file
 * I/O profiling
 */

#include <arki/libconfig.h>
#include <arki/core/fwd.h>
#include <arki/stream/fwd.h>
#include <vector>
#include <string>
#include <cstddef>
#include <sys/types.h>

namespace arki {
namespace iotrace {

#ifdef ARKI_IOTRACE

/// Information about one I/O event
struct Event
{
    std::string filename;
    off_t offset;
    size_t size;
    const char* desc;
};

/// Abstract interface for I/O trace events listeners
struct Listener
{
    virtual ~Listener() {}
    virtual void operator()(const Event& e) = 0;
};

/**
 * Collect I/O trace events during its lifetime
 */
struct Collector : public Listener
{
    std::vector<Event> events;

    Collector();
    ~Collector();

    virtual void operator()(const Event& e);

    void dump(std::ostream& out) const;
};

struct Logger : public Listener
{
    FILE* out;

    Logger(FILE* out) : out(out) {}

    virtual void operator()(const Event& e);
};

/**
 * Init the iotrace system.
 *
 * This must be called before tracing happens, or the trace_file() functions
 * will segfault.
 */
void init();

/**
 * Trace file I/O
 *
 * @param name Name of the file where I/O happens
 * @param offset Offset of the I/O operation
 * @param size Size of the I/O operation
 * @param desc
 *   Description of the I/O operation. It MUST be some static string, since its
 *   contents are not copied but only a pointer to it is kepy.
 */
void trace_file(const std::string& name, off_t offset, size_t size, const char* desc);

/// Specialised implementation for C-style filenames
void trace_file(const char* name, off_t offset, size_t size, const char* desc);

/// Specialised implementation NamedFileDescriptor
void trace_file(core::NamedFileDescriptor& fd, off_t offset, size_t size, const char* desc);

/// Specialised implementation AbstractInputFile
void trace_file(core::AbstractInputFile& fd, off_t offset, size_t size, const char* desc);

/// Specialised implementation StreamOutput
void trace_file(StreamOutput& fd, off_t offset, size_t size, const char* desc);

void add_listener(Listener& l);
void remove_listener(Listener& l);

#else

void init() {}
void trace_file(const std::string& name, off_t offset, size_t size, const char* desc) {}
void trace_file(const char* name, off_t offset, size_t size, const char* desc) {}

#endif

}
}

#endif
