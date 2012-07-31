/*
 * iotrace - I/O profiling
 *
 * Copyright (C) 2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#ifndef ARKI_IOTRACE_H
#define ARKI_IOTRACE_H

/** @file
 * I/O profiling
 */

#include <arki/libconfig.h>
#include <vector>
#include <string>
#include <iosfwd>
#include <sys/types.h>

namespace arki {
namespace iotrace {

#ifdef ARKI_IOTRACE

/// Information about one I/O event
struct Event
{
    unsigned file_id;
    off64_t offset;
    size_t size;
    const char* desc;

    // Return the file name
    const std::string& filename() const;
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
    std::ostream& out;

    Logger(std::ostream& out) : out(out) {}

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
void trace_file(const std::string& name, off64_t offset, size_t size, const char* desc);

/// Specialised implementation for C-style filenames
void trace_file(const char* name, off64_t offset, size_t size, const char* desc);

void add_listener(Listener& l);
void remove_listener(Listener& l);

#else

void init() {}
void trace_file(const std::string& name, off64_t offset, size_t size, const char* desc) {}
void trace_file(const char* name, off64_t offset, size_t size, const char* desc) {}

#endif

}
}

#endif
