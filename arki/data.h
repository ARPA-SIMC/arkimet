#ifndef ARKI_DATA_H
#define ARKI_DATA_H

/*
 * data - Read/write functions for data blobs
 *
 * Copyright (C) 2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/transaction.h>
#include <string>
#include <iosfwd>
#include <sys/types.h>

namespace wibble {
namespace sys {
class Buffer;
}
}

namespace arki {
class Metadata;

namespace data {

namespace impl {
class Reader;
class Writer;
}

template<typename T>
class Base
{
protected:
    T* impl;

public:
    Base(T* impl);
    Base(const Base<T>& val);
    ~Base();

    /// Assignment
    Base<T>& operator=(const Base<T>& val);

    const std::string& fname() const;

    /**
     * Return the pointer to the implementation object.
     *
     * This is not part of the API, but it is used to unit test implementation
     * internals.
     */
    T* _implementation() { return impl; }
};

class Reader : public Base<impl::Reader>
{
protected:
    Reader(impl::Reader* impl);

public:
    ~Reader();

    static Reader get(const std::string& format, const std::string& fname);
};

class Writer : public Base<impl::Writer>
{
protected:
    Writer(impl::Writer* impl);

public:
    ~Writer();

    /**
     * Append the data, updating md's source information.
     *
     * In case of write errors (for example, disk full) it tries to truncate
     * the file as it was before, before raising an exception.
     */
    void append(Metadata& md);

    /**
     * Append raw data to the file, wrapping it with the right envelope if
     * needed.
     *
     * All exceptions are propagated upwards without special handling. If this
     * operation fails, the file should be considered invalid.
     *
     * This function is intended to be used by low-level maintenance operations,
     * like a file repack.
     */
    void append(const wibble::sys::Buffer& buf);

    /**
     * Append the data, in a transaction, updating md's source information.
     *
     * At the beginning of the transaction, the file is locked and the write
     * offset is read. Committing the transaction actually writes the data to
     * the file.
     */
    Pending append(Metadata& md, off_t* ofs);

    static Writer get(const std::string& format, const std::string& fname);
};

/**
 * Functor class with format-specific serialization routines
 */
class OstreamWriter
{
public:
    virtual ~OstreamWriter();

    virtual size_t stream(Metadata& md, std::ostream& out) const = 0;
    virtual size_t stream(Metadata& md, int out) const = 0;

    /**
     * Returns a pointer to a static instance of the appropriate OstreamWriter
     * for the given format
     */
    static const OstreamWriter* get(const std::string& format);
};

}
}

#endif
