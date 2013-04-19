#ifndef ARKI_DATA_IMPL_H
#define ARKI_DATA_IMPL_H

/*
 * data - Read/write functions for data blobs, basic implementation
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
#include <map>
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

/**
 * Registry of readers or writers.
 *
 * Holds shared instances indexed by file names, so that readers/writers to the
 * same file can be reused.
 */
template<typename T>
struct Registry
{
    std::map<std::string, T*> reg;

    /**
     * Get the shared instance for the given file, if available, else return 0
     */
    T* get(const std::string& fname) const;

    /// Add a new instance
    T* add(T* val);

    /// Drop an instance when not used anymore
    void drop(const std::string& fname);

    static Registry<T>& registry();
};

template<typename T>
class Base
{
protected:
    mutable int _ref;

public:
    std::string fname;

    /// Increment the reference count to this Data object
    void ref() const { ++_ref; }

    /**
     * Decrement the reference count to this Data object, and delete the object
     * if the reference count goes down to 0
     */
    void unref()
    {
        if ((--_ref) == 0)
        {
            Registry<T>::registry().drop(fname);
            delete this;
        }
    }

    Base(const std::string& fname)
        : _ref(0), fname(fname) {}
    virtual ~Base() {}

    static Registry<T>& registry() { return Registry<T>::registry(); }
};

struct Reader : public Base<Reader>
{
    Reader(const std::string& fname) : Base<Reader>(fname) {}
    virtual ~Reader();
};

struct Writer : public Base<Writer>
{
    Writer(const std::string& fname) : Base<Writer>(fname) {}
    virtual ~Writer();

    /**
     * Append the data, updating md's source information.
     *
     * In case of write errors (for example, disk full) it tries to truncate
     * the file as it was before, before raising an exception.
     */
    virtual void append(Metadata& md) = 0;

    /**
     * Append raw data to the file, wrapping it with the right envelope if
     * needed.
     */
    virtual void append(const wibble::sys::Buffer& buf) = 0;

    /**
     * Append the data, in a transaction, updating md's source information.
     *
     * At the beginning of the transaction, the file is locked and the write
     * offset is read. Committing the transaction actually writes the data to
     * the file.
     */
    virtual Pending append(Metadata& md, off_t* ofs) = 0;

    /**
     * Read the write offset
     */
    virtual off_t wrpos() = 0;
};


}
}
}

#endif
