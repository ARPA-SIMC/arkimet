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
#include <sys/types.h>

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
     * Append the data, in a transaction, updating md's source information.
     *
     * At the beginning of the transaction, the file is locked and the write
     * offset is read. Committing the transaction actually writes the data to
     * the file.
     */
    Pending append(Metadata& md, off_t* ofs);

    static Writer get(const std::string& format, const std::string& fname);
};

}
}

#endif
