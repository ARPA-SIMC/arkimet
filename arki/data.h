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

#include <string>

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
    Base(T* impl) : impl(impl)
    {
        impl->ref();
    }

    Base(const Base<T>& val)
        : impl(val.impl)
    {
        impl->ref();
    }

    ~Base() { impl->unref(); }

    /// Assignment
    Base<T>& operator=(const Base<T>& val)
    {
        if (val.impl) val.impl->ref();
        impl->unref();
        impl = val.impl;
        return *this;
    }
};

class Reader : Base<impl::Reader>
{
protected:
    Reader(impl::Reader* impl);

public:
    ~Reader();

    static Reader get(const std::string& format, const std::string& fname);
};

class Writer : Base<impl::Writer>
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

    static Writer get(const std::string& format, const std::string& fname);
};

}
}

#endif
