#ifndef ARKI_DATA_CONCAT_H
#define ARKI_DATA_CONCAT_H

/*
 * data - Read/write functions for data blobs without envelope
 *
 * Copyright (C) 2012--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/data.h>
#include <arki/data/fd.h>
#include <string>

namespace wibble {
namespace sys {
class Buffer;
}
}

namespace arki {
namespace data {
namespace concat {

class Writer : public fd::Writer
{
public:
    Writer(const std::string& fname);

    virtual void append(Metadata& md);
    virtual off_t append(const wibble::sys::Buffer& buf);
    virtual Pending append(Metadata& md, off_t* ofs);
};

class OstreamWriter : public data::OstreamWriter
{
protected:
    sigset_t blocked;

public:
    OstreamWriter();
    virtual ~OstreamWriter();

    virtual size_t stream(Metadata& md, std::ostream& out) const;
    virtual size_t stream(Metadata& md, int out) const;
};

class Info : public data::Info
{
public:
    virtual ~Info();

    virtual void raw_to_wrapped(off_t& offset, size_t& size) const;
};

}
}
}

#endif

