#ifndef ARKI_DATA_LINES_H
#define ARKI_DATA_LINES_H

/*
 * data - Read/write functions for data blobs with newline separators
 *
 * Copyright (C) 2012--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/libconfig.h>
#include <arki/dataset/data/fd.h>
#include <string>

namespace wibble {
namespace sys {
class Buffer;
}
}

namespace arki {
namespace dataset {
namespace data {
namespace lines {

class Writer : public fd::Writer
{
public:
    Writer(const std::string& relname, const std::string& absname, bool truncate=false);

    void write(const wibble::sys::Buffer& buf);

    virtual void append(Metadata& md);
    virtual off_t append(const wibble::sys::Buffer& buf);
    virtual Pending append(Metadata& md, off_t* ofs);
};

class Maint : public fd::Maint
{
    FileState check(const std::string& absname, const metadata::Collection& mds, bool quick=true);
    Pending repack(const std::string& rootdir, const std::string& relname, metadata::Collection& mds);
};

class OstreamWriter : public data::OstreamWriter
{
protected:
    sigset_t blocked;

public:
    OstreamWriter();
    virtual ~OstreamWriter();

    virtual size_t stream(const Metadata& md, std::ostream& out) const;
    virtual size_t stream(const Metadata& md, int out) const;
};

}
}
}
}

#endif
