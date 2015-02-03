#ifndef ARKI_DATASET_DATA_CONCAT_H
#define ARKI_DATASET_DATA_CONCAT_H

/*
 * data - Read/write functions for data blobs without envelope
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
namespace concat {

class Writer : public fd::Writer
{
public:
    Writer(const std::string& relname, const std::string& absname, bool truncate=false);

    void append(Metadata& md) override;
    off_t append(const wibble::sys::Buffer& buf) override;
    Pending append(Metadata& md, off_t* ofs) override;
};

class HoleWriter : public Writer
{
public:
    HoleWriter(const std::string& relname, const std::string& absname, bool truncate=false)
        : Writer(relname, absname, truncate) {}

    void write(const wibble::sys::Buffer& buf) override;
};

class Maint : public fd::Maint
{
    FileState check(const std::string& absname, const metadata::Collection& mds, bool quick=true) override;
    Pending repack(const std::string& rootdir, const std::string& relname, metadata::Collection& mds) override;
};

class HoleMaint : public Maint
{
    Pending repack(const std::string& rootdir, const std::string& relname, metadata::Collection& mds) override;
};

class OstreamWriter : public data::OstreamWriter
{
protected:
    sigset_t blocked;

public:
    OstreamWriter();
    virtual ~OstreamWriter();

    size_t stream(Metadata& md, std::ostream& out) const override;
    size_t stream(Metadata& md, int out) const override;
};

}
}
}
}

#endif

