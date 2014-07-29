#ifndef ARKI_DATASET_DATA_DIR_H
#define ARKI_DATASET_DATA_DIR_H

/*
 * data/dir - Directory based data collection
 *
 * Copyright (C) 2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/data.h>

namespace arki {
class Metadata;

namespace dataset {
namespace data {
namespace dir {

class Writer : public data::Writer
{
protected:
    std::string format;
    std::string seqfile;

public:
    Writer(const std::string& format, const std::string& relname, const std::string& absname, bool truncate=false);
    ~Writer();

    virtual void append(Metadata& md);
    virtual off_t append(const wibble::sys::Buffer& buf);
    virtual Pending append(Metadata& md, off_t* ofs);
    /**
     * Append a hardlink to the data pointed by md.
     *
     * This of course only works if it is possible to hardlink from this
     * segment to the file pointed by md. That is, if they are in the same file
     * system.
     *
     * @returns the offset in the segment at which md was appended
     */
    off_t link(const std::string& absname);
};

struct Maint : public data::Maint
{
    FileState check(const std::string& absname, const metadata::Collection& mds, bool quick=true);
    size_t remove(const std::string& absname);
    void truncate(const std::string& absname, size_t offset);
    Pending repack(const std::string& rootdir, const std::string& relname, metadata::Collection& mds);
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


}
}
}
}

#endif
