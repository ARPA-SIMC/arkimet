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

namespace arki {
namespace dataset {
namespace data {
namespace concat {

class Segment : public fd::Segment
{
public:
    Segment(const std::string& relname, const std::string& absname);

    void append(Metadata& md) override;
    off_t append(const std::vector<uint8_t>& buf) override;
    Pending append(Metadata& md, off_t* ofs) override;

    FileState check(const metadata::Collection& mds, bool quick=true) override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds) override;
};

class HoleSegment : public Segment
{
public:
    HoleSegment(const std::string& relname, const std::string& absname)
        : Segment(relname, absname) {}

    void write(const std::vector<uint8_t>& buf) override;

    Pending repack(const std::string& rootdir, metadata::Collection& mds) override;
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

