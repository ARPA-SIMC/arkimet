#ifndef ARKI_DATASET_DATA_FD_H
#define ARKI_DATASET_DATA_FD_H

/*
 * data - Base class for unix fd based read/write functions
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
#include <arki/dataset/data.h>
#include <string>

namespace arki {
namespace dataset {
namespace data {
namespace fd {

class Segment : public data::Segment
{
protected:
    int fd;

public:
    Segment(const std::string& relname, const std::string& absname);
    ~Segment();

    void open();
    void truncate_and_open();
    void close();
    void lock();
    void unlock();
    off_t wrpos();
    virtual void write(const std::vector<uint8_t>& buf);
    void fdtruncate(off_t pos);

    size_t remove();
    void truncate(size_t offset);

    virtual FileState check(const metadata::Collection& mds, unsigned max_gap=0, bool quick=true);

    /**
     * If skip_validation is true, repack will skip validating the data that is
     * being read.
     *
     * This is only used during tests to support repacking files with mock data
     * inside. The files are made of filesystem holes, so the data that is read
     * from them is always zeroes.
     */
    static Pending repack(
            const std::string& rootdir,
            const std::string& relname,
            metadata::Collection& mds,
            data::Segment* make_repack_segment(const std::string&, const std::string&),
            bool skip_validation=false);
};

}
}
}
}

#endif

