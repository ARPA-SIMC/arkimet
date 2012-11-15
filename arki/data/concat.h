#ifndef ARKI_DATA_CONCAT_H
#define ARKI_DATA_CONCAT_H

/*
 * data - Read/write functions for data blobs without envelope
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

#include <arki/data/impl.h>
#include <string>

namespace arki {
namespace data {
namespace concat {

class Writer : public impl::Writer
{
protected:
    int fd;

    void lock();
    void unlock();

public:
    Writer(const std::string& fname);

    virtual void append(Metadata& md);
};

}
}
}

#endif

