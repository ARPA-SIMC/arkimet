#ifndef ARKI_DATA_FD_H
#define ARKI_DATA_FD_H

/*
 * data - Base class for unix fd based read/write functions
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

#include <arki/data.h>
#include <string>

namespace wibble {
namespace sys {
class Buffer;
}
}

namespace arki {
namespace data {
namespace fd {

class Writer : public data::Writer
{
protected:
    int fd;

public:
    Writer(const std::string& relname, const std::string& absname, bool truncate=false);
    ~Writer();

    void lock();
    void unlock();
    off_t wrpos();
    void write(const wibble::sys::Buffer& buf);
    void truncate(off_t pos);
};

}
}
}

#endif

