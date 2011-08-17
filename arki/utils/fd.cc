/*
 * utils/fd - utility functions to work with file descriptors
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/utils/fd.h>
#include <wibble/exception.h>

#include <sys/stat.h>
#include <unistd.h>


using namespace std;
using namespace wibble;

namespace arki {
namespace utils {
namespace fd {

void HandleWatch::close()
{
    if (fd > 0)
    {
        if (::close(fd) < 0)
            throw wibble::exception::File(fname, "closing file");
        fd = -1;
    }
}

TempfileHandleWatch::~TempfileHandleWatch()
{
    if (fd > 0)
    {
        close();
        ::unlink(fname.c_str());
    }
}

void write_all(int fd, const void* buf, size_t len)
{
    size_t done = 0;
    while (done < len)
    {
        ssize_t res = write(fd, (const unsigned char*)buf+done, len-done);
        if (res < 0)
            throw wibble::exception::System("writing to file descriptor");
        done += res;
    }
}

}
}
}
// vim:set ts=4 sw=4:
