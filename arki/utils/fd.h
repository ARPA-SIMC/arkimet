#ifndef ARKI_UTILS_FD_H
#define ARKI_UTILS_FD_H

/*
 * utils/fd - utility functions to work with file descriptors
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
namespace utils {
namespace fd {

/**
 * Ensure that a posix file handle is closed when this object goes out of scope
 */
struct HandleWatch
{
    std::string fname;
    int fd;
    HandleWatch(const std::string& fname, int fd) : fname(fname), fd(fd) {}
    ~HandleWatch() { close(); }

    int release()
    {
        int res = fd;
        fd = -1;
        return res;
    }

    void close();

private:
    // Disallow copy
    HandleWatch(const HandleWatch&);
    HandleWatch& operator=(const HandleWatch&);
};

/**
 * Ensure that a posix file handle is closed when this object goes out of scope.
 *
 * unlink the file at destructor time
 */
struct TempfileHandleWatch : public HandleWatch
{
    TempfileHandleWatch(const std::string& fname, int fd) : HandleWatch(fname, fd) {}
    ~TempfileHandleWatch();

private:
    // Disallow copy
    TempfileHandleWatch(const HandleWatch&);
    TempfileHandleWatch& operator=(const HandleWatch&);
};

/**
 * Write all the buffer to the file descriptor, retrying after partial writes,
 * blocking as needed.
 */
void write_all(int fd, const void* buf, size_t len);

/// write_all() using a string for the buffer
static inline void write_all(int fd, const std::string& buf) { write_all(fd, buf.data(), buf.size()); }

}
}
}

// vim:set ts=4 sw=4:
#endif
