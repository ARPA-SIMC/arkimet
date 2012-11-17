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

#include "arki/data/concat.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include <wibble/exception.h>
#include <wibble/sys/buffer.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace data {
namespace fd {

Writer::Writer(const std::string& fname)
    : impl::Writer(fname), fd(-1)
{
    // Open the data file
    fd = ::open(fname.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (fd == -1)
        throw wibble::exception::File(fname, "opening file for appending data");
}

void Writer::lock()
{
    struct flock lock;
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    // Use SETLKW, so that if it is already locked, we just wait
    if (fcntl(fd, F_SETLKW, &lock) == -1)
        throw wibble::exception::System("locking the file " + fname + " for writing");
}

void Writer::unlock()
{
    struct flock lock;
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    fcntl(fd, F_SETLK, &lock);
}

off_t Writer::wrpos()
{
    // Get the write position in the data file
    off_t size = lseek(fd, 0, SEEK_END);
    if (size == (off_t)-1)
        throw wibble::exception::File(fname, "reading the current position");
    return size;
}

void Writer::truncate(off_t pos)
{
    if (ftruncate(fd, pos) == -1)
        nag::warning("truncating %s to previous size %zd (rollback of append operation): %m", fname.c_str(), pos);
}

void Writer::write(const wibble::sys::Buffer& buf)
{
    // Prevent caching (ignore function result)
    //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

    // Append the data
    ssize_t res = ::write(fd, buf.data(), buf.size());
    if (res < 0 || (unsigned)res != buf.size())
        throw wibble::exception::File(fname, "writing " + str::fmt(buf.size()) + " bytes to " + fname);

    if (fdatasync(fd) < 0)
        throw wibble::exception::File(fname, "flushing write to " + fname);
}

}
}
}
