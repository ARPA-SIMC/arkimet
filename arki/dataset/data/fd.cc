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

#include "fd.h"
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
namespace dataset {
namespace data {
namespace fd {

Writer::Writer(const std::string& relname, const std::string& absname, bool truncate)
    : data::Writer(relname, absname), fd(-1)
{
    // Open the data file
    if (truncate)
        fd = ::open(absname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    else
        fd = ::open(absname.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (fd == -1)
        throw wibble::exception::File(absname, "opening file for appending data");
}

Writer::~Writer()
{
    //if (fdatasync(fd_dst) != 0) throw wibble::exception::File(dst, "flushing data to file");
    if (fd != -1) close(fd);
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
        throw wibble::exception::System("locking the file " + absname + " for writing");
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
        throw wibble::exception::File(absname, "reading the current position");
    return size;
}

void Writer::truncate(off_t pos)
{
    if (ftruncate(fd, pos) == -1)
        nag::warning("truncating %s to previous size %zd (rollback of append operation): %m", absname.c_str(), pos);
}

void Writer::write(const wibble::sys::Buffer& buf)
{
    // Prevent caching (ignore function result)
    //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

    // Append the data
    ssize_t res = ::write(fd, buf.data(), buf.size());
    if (res < 0 || (unsigned)res != buf.size())
        throw wibble::exception::File(absname, "writing " + str::fmt(buf.size()) + " bytes");

    if (fdatasync(fd) < 0)
        throw wibble::exception::File(absname, "flushing write");
}

}
}
}
}
