/*
 * data - Read/write functions for data blobs with newline separators
 *
 * Copyright (C) 2012--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "lines.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include <wibble/exception.h>
#include <wibble/sys/buffer.h>
#include <wibble/sys/signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {
namespace data {
namespace lines {

namespace {

struct Append : public Transaction
{
    Writer& w;
    Metadata& md;
    bool fired;
    wibble::sys::Buffer buf;
    off_t pos;

    Append(Writer& w, Metadata& md) : w(w), md(md), fired(false)
    {
        // Get the data blob to append
        buf = md.getData();

        // Lock the file so that we are the only ones writing to it
        w.lock();

        // Insertion offset
        pos = w.wrpos();
    }

    virtual ~Append()
    {
        if (!fired) rollback();
    }

    virtual void commit()
    {
        if (fired) return;

        // Append the data
        w.write(buf);

        w.unlock();

        // Set the source information that we are writing in the metadata
        md.source = types::source::Blob::create(md.source->format, "", w.absname, pos, buf.size());

        fired = true;
    }

    virtual void rollback()
    {
        if (fired) return;

        // If we had a problem, attempt to truncate the file to the original size
        w.truncate(pos);

        w.unlock();
        fired = true;
    }
};

}



Writer::Writer(const std::string& relname, const std::string& absname, bool truncate)
    : fd::Writer(relname, absname, truncate)
{
}

void Writer::write(const wibble::sys::Buffer& buf)
{
    struct iovec todo[2] = {
        { (void*)buf.data(), buf.size() },
        { (void*)"\n", 1 },
    };

    // Prevent caching (ignore function result)
    //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

    // Append the data
    ssize_t res = ::writev(fd, todo, 2);
    if (res < 0 || (unsigned)res != buf.size() + 1)
        throw wibble::exception::File(absname, "writing " + str::fmt(buf.size() + 1) + " bytes");

    if (fdatasync(fd) < 0)
        throw wibble::exception::File(absname, "flushing write");
}

void Writer::append(Metadata& md)
{
    // Get the data blob to append
    sys::Buffer buf = md.getData();

    // Lock the file so that we are the only ones writing to it
    lock();

    // Get the write position in the data file
    off_t pos = wrpos();

    try {
        // Append the data
        write(buf);
    } catch (...) {
        // If we had a problem, attempt to truncate the file to the original size
        truncate(pos);

        unlock();

        throw;
    }

    unlock();

    // Set the source information that we are writing in the metadata
    md.source = types::source::Blob::create(md.source->format, "", absname, pos, buf.size());
}

off_t Writer::append(const wibble::sys::Buffer& buf)
{
    off_t pos = wrpos();
    write(buf);
    return pos;
}

Pending Writer::append(Metadata& md, off_t* ofs)
{
    Append* res = new Append(*this, md);
    *ofs = res->pos;
    return res;
}

FileState Writer::check(const std::string& absname, const metadata::Collection& mds, bool quick)
{
    return fd::Writer::check(absname, mds, 2, quick);
}

static data::Writer* make_repack_writer(const std::string& relname, const std::string& absname)
{
    return new lines::Writer(relname, absname, true);
}

Pending Writer::repack(const std::string& rootdir, const std::string& relname, metadata::Collection& mds)
{
    return fd::Writer::repack(rootdir, relname, mds, make_repack_writer);
}

OstreamWriter::OstreamWriter()
{
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGINT);
    sigaddset(&blocked, SIGTERM);
}

OstreamWriter::~OstreamWriter()
{
}

size_t OstreamWriter::stream(Metadata& md, std::ostream& out) const
{
    wibble::sys::Buffer buf = md.getData();
    sys::sig::ProcMask pm(blocked);
    out.write((const char*)buf.data(), buf.size());
    // Cannot use endl since we don't know how long it is, and we would risk
    // returning the wrong number of bytes written
    out << "\n";
    out.flush();
    return buf.size() + 1;
}

size_t OstreamWriter::stream(Metadata& md, int out) const
{
    wibble::sys::Buffer buf = md.getData();
    sys::sig::ProcMask pm(blocked);

    ssize_t res = ::write(out, buf.data(), buf.size());
    if (res < 0 || (unsigned)res != buf.size())
        throw wibble::exception::System("writing " + str::fmt(buf.size()) + " bytes");

    res = ::write(out, "\n", 1);
    if (res < 0 || (unsigned)res != 1)
        throw wibble::exception::System("writing newline");

    return buf.size() + 1;
}


}
}
}
}
