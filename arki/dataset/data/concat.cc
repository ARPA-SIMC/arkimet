/*
 * data - Read/write functions for data blobs without envelope
 *
 * Copyright (C) 2012--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "concat.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/utils/files.h"
#include <wibble/exception.h>
#include <wibble/sys/buffer.h>
#include <wibble/sys/signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace wibble;
using namespace arki::types;

namespace arki {
namespace dataset {
namespace data {
namespace concat {

namespace {

struct Append : public Transaction
{
    Segment& w;
    Metadata& md;
    bool fired;
    wibble::sys::Buffer buf;
    off_t pos;

    Append(Segment& w, Metadata& md) : w(w), md(md), fired(false)
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
        md.set_source(Source::createBlob(md.source().format, "", w.absname, pos, buf.size()));

        fired = true;
    }

    virtual void rollback()
    {
        if (fired) return;

        // If we had a problem, attempt to truncate the file to the original size
        w.fdtruncate(pos);

        w.unlock();
        fired = true;
    }
};

}



Segment::Segment(const std::string& relname, const std::string& absname)
    : fd::Segment(relname, absname)
{
}

void Segment::append(Metadata& md)
{
    open();

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
        fdtruncate(pos);

        unlock();

        throw;
    }

    unlock();

    // Set the source information that we are writing in the metadata
    md.set_source(Source::createBlob(md.source().format, "", absname, pos, buf.size()));
}

off_t Segment::append(const wibble::sys::Buffer& buf)
{
    open();

    off_t pos = wrpos();
    write(buf);
    return pos;
}

Pending Segment::append(Metadata& md, off_t* ofs)
{
    open();

    Append* res = new Append(*this, md);
    *ofs = res->pos;
    return res;
}

void HoleSegment::write(const wibble::sys::Buffer& buf)
{
    // Get the current file size
    off_t pos = ::lseek(fd, 0, SEEK_END);
    if (pos == (off_t)-1)
        throw wibble::exception::File(absname, "getting file size");

    // Enlarge its apparent size to include the size of buf
    int res = ftruncate(fd, pos + buf.size());
    if (res < 0)
        throw wibble::exception::File(absname, "adding " + str::fmt(buf.size()) + " bytes to the apparent file size");
}

FileState Segment::check(const metadata::Collection& mds, bool quick)
{
    return fd::Segment::check(mds, 0, quick);
}

static data::Segment* make_repack_segment(const std::string& relname, const std::string& absname)
{
    unique_ptr<concat::Segment> res(new concat::Segment(relname, absname));
    res->truncate_and_open();
    return res.release();
}
Pending Segment::repack(const std::string& rootdir, metadata::Collection& mds)
{
    return fd::Segment::repack(rootdir, relname, mds, make_repack_segment);
}

static data::Segment* make_repack_hole_segment(const std::string& relname, const std::string& absname)
{
    unique_ptr<concat::Segment> res(new concat::HoleSegment(relname, absname));
    res->truncate_and_open();
    return res.release();
}
Pending HoleSegment::repack(const std::string& rootdir, metadata::Collection& mds)
{
    close();
    return fd::Segment::repack(rootdir, relname, mds, make_repack_hole_segment, true);
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
    out.flush();
    return buf.size();
}

size_t OstreamWriter::stream(Metadata& md, int out) const
{
    wibble::sys::Buffer buf = md.getData();
    sys::sig::ProcMask pm(blocked);

    ssize_t res = ::write(out, buf.data(), buf.size());
    if (res < 0 || (unsigned)res != buf.size())
        throw wibble::exception::System("writing " + str::fmt(buf.size()) + " bytes");

    return buf.size();
}

}
}
}
}
