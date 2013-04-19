/*
 * data - Read/write functions for data blobs without envelope
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

#include "arki/data/concat.h"
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

namespace arki {
namespace data {
namespace concat {

namespace {

struct Append : public Transaction
{
    Writer& w;
    Metadata& md;
    bool fired;
    wibble::sys::Buffer buf;
    off64_t pos;

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
        md.source = types::source::Blob::create(md.source->format, "", w.fname, pos, buf.size());

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



Writer::Writer(const std::string& fname)
    : fd::Writer(fname)
{
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
    md.source = types::source::Blob::create(md.source->format, "", fname, pos, buf.size());
}

off_t Writer::append(const wibble::sys::Buffer& buf)
{
    off_t pos = wrpos();
    write(buf);
    return pos;
}

Pending Writer::append(Metadata& md, off64_t* ofs)
{
    Append* res = new Append(*this, md);
    *ofs = res->pos;
    return res;
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
