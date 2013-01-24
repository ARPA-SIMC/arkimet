/*
 * dataset/simple/datafile - Handle a data file plus its associated files
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/simple/datafile.h>
#include <arki/metadata.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/scan/any.h>
#include <arki/nag.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

namespace datafile {

MdBuf::MdBuf(const std::string& pathname)
    : pathname(pathname), dirname(str::dirname(pathname)), basename(str::basename(pathname)), flushed(true)
{
    if (sys::fs::exists(pathname))
    {
        // Read the metadata
        scan::scan(pathname, mds);

        // Read the summary
        if (!mds.empty())
        {
            string sumfname = pathname + ".summary";
            if (utils::files::timestamp(pathname) <= utils::files::timestamp(sumfname))
                sum.readFile(sumfname);
            else
            {
                metadata::Summarise s(sum);
                mds.sendTo(s);
            }
        }
    }
}

MdBuf::~MdBuf()
{
    flush();
}

void MdBuf::add(const Metadata& md)
{
    using namespace arki::types;

    mds.push_back(md);

    // Replace the pathname with its basename
    Item<source::Blob> os = md.source.upcast<source::Blob>();
    mds.back().source = types::source::Blob::create(os->format, dirname, basename, os->offset, os->size);

    sum.add(md);
    flushed = false;
}

void MdBuf::flush()
{
    if (flushed) return;
    mds.writeAtomically(pathname + ".metadata");
    sum.writeAtomically(pathname + ".summary");
}

}

Datafile::Datafile(const std::string& pathname, const std::string& format)
    : writer(data::Writer::get(format, pathname)), mdbuf(pathname)
{
}

Datafile::~Datafile()
{
}

void Datafile::flush()
{
    mdbuf.flush();
}

void Datafile::append(Metadata& md)
{
    writer.append(md);
    mdbuf.add(md);
}

}
}
}
// vim:set ts=4 sw=4:
