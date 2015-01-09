/*
 * dataset/simple/datafile - Handle a data file plus its associated files
 *
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/source/blob.h>
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
            if (sys::fs::timestamp(pathname, 0) <= sys::fs::timestamp(sumfname, 0))
                sum.readFile(sumfname);
            else
                mds.add_to_summary(sum);
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

    const source::Blob& os = md.sourceBlob();

    // Replace the pathname with its basename
    auto_ptr<Metadata> copy(md.clone());
    copy->set_source(Source::createBlob(os.format, dirname, basename, os.offset, os.size));
    sum.add(*copy);
    mds.eat(copy);
    flushed = false;
}

void MdBuf::flush()
{
    if (flushed) return;
    mds.writeAtomically(pathname + ".metadata");
    sum.writeAtomically(pathname + ".summary");
}

}

}
}
}
// vim:set ts=4 sw=4:
