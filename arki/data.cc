/*
 * data - Read/write functions for data blobs
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

#include "arki/data.h"
#include "arki/data/impl.h"
#include "arki/data/concat.h"
#include <wibble/exception.h>

namespace arki {
namespace data {

Reader::Reader(impl::Reader* impl) : Base<impl::Reader>(impl) {}
Reader::~Reader() {}

Reader Reader::get(const std::string& format, const std::string& fname)
{
}

Writer::Writer(impl::Writer* impl) : Base<impl::Writer>(impl) {}
Writer::~Writer() {}

void Writer::append(Metadata& md)
{
    impl->append(md);
}

Writer Writer::get(const std::string& format, const std::string& fname)
{
    // Cached instance
    impl::Registry<impl::Writer>& reg = impl::Registry<impl::Writer>::registry();

    // Try to reuse an existing instance
    impl::Writer* w = reg.get(fname);
    if (w)
        return Writer(w);

    // Else we need to create an appropriate one
    if (format == "grib")
    {
        return Writer(reg.add(new concat::Writer(fname)));
    } else if (format == "bufr") {
        return Writer(reg.add(new concat::Writer(fname)));
    } else if (format == "vm2") {
        // TODO: wrong one, but will be the same as the old wrong behaviour until we have the right one
        return Writer(reg.add(new concat::Writer(fname)));
    } else {
        throw wibble::exception::Consistency(
                "getting writer for " + format + " file " + fname,
                "format not supported");
    }
}

}
}
