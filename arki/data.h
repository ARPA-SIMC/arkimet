#ifndef ARKI_DATA_H
#define ARKI_DATA_H

/*
 * data - Data loading and caching
 *
 * Copyright (C) 2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <wibble/sys/buffer.h>

namespace arki {

namespace types {
namespace source {
class Blob;
}
}

struct Data
{
    virtual ~Data() {}

    virtual void add(const types::source::Blob&, wibble::sys::Buffer buf) = 0;
    virtual void drop(const types::source::Blob&) = 0;
    virtual wibble::sys::Buffer read(const types::source::Blob&) = 0;

    /// Get the Data instance that is currently active
    static Data& current();
};

}

#endif
