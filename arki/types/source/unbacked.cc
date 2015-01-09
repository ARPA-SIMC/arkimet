/*
 * types/source/unbacked - Base type for sources that cannot load their data
 *                         from external sources
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

#include "unbacked.h"

namespace arki {
namespace types {
namespace source {

bool Unbacked::hasCachedData() const
{
    return m_inline_buf.data() != 0;
}

wibble::sys::Buffer Unbacked::getCachedData() const
{
    return m_inline_buf;
}

void Unbacked::setCachedData(const wibble::sys::Buffer& buf)
{
    m_inline_buf = buf;
}

}
}
}
