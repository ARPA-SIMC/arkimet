#ifndef ARKI_CORE_H
#define ARKI_CORE_H

/*
 * core - Arkimet core generic interfaces
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
#include <iosfwd>
#include <string>

namespace arki {

/// Named output stream
struct Output
{
    virtual ~Output() {}

    /// Output stream to write to
    virtual std::ostream& stream() = 0;

    /**
     * Name for the stream (file name or description) to use for error
     * messages
     */
    virtual const std::string& name() const = 0;
};

}
#endif
