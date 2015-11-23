#ifndef ARKI_BBOX_H
#define ARKI_BBOX_H

/*
 * arki/bbox - Compute bounding boxes
 *
 * Copyright (C) 2009--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types/area.h>
#include <arki/utils/geosfwd.h>
#include <arki/utils/lua.h>
#include <string>
#include <memory>

namespace arki {

class BBox
{
protected:
    Lua *L;
    ARKI_GEOS_GEOMETRYFACTORY* gf;
    unsigned funcCount;

public:
    BBox(const std::string& code = std::string());
    virtual ~BBox();

    /**
     * Compute the bounding box for an area.
     *
     * The lua code must produce a table called 'bbox' that contains a
     * vector of (lat, lon) pairs.
     *
     * @return the Geometry object with the bounding box, or 0 if the
     * computation is unsupported for this area.
     */
    virtual std::unique_ptr<ARKI_GEOS_GEOMETRY> operator()(const types::Area& v) const;
};

}

// vim:set ts=4 sw=4:
#endif
