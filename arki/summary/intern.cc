/*
 * summary/intern - Intern table to map types to unique pointers
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

#include "intern.h"

using namespace std;
using namespace arki::types;

namespace arki {
namespace summary {

TypeIntern::TypeIntern()
{
}

TypeIntern::~TypeIntern()
{
}

const types::Type* TypeIntern::lookup(const Type& item) const
{
    return known_items.find(item);
}

const Type* TypeIntern::intern(const Type& item)
{
    const Type* res = known_items.find(item);
    if (res) return res;
    return known_items.insert(item);
}

const types::Type* TypeIntern::intern(std::auto_ptr<types::Type> item)
{
    const Type* res = known_items.find(*item);
    if (res) return res;
    return known_items.insert(item);
}

}
}
