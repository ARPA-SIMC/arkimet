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
    for (container::iterator i = known_items.begin(); i != known_items.end(); ++i)
        delete *i;
}

const types::Type* TypeIntern::lookup(const Type& item) const
{
    container::const_iterator i = known_items.find(&item);
    if (i != known_items.end()) return *i;
    return 0;
}

const Type* TypeIntern::intern(const Type& item)
{
    container::const_iterator i = known_items.find(&item);
    if (i != known_items.end()) return *i;
    pair<container::const_iterator, bool> res = known_items.insert(item.clone());
    return *res.first;
}

const types::Type* TypeIntern::intern(std::auto_ptr<types::Type> item)
{
    container::const_iterator i = known_items.find(item.get());
    if (i != known_items.end()) return *i;
    pair<container::const_iterator, bool> res = known_items.insert(item.release());
    return *res.first;
}

}
}
