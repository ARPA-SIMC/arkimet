#ifndef ARKI_SUMMARY_INTERN_H
#define ARKI_SUMMARY_INTERN_H

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

#include <arki/types.h>
#include <arki/types/typeset.h>
#include <set>

namespace arki {
namespace summary {

/**
 * Maintain a set of unique pointers for Type items.
 *
 * It allows to reuse owner pointers and create structures where equality
 * between items can be tested just by comparing pointers.
 */
class TypeIntern
{
protected:
    types::TypeSet known_items;

public:
    TypeIntern();
    ~TypeIntern();

    typedef types::TypeSet::const_iterator const_iterator;

    const_iterator begin() const { return known_items.begin(); }
    const_iterator end() const { return known_items.end(); }

    /// Lookup an item, returning 0 if we have never seen it
    const types::Type* lookup(const types::Type& item) const;

    /**
     * Lookup an item, adding a clone of it to the collection if we have never
     * seen it
     */
    const types::Type* intern(const types::Type& item);

    /**
     * Same as intern(const Type&) but if the item is missing, it reuses this
     * one instead of cloning a new one
     */
    const types::Type* intern(std::auto_ptr<types::Type> item);

private:
    TypeIntern(const TypeIntern&);
    TypeIntern& operator=(const TypeIntern&);
};

}
}

#endif
