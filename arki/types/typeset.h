#ifndef ARKI_TYPES_TYPESET_H
#define ARKI_TYPES_TYPESET_H

/*
 * types/typeset - set of types
 *
 * copyright (c) 2015  arpa-sim <urpsim@smr.arpa.emr.it>
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
#include <set>

namespace arki {
namespace types {

/**
 * Vector of owned Type pointers.
 *
 * FIXME: it would be nice if const_iterator gave pointers to const Type*, but
 * it seems overkill to implement it now.
 */
class TypeSet
{
protected:
    struct TypeptrLt
    {
        inline bool operator()(const types::Type* a, const types::Type* b) const { return *a < *b; }
    };
    typedef std::set<const types::Type*, TypeptrLt> container;
    container vals;

public:
    typedef container::const_iterator const_iterator;

    TypeSet();
    TypeSet(const TypeSet& o);
    ~TypeSet();

    const_iterator begin() const { return vals.begin(); }
    const_iterator end() const { return vals.end(); }
    size_t size() const { return vals.size(); }
    bool empty() const { return vals.empty(); }

    /**
     * Insert val, return the pointer to the item stored in the set (either a
     * copy of val, or the previously stored item)
     */
    const Type* insert(const types::Type& val);

    /**
     * Insert val, return the pointer to the item stored in the set (either
     * val, or the previously stored item)
     */
    const Type* insert(std::auto_ptr<types::Type> val);

    /**
     * Look for an element in the set.
     *
     * If found, returns the pointer to the item stored in the set.
     *
     * If not found, returns 0.
     */
    const types::Type* find(const types::Type& val) const;

    /**
     * Remove an item, if present.
     *
     * Returns true if it was erased, false if it was not present.
     */
    bool erase(const types::Type& val);

    bool operator==(const TypeSet&) const;
    bool operator!=(const TypeSet& o) const { return !operator==(o); }

protected:
    TypeSet& operator=(const TypeSet&);
};

}
}
#endif
