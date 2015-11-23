#ifndef ARKI_TYPES_TYPEVECTOR_H
#define ARKI_TYPES_TYPEVECTOR_H

/*
 * types/typevector - Vector of nullable types
 *
 * Copyright (C) 2014--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <vector>

namespace arki {
namespace types {

/**
 * Vector of owned Type pointers.
 *
 * FIXME: it would be nice if const_iterator gave pointers to const Type*, but
 * it seems overkill to implement it now.
 */
class TypeVector
{
protected:
    std::vector<types::Type*> vals;

public:
    typedef std::vector<types::Type*>::const_iterator const_iterator;

    TypeVector();
    TypeVector(const TypeVector& o);
    ~TypeVector();

    const_iterator begin() const { return vals.begin(); }
    const_iterator end() const { return vals.end(); }
    size_t size() const { return vals.size(); }
    bool empty() const { return vals.empty(); }

    bool operator==(const TypeVector&) const;
    bool operator!=(const TypeVector& o) const { return !operator==(o); }

    types::Type* operator[](unsigned i) { return vals[i]; }
    const types::Type* operator[](unsigned i) const { return vals[i]; }

    void resize(size_t new_size);

    /// Set a value, expanding the vector if needed
    void set(size_t pos, std::unique_ptr<types::Type> val);

    /// Set a value, expanding the vector if needed
    void set(size_t pos, const types::Type* val);

    /// Append an item to the vector
    void push_back(std::unique_ptr<types::Type>&& val);

    /// Append an item to the vector
    void push_back(const types::Type& val);

    /// Set a value to 0, or do nothing if pos > size()
    void unset(size_t pos);

    /// Remove trailing undefined items
    void rtrim();

    /**
     * Split this vector in two at \a pos.
     *
     * This vector will be shortened to be exactly of size \a pos.
     *
     * Returns a vector with the rest of the items
     */
    void split(size_t pos, TypeVector& dest);

    /// Return the raw pointer to the items
    const types::Type* const* raw_items() const { return vals.data(); }

    /// Assuming that the vector is sorted, finds type using a binary search
    const_iterator sorted_find(const types::Type& type) const;

    /**
     * Assuming that the vector is sorted, insert type preserving the sort
     * order. If the item already exists, it returns false. Else, it performs
     * the insert and returns true.
     */
    bool sorted_insert(const types::Type& type);

    /**
     * Same as sorted_insert(const Type&) but it uses type instead of cloning
     * when inserting.
     */
    bool sorted_insert(std::unique_ptr<types::Type>&& type);

protected:
    TypeVector& operator=(const TypeVector&);
};

}
}
#endif
