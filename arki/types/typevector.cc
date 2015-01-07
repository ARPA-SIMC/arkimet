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

#include "typevector.h"

using namespace std;
using namespace wibble;
using namespace arki::types;

namespace arki {
namespace types {

TypeVector::TypeVector() {}

TypeVector::TypeVector(const TypeVector& o)
{
    vals.reserve(o.vals.size());
    for (const_iterator i = o.begin(); i != o.end(); ++i)
        vals.push_back(*i ? (*i)->clone() : 0);
}

TypeVector::~TypeVector()
{
    for (vector<Type*>::iterator i = vals.begin(); i != vals.end(); ++i)
        delete *i;
}

bool TypeVector::operator==(const TypeVector& o) const
{
    if (size() != o.size()) return false;
    const_iterator a = begin();
    const_iterator b = o.begin();
    while (a != end() && b != o.end())
        if (!Type::nullable_equals(*a, *b))
            return false;
    return true;
}

void TypeVector::set(size_t pos, std::auto_ptr<types::Type> val)
{
    if (pos >= vals.size())
        vals.resize(pos + 1);
    else if (vals[pos])
        delete vals[pos];
    vals[pos] = val.release();
}

void TypeVector::set(size_t pos, const Type* val)
{
    if (pos >= vals.size())
        vals.resize(pos + 1);
    else if (vals[pos])
        delete vals[pos];

    if (val)
        vals[pos] = val->clone();
    else
        vals[pos] = 0;
}

void TypeVector::unset(size_t pos)
{
    if (pos >= vals.size()) return;
    delete vals[pos];
    vals[pos] = 0;
}

void TypeVector::resize(size_t new_size)
{
    if (new_size < size())
    {
        // If we are shrinking, we need to deallocate the elements that are left
        // out
        for (size_t i = new_size; i < size(); ++i)
            delete vals[i];
    }

    // For everything else, just go with the vector default of padding with
    // zeroes
    vals.resize(new_size);
    return;

}

void TypeVector::rtrim()
{
    while (!vals.empty() && !vals.back())
        vals.pop_back();
}

void TypeVector::split(size_t pos, TypeVector& dest)
{
    dest.vals.reserve(dest.size() + size() - pos);
    for (unsigned i = pos; i < size(); ++i)
        dest.vals.push_back(vals[i]);
    vals.resize(pos);
}

void TypeVector::push_back(std::auto_ptr<types::Type> val)
{
    vals.push_back(val.release());
}

void TypeVector::push_back(const types::Type& val)
{
    vals.push_back(val.clone());
}

}
}
