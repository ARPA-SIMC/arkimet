/*
 * data - Read/write functions for data blobs, basic implementation
 *
 * Copyright (C) 2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "arki/data/impl.h"

namespace arki {
namespace data {
namespace impl {

template<typename T>
T* Registry<T>::get(const std::string& fname) const
{
    typename std::map<std::string, T*>::const_iterator i = reg.find(fname);
    if (i == reg.end())
        return 0;
    else
        return i->second;
}

template<typename T>
T* Registry<T>::add(T* val)
{
    reg.insert(make_pair(val->fname, val));
    return val;
}

template<typename T>
void Registry<T>::drop(const std::string& fname)
{
    reg.erase(fname);
}

template<typename T>
Registry<T>& Registry<T>::registry()
{
    static __thread Registry<T>* registry = 0;
    if (registry == 0)
        registry = new Registry<T>;
    return *registry;
}

template class Registry<Reader>;
template class Registry<Writer>;

Reader::~Reader() {}
Writer::~Writer() {}

}
}
}
