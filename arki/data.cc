/*
 * data - Data loading and caching
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

#include "data.h"
#include "types/source/unbacked.h"
#include "types/source/blob.h"
#include "utils/datareader.h"
#include <map>

using namespace std;
using namespace wibble;
using namespace arki::types;

namespace arki {

// TODO: @WARNING this is NOT thread safe
arki::utils::DataReader dataReader;

struct PermanentCache : public Data
{
    map<source::Blob, sys::Buffer> cache;

    void add(const types::source::Blob& s, wibble::sys::Buffer buf) override
    {
        cache[s] = buf;
    }

    void drop(const types::source::Blob& s) override
    {
        cache.erase(s);
    }

    wibble::sys::Buffer read(const types::source::Blob& s) override
    {
        map<source::Blob, sys::Buffer>::const_iterator i = cache.find(s);
        if (i != cache.end()) return i->second;

        wibble::sys::Buffer buf(s.size);
        dataReader.read(s.absolutePathname(), s.offset, s.size, buf.data());
        cache[s] = buf;
        return buf;
    }

    bool is_cached(const types::source::Blob& s) const override
    {
        return cache.find(s) != cache.end();
    }
};

struct NeverCache : public Data
{
    void add(const types::source::Blob& s, wibble::sys::Buffer buf) override
    {
    }

    void drop(const types::source::Blob& s) override
    {
    }

    wibble::sys::Buffer read(const types::source::Blob& s) override
    {
        wibble::sys::Buffer buf(s.size);
        dataReader.read(s.absolutePathname(), s.offset, s.size, buf.data());
        return buf;
    }

    bool is_cached(const types::source::Blob& s) const override
    {
        return false;
    }
};

sys::Buffer Data::read(const Source& source)
{
    switch (source.style())
    {
        case Source::BLOB:
            return read(*dynamic_cast<const source::Blob*>(&source));
        case Source::URL:
        case Source::INLINE:
            return dynamic_cast<const source::Unbacked*>(&source)->getCachedData();
        default:
            throw wibble::exception::Consistency("reading data", "reading data from source style " + Source::formatStyle(source.style()) + " is not supported");
    }
}

Data& Data::current()
{
    static Data* m_current = 0;

    if (m_current == 0)
        m_current = new NeverCache;

    return *m_current;
}

void Data::flushDataReaders()
{
    dataReader.flush();
}


}
