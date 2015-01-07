/*
 * metadata/collection - In-memory collection of metadata
 *
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include "memory.h"
#include <arki/summary.h>
#include <arki/sort.h>
#include <arki/utils/dataset.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

Memory::Memory() {}
Memory::~Memory() {}

void Memory::queryData(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
    // First ask the index.  If it can do something useful, iterate with it
    //
    // If the index would just do a linear scan of everything, then instead
    // scan the directories in sorted order.
    //
    // For each directory try to match its summary first, and if it matches
    // then produce all the contents.
    metadata::Consumer* c = &consumer;
    auto_ptr<sort::Stream> sorter;
    auto_ptr<ds::TemporaryDataInliner> inliner;

    if (q.withData)
    {
        inliner.reset(new ds::TemporaryDataInliner(*c));
        c = inliner.get();
    }

    if (q.sorter)
    {
        sorter.reset(new sort::Stream(*q.sorter, *c));
        c = sorter.get();
    }

    for (std::vector<Metadata>::iterator i = begin();
            i != end(); ++i)
        if (q.matcher(*i))
            if (!(*c)(*i))
                break;
}

void Memory::querySummary(const Matcher& matcher, Summary& summary)
{
    using namespace wibble::str;

    for (std::vector<Metadata>::iterator i = begin();
            i != end(); ++i)
        if (matcher(*i))
            summary.add(*i);
}


}
}
