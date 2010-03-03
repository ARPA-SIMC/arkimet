/*
 * metadatagrid - Index values by a combination of metadata
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/metadatagrid.h>
#include <arki/metadata.h>

// #include <iostream>

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {

MetadataGrid::MetadataGrid()
        : maxidx(0) {}

int MetadataGrid::index(const ItemSet& md) const
{
        int res = 0;
        size_t dim = 0;
        for (std::map<types::Code, std::vector< Item<> > >::const_iterator i = dims.begin();
                        i != dims.end(); ++i, ++dim)
        {
                UItem<> mdi = md.get(i->first);
                if (!mdi.defined())
                        return -1;
                vector< Item<> >::const_iterator lb =
                        lower_bound(i->second.begin(), i->second.end(), mdi);
                if (lb == i->second.end())
                        return -1;
                if (*lb != mdi)
                        return -1;
                int idx = lb - i->second.begin();
                res += idx * dim_sizes[dim];
        }
        return res;
}

std::vector< Item<> > MetadataGrid::expand(size_t index) const
{
        vector< Item<> > res;
        size_t dim = 0;
        for (std::map<types::Code, std::vector< Item<> > >::const_iterator i = dims.begin();
                        i != dims.end(); ++i, ++dim)
        {
                size_t idx = index / dim_sizes[dim];
                res.push_back(i->second[idx]);
                index = index % dim_sizes[dim];
        }
        return res;
}

std::string MetadataGrid::make_query() const
{
        set<types::Code> dimensions;

        // Collect the codes as dimensions of relevance
        for (std::map<types::Code, std::vector< Item<> > >::const_iterator i = dims.begin();
                        i != dims.end(); ++i)
                dimensions.insert(i->first);

        vector<string> ands;
        for (set<types::Code>::const_iterator i = dimensions.begin(); i != dimensions.end(); ++i)
        {
                vector<string> ors;
                std::map<types::Code, std::vector< Item<> > >::const_iterator si = dims.find(*i);
                if (si != dims.end())
                        for (std::vector< Item<> >::const_iterator j = si->second.begin();
                                        j != si->second.end(); ++j)
                                ors.push_back((*j)->exactQuery());
                ands.push_back(types::tag(*i) + ":" + str::join(ors.begin(), ors.end(), " or "));
        }
        return str::join(ands.begin(), ands.end(), "; ");
}

void MetadataGrid::clear()
{
        dims.clear();
        dim_sizes.clear();
        maxidx = 0;
}

void MetadataGrid::add(const Item<>& item)
{
        // Insertion sort; at the end, everything is already sorted and we
        // avoid inserting lots of duplicate items
        vector< Item<> >& v = dims[item->serialisationCode()];
        vector< Item<> >::iterator lb = lower_bound(v.begin(), v.end(), item);
        if (lb == v.end())
                v.push_back(item);
        else if (*lb != item)
                v.insert(lb, item);
}

void MetadataGrid::consolidate()
{
        dim_sizes.clear();
        maxidx = dims.empty() ? 0 : 1;
        for (std::map<types::Code, std::vector< Item<> > >::iterator i = dims.begin();
                        i != dims.end(); ++i)
        {
                // Update the number of matrix elements below every dimension
                for (vector<size_t>::iterator j = dim_sizes.begin();
                                j != dim_sizes.end(); ++j)
                        *j *= i->second.size();
                dim_sizes.push_back(1);
                maxidx *= i->second.size();
        }
}

}
