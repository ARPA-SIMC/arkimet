#ifndef ARKI_DATASET_GRIDQUERY_H
#define ARKI_DATASET_GRIDQUERY_H

/*
 * dataset/gridquery - Lay out a metadata grid and check that metadata fit 
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

#include <arki/dataset.h>
#include <arki/types.h>
#include <arki/types/time.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/metadatagrid.h>
#include <string>
#include <vector>
#include <set>
#include <map>
#include <iosfwd>

struct lua_State;

namespace arki {

class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {

struct GridQuery
{
protected:
    // Dataset we query
    ReadonlyDataset& ds;
    // Global summary of ds
    Summary summary;
    // Metadata grid of requested items per every reference time
    MetadataGrid mdgrid;
    // Itemsets actually requested (not all elements in the grid are needed)
    std::vector<ItemSet> items;
    // Reference times requested
    std::vector< Item<types::Time> > times;
    // Sorted list of mdgrid indices requested per every reference time
    std::vector<int> wantedidx;
    // Bitmaps corresponding to wantedidx, of seen items per reftime step
    std::vector<bool> todolist;
    // Extra filters to add to the merged query
    std::vector<Matcher> filters;

public:
    GridQuery(ReadonlyDataset& ds);

    /// Add metadata resulting from the matcher expansion
    void add(const Matcher& m);

    /// Add a reftime
    void addTime(const Item<types::Time>& rt);

    // Add a discrete time sequence (@see types::Time::generate())
    void addTimes(const Item<types::Time>& begin, const Item<types::Time>& end, int step);

    /// Add an extra filter expression
    void addFilter(const Matcher& m);

    /**
     * Done with adding requests, initialise structure to filter results
     */
    void consolidate();

    /**
     * Build the merged query matching at least all the items accepted by this
     * GridQuery
     */
    Matcher mergedQuery() const;

    /// Return the number of items in TODO list
    size_t expectedItems() const;

    /// Check if a metadata fits in the result, and mark it as seen
    bool checkAndMark(const ItemSet& md);

    /// Check that all items in todolist are true
    bool satisfied() const;

    /// Dump the GridQuery state to the given stream
    void dump(std::ostream& out) const;

    // LUA functions
    /// Push to the LUA stack a userdata to access this Origin
    void lua_push(lua_State* L);

    /**
     * Check that the element at \a idx is a GridQuery userdata
     *
     * @return the GridQuery element, or 0 if the check failed
     */
    static GridQuery* lua_check(lua_State* L, int idx);

    /**
     * Load summary functions into a lua VM
     */
    static void lua_openlib(lua_State* L);
};

}
}

// vim:set ts=4 sw=4:
#endif
