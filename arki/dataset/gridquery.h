#ifndef ARKI_DATASET_GRIDQUERY_H
#define ARKI_DATASET_GRIDQUERY_H

#include <arki/dataset.h>
#include <arki/types.h>
#include <arki/core/time.h>
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
    Reader& ds;
    // Global summary of ds
    Summary summary;
    // Metadata grid of requested items per every reference time
    MetadataGrid mdgrid;
    // Itemsets actually requested (not all elements in the grid are needed)
    std::vector<ItemSet> items;
    // Reference times requested
    std::vector<core::Time> times;
    // Sorted list of mdgrid indices requested per every reference time
    std::vector<int> wantedidx;
    /**
     * Bitmaps corresponding to wantedidx*times, marking items that have been
     * seen at each requested time
     */
    std::vector<bool> todolist;
    // Extra filters to add to the merged query
    std::vector<Matcher> filters;

public:
    GridQuery(Reader& ds);

    /// Add metadata resulting from the matcher expansion
    void add(const Matcher& m);

    /// Add a reftime
    void addTime(const core::Time& rt);

    // Add a discrete time sequence (@see Time::generate())
    void addTimes(const core::Time& begin, const core::Time& end, int step);

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
