#include "config.h"
#include "metadatagrid.h"
#include "metadata.h"
#include "utils/string.h"
// #include <iostream>

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {

namespace {
struct TypeptrLt
{
    inline bool operator()(const types::Type* a, const types::Type* b) const { return *a < *b; }
};
}

MetadataGrid::MetadataGrid() {}

int MetadataGrid::index(const ItemSet& md) const
{
    int res = 0;
    size_t dim = 0;
    for (std::map<Code, TypeVector>::const_iterator i = dims.begin();
            i != dims.end(); ++i, ++dim)
    {
        const Type* mdi = md.get(i->first);
        if (!mdi) return -1;
        TypeVector::const_iterator j = i->second.sorted_find(*mdi);
        if (j == i->second.end()) return -1;
        int idx = j - i->second.begin();
        res += idx * dim_sizes[dim];
    }
    return res;
}

TypeVector MetadataGrid::expand(size_t index) const
{
    TypeVector res;
    size_t dim = 0;
    for (std::map<Code, TypeVector>::const_iterator i = dims.begin();
            i != dims.end(); ++i, ++dim)
    {
        size_t idx = index / dim_sizes[dim];
        res.push_back(*i->second[idx]);
        index = index % dim_sizes[dim];
    }
    return res;
}

std::string MetadataGrid::make_query() const
{
    set<types::Code> dimensions;

    // Collect the codes as dimensions of relevance
    for (std::map<Code, TypeVector>::const_iterator i = dims.begin();
            i != dims.end(); ++i)
        dimensions.insert(i->first);

    vector<string> ands;
    for (set<Code>::const_iterator i = dimensions.begin(); i != dimensions.end(); ++i)
    {
        vector<string> ors;
        std::map<types::Code, TypeVector>::const_iterator si = dims.find(*i);
        if (si != dims.end())
            for (TypeVector::const_iterator j = si->second.begin();
                    j != si->second.end(); ++j)
                ors.push_back((*j)->exactQuery());
        ands.push_back(types::tag(*i) + ":" + str::join(" or ", ors.begin(), ors.end()));
    }
    return str::join("; ", ands.begin(), ands.end());
}

void MetadataGrid::clear()
{
    dims.clear();
    dim_sizes.clear();
}

void MetadataGrid::add(const Type& item)
{
    // Insertion sort; at the end, everything is already sorted and we
    // avoid inserting lots of duplicate items
    TypeVector& v = dims[item.type_code()];
    v.sorted_insert(item);
}

void MetadataGrid::add(const ItemSet& is)
{
    for (ItemSet::const_iterator i = is.begin(); i != is.end(); ++i)
        add(*(i->second));
}

void MetadataGrid::consolidate()
{
    dim_sizes.clear();
    for (std::map<Code, TypeVector>::iterator i = dims.begin();
            i != dims.end(); ++i)
    {
        // Update the number of matrix elements below every dimension
        for (vector<size_t>::iterator j = dim_sizes.begin();
                j != dim_sizes.end(); ++j)
            *j *= i->second.size();
        dim_sizes.push_back(1);
    }
}

}
