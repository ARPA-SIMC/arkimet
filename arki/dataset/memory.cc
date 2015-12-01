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

void Memory::query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest)
{
    if (q.sorter)
        sort(*q.sorter);

    for (const_iterator i = begin(); i != end(); ++i)
        if (q.matcher(**i))
            if (!dest(Metadata::create_copy(**i)))
                break;
}

void Memory::querySummary(const Matcher& matcher, Summary& summary)
{
    for (const_iterator i = begin(); i != end(); ++i)
        if (matcher(**i))
            summary.add(**i);
}


}
}
