#include "memory.h"
#include <arki/summary.h>
#include <arki/sort.h>

using namespace std;

namespace arki {
namespace dataset {

Memory::Memory()
{
    m_config.name = "memory";
}
Memory::~Memory() {}

std::string Memory::type() const { return "memory"; }

void Memory::query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest)
{
    if (q.sorter)
        sort(*q.sorter);

    for (const_iterator i = begin(); i != end(); ++i)
        if (q.matcher(**i))
            if (!dest(Metadata::create_copy(**i)))
                break;
}

void Memory::query_summary(const Matcher& matcher, Summary& summary)
{
    for (const_iterator i = begin(); i != end(); ++i)
        if (matcher(**i))
            summary.add(**i);
}


}
}
