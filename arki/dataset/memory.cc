#include "memory.h"
#include "arki/summary.h"
#include "arki/metadata/sort.h"

using namespace std;

namespace arki {
namespace dataset {

Memory::Memory(std::shared_ptr<Session> session)
    : m_config(session, "memory")
{
}
Memory::~Memory() {}

std::string Memory::type() const { return "memory"; }

bool Memory::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    if (q.sorter)
        sort(*q.sorter);

    for (const auto& md: vals)
        if (q.matcher(*md))
            if (!dest(md))
                return false;

    return true;
}

void Memory::query_summary(const Matcher& matcher, Summary& summary)
{
    for (const_iterator i = begin(); i != end(); ++i)
        if (matcher(**i))
            summary.add(**i);
}


}
}
