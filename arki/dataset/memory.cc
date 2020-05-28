#include "memory.h"
#include "arki/summary.h"
#include "arki/metadata/sort.h"
#include "arki/dataset/query.h"

using namespace std;

namespace arki {
namespace dataset {
namespace memory {

Dataset::Dataset(std::shared_ptr<Session> session)
    : dataset::Dataset(session, "memory")
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<Reader>(static_pointer_cast<Dataset>(shared_from_this()));
}


Reader::~Reader() {}

std::string Reader::type() const { return "memory"; }

bool Reader::impl_query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    if (q.sorter)
        m_dataset->sort(*q.sorter);

    for (const auto& md: *m_dataset)
        if (q.matcher(*md))
            if (!dest(md))
                return false;

    return true;
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    for (const auto& md: *m_dataset)
        if (matcher(*md))
            summary.add(*md);
}


}
}
}
