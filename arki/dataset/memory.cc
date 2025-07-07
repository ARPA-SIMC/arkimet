#include "memory.h"
#include "arki/core/time.h"
#include "arki/query.h"
#include "arki/summary.h"

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
    return std::make_shared<Reader>(
        static_pointer_cast<Dataset>(shared_from_this()));
}

Reader::~Reader() {}

std::string Reader::type() const { return "memory"; }

bool Reader::impl_query_data(const query::Data& q, metadata_dest_func dest)
{
    if (q.sorter)
        m_dataset->sort(*q.sorter);

    for (const auto& md : *m_dataset)
        if (q.matcher(*md))
            if (!dest(md))
                return false;

    return true;
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    for (const auto& md : *m_dataset)
        if (matcher(*md))
            summary.add(*md);
}

core::Interval Reader::get_stored_time_interval()
{
    throw std::runtime_error(
        "memory::Reader::get_stored_time_interval not yet implemented");
}

} // namespace memory
} // namespace dataset
} // namespace arki
