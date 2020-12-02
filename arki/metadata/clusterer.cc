#include "clusterer.h"
#include "data.h"
#include "arki/metadata.h"
#include "arki/types/source.h"
#include "arki/types/reftime.h"
#include <cstring>

using namespace std;
using namespace arki;
using namespace arki::types;
using arki::core::Time;

namespace arki {
namespace metadata {

Clusterer::Clusterer()
    : last_timerange(0)
{
    cur_interval[0] = -1;
}

Clusterer::~Clusterer()
{
    delete last_timerange;
    // Cannot flush here, since flush is virtual and we won't give subclassers
    // a chance to do their own flushing. Flushes must be explicit.
//    if (!format.empty())
//        flush();
}

bool Clusterer::exceeds_count(const Metadata& md) const
{
    return (max_count != 0 && count >= max_count);
}

bool Clusterer::exceeds_size(size_t data_size) const
{
    if (max_bytes == 0 || size == 0) return false;
    return size + data_size > max_bytes;
}

bool Clusterer::exceeds_interval(const Metadata& md) const
{
    if (max_interval == 0) return false;
    if (cur_interval[0] == -1) return false;
    int candidate[6];
    md_to_interval(md, candidate);
    return memcmp(cur_interval, candidate, 6*sizeof(int)) != 0;
}

bool Clusterer::exceeds_timerange(const Metadata& md) const
{
    if (not split_timerange) return false;
    if (not last_timerange) return false;
    if (*last_timerange == *md.get<types::Timerange>()) return false;
    return true;
}

void Clusterer::start_batch(const std::string& new_format)
{
    format = new_format;
    count = 0;
    size = 0;
}

void Clusterer::add_to_batch(std::shared_ptr<Metadata> md)
{
    size += md->data_size();
    ++count;
    if (cur_interval[0] == -1 && max_interval != 0)
        md_to_interval(*md, cur_interval);
    const Reftime* rt = md->get<types::Reftime>();
    if (!rt) return;
    rt->expand_date_range(timespan);
    if (split_timerange and not last_timerange)
        last_timerange = md->get<types::Timerange>()->clone();
}

void Clusterer::flush_batch()
{
    // Reset the information about the current cluster
    format.clear();
    count = 0;
    size = 0;
    cur_interval[0] = -1;
    timespan = core::Interval();
    if (split_timerange)
    {
        delete last_timerange;
        last_timerange = 0;
    }
}

void Clusterer::flush()
{
    if (!format.empty())
        flush_batch();
}

bool Clusterer::eat(std::shared_ptr<Metadata> md)
{
    const auto& data = md->get_data();

    if (format.empty() || format != md->source().format ||
        exceeds_count(*md) || exceeds_size(data.size()) || exceeds_interval(*md) || exceeds_timerange(*md))
    {
        flush();
        start_batch(md->source().format);
    }

    add_to_batch(md);

    return true;
}

void Clusterer::md_to_interval(const Metadata& md, int* interval) const
{
    const Reftime* rt = md.get<Reftime>();
    if (!rt) throw runtime_error("cannot compute time interval: metadata has no reference time");
    Time t = rt->get_Position();
    interval[0] = max_interval > 0 ? t.ye : -1;
    interval[1] = max_interval > 1 ? t.mo : -1;
    interval[2] = max_interval > 2 ? t.da : -1;
    interval[3] = max_interval > 3 ? t.ho : -1;
    interval[4] = max_interval > 4 ? t.mi : -1;
    interval[5] = max_interval > 5 ? t.se : -1;
}


}
}
