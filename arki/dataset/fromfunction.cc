#include "fromfunction.h"
#include "arki/query.h"
#include "arki/query/progress.h"
#include "arki/core/time.h"

using namespace std;

namespace arki {
namespace dataset {
namespace fromfunction {

std::shared_ptr<dataset::Reader> Dataset::create_reader() { return std::make_shared<Reader>(shared_from_this()); }


bool Reader::impl_query_data(const query::Data& q, metadata_dest_func dest)
{
    query::TrackProgress track(q.progress);
    dest = track.wrap(dest);
    return track.done(generator([&](std::shared_ptr<Metadata> md) {
        if (!q.matcher(*md))
            return true;
        return dest(md);
    }));
}

core::Interval Reader::get_stored_time_interval()
{
    throw std::runtime_error("fromfunction::Reader::get_stored_time_interval not yet implemented");
}

}
}
}
