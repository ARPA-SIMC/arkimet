#include "offline.h"
#include "arki/utils/string.h"
#include "arki/core/time.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace offline {

Dataset::Dataset(std::shared_ptr<Session> session, const std::string& pathname)
    : dataset::Dataset(session), summary_pathname(pathname + ".summary")
{
}


Reader::Reader(std::shared_ptr<Dataset> dataset)
    : DatasetAccess(dataset)
{
    sum.readFile(dataset->summary_pathname);
}

std::string Reader::type() const { return "offline"; }

bool Reader::impl_query_data(const dataset::DataQuery& q, metadata_dest_func)
{
    // TODO: if the matcher would match the summary, output some kind of note about it
    return true;
}
void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    sum.filter(matcher, summary);
}

core::Interval Reader::get_stored_time_interval()
{
    if (sum.empty())
        return core::Interval();
    return sum.get_reference_time();
}

}
}
}
