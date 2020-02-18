#include "offline.h"
#include "arki/utils/string.h"

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

bool Reader::query_data(const dataset::DataQuery& q, metadata_dest_func)
{
    // TODO: if the matcher would match the summary, output some kind of note about it
    return true;
}
void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    sum.filter(matcher, summary);
}

void Reader::expand_date_range(unique_ptr<core::Time>& begin, unique_ptr<core::Time>& end)
{
    sum.expand_date_range(begin, end);
}

}
}
}
