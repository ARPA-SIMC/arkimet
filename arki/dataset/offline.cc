#include "offline.h"
#include "arki/utils/string.h"
#include "arki/types/time.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

OfflineReader::OfflineReader(const std::string& fname)
    : Reader(str::basename(fname)), fname(fname)
{
    sum.readFile(fname);
}

void OfflineReader::query_data(const dataset::DataQuery& q, metadata_dest_func)
{
    // TODO: if the matcher would match the summary, output some kind of note about it
}
void OfflineReader::query_summary(const Matcher& matcher, Summary& summary)
{
    sum.filter(matcher, summary);
}

void OfflineReader::expand_date_range(unique_ptr<types::Time>& begin, unique_ptr<types::Time>& end)
{
    sum.expand_date_range(begin, end);
}

}
}
