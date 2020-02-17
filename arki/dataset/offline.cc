#include "offline.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

OfflineConfig::OfflineConfig(std::shared_ptr<Session> session, const std::string& pathname)
    : dataset::Config(session), summary_pathname(pathname + ".summary")
{
}

std::shared_ptr<const OfflineConfig> OfflineConfig::create(std::shared_ptr<Session> session, const std::string& pathname)
{
    return std::shared_ptr<const OfflineConfig>(new OfflineConfig(session, pathname));
}


OfflineReader::OfflineReader(std::shared_ptr<const OfflineConfig> config)
    : m_config(config)
{
    sum.readFile(config->summary_pathname);
}

std::string OfflineReader::type() const { return "offline"; }

bool OfflineReader::query_data(const dataset::DataQuery& q, metadata_dest_func)
{
    // TODO: if the matcher would match the summary, output some kind of note about it
    return true;
}
void OfflineReader::query_summary(const Matcher& matcher, Summary& summary)
{
    sum.filter(matcher, summary);
}

void OfflineReader::expand_date_range(unique_ptr<core::Time>& begin, unique_ptr<core::Time>& end)
{
    sum.expand_date_range(begin, end);
}

}
}
