#include "config.h"
#include "reader.h"
#include "index.h"
#include "arki/dataset/step.h"
#include "arki/utils/sys.h"
#include <algorithm>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace iseg {

Reader::Reader(std::shared_ptr<const iseg::Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

Reader::~Reader()
{
}

std::string Reader::type() const { return "iseg"; }

bool Reader::is_dataset(const std::string& dir)
{
    return true;
}

void Reader::list_segments(const Matcher& matcher, std::function<void(const std::string& relpath)> dest)
{
    vector<string> seg_relpaths;
    config().step().list_segments(config().path, config().format, matcher, [&](std::string&& s) { seg_relpaths.emplace_back(move(s)); });
    std::sort(seg_relpaths.begin(), seg_relpaths.end());
    for (const auto& relpath: seg_relpaths)
        dest(relpath);
}

void Reader::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    segmented::Reader::query_data(q, dest);

    list_segments(q.matcher, [&](const std::string& relpath) {
        RIndex idx(m_config, relpath);
        idx.query_data(q, dest);
    });
}

void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    // Query the archives first
    segmented::Reader::query_summary(matcher, summary);

    // TODO: use summary cache if available
    list_segments(matcher, [&](const std::string& relpath) {
        RIndex idx(m_config, relpath);
        idx.query_summary_from_db(matcher, summary);
    });
}

void Reader::expand_date_range(unique_ptr<core::Time>& begin, unique_ptr<core::Time>& end)
{
    //m_mft->expand_date_range(begin, end);
}

}
}
}
