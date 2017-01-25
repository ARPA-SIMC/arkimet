#include "config.h"
#include "reader.h"
#include "arki/dataset/index/manifest.h"
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
    });
#if 0
    if (!m_idx) return;
    // FIXME: this is cargo culted from the old ondisk2 reader: what is the use case for this?
    if (!m_idx->query_data(q, dest))
        throw std::runtime_error("cannot query " + config().path + ": index could not be used");
#endif
}

void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    // Query the archives first
    segmented::Reader::query_summary(matcher, summary);

    list_segments(matcher, [&](const std::string& relpath) {
    });
#if 0
    if (!m_idx) return;
    // FIXME: this is cargo culted from the old ondisk2 reader: what is the use case for this?
    if (!m_idx->query_summary(matcher, summary))
        throw std::runtime_error("cannot query " + config().path + ": index could not be used");
#endif
}

void Reader::expand_date_range(unique_ptr<core::Time>& begin, unique_ptr<core::Time>& end)
{
    //m_mft->expand_date_range(begin, end);
}

}
}
}
