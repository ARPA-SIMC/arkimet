#include "reader.h"
#include "arki/dataset/index.h"
#include "arki/dataset/lock.h"
#include "arki/dataset/query.h"
#include "arki/dataset/progress.h"
#include "arki/dataset/index/manifest.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

bool Reader::impl_query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    dataset::TrackProgress track(q.progress);
    dest = track.wrap(dest);

    auto lock = dataset().read_lock_dataset();
    if (!local::Reader::impl_query_data(q, dest))
        return false;
    if (!m_idx) return true;
    m_idx->lock = lock;
    return track.done(m_idx->query_data(q, dest));
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    auto lock = dataset().read_lock_dataset();
    // Query the archives first
    local::Reader::impl_query_summary(matcher, summary);
    if (!m_idx) return;
    m_idx->lock = lock;
    // FIXME: this is cargo culted from the old ondisk2 reader: what is the use case for this?
    if (!m_idx->query_summary(matcher, summary))
        throw std::runtime_error("cannot query " + dataset().path + ": index could not be used");
}

Reader::Reader(std::shared_ptr<simple::Dataset> dataset)
    : DatasetAccess(dataset)
{
    // Create the directory if it does not exist
    sys::makedirs(dataset->path);

    if (index::Manifest::exists(dataset->path))
    {
        unique_ptr<index::Manifest> mft = index::Manifest::create(m_dataset);
        mft->openRO();
        m_idx = m_mft = mft.release();
    }
}

Reader::~Reader()
{
    delete m_idx;
}

std::string Reader::type() const { return "simple"; }

bool Reader::is_dataset(const std::string& dir)
{
    return index::Manifest::exists(dir);
}

core::Interval Reader::get_stored_time_interval()
{
    return m_mft->get_stored_time_interval();
}

}
}
}
