#include "reader.h"
#include "arki/dataset/index/manifest.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

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
}

std::string Reader::type() const { return "simple"; }

bool Reader::is_dataset(const std::string& dir)
{
    return index::Manifest::exists(dir);
}

void Reader::expand_date_range(core::Interval& interval)
{
    m_mft->expand_date_range(interval);
}

}
}
}
