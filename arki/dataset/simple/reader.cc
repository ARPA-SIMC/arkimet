#include "config.h"
#include "reader.h"
#include "arki/dataset/index/manifest.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

Reader::Reader(std::shared_ptr<const simple::Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);

    if (index::Manifest::exists(config->path))
    {
        unique_ptr<index::Manifest> mft = index::Manifest::create(config->path);
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

void Reader::expand_date_range(unique_ptr<core::Time>& begin, unique_ptr<core::Time>& end)
{
    m_mft->expand_date_range(begin, end);
}

std::string ShardingReader::type() const { return "simple"; }

}
}
}
