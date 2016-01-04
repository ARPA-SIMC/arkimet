#include "config.h"
#include "reader.h"
#include "arki/dataset/index/manifest.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

Reader::Reader(const ConfigFile& cfg)
    : IndexedReader(cfg)
{
    // Create the directory if it does not exist
    sys::makedirs(m_path);

    if (index::Manifest::exists(m_path))
    {
        unique_ptr<index::Manifest> mft = index::Manifest::create(m_path);
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

void Reader::expand_date_range(unique_ptr<types::Time>& begin, unique_ptr<types::Time>& end)
{
    m_mft->expand_date_range(begin, end);
}

}
}
}
