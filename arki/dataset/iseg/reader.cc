#include "config.h"
#include "reader.h"
#include "arki/dataset/index/manifest.h"
#include "arki/utils/sys.h"

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

void Reader::expand_date_range(unique_ptr<core::Time>& begin, unique_ptr<core::Time>& end)
{
    //m_mft->expand_date_range(begin, end);
}

}
}
}
