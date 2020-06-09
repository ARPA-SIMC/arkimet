#include "config.h"
#include "reader.h"
#include "index.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {

Reader::Reader(std::shared_ptr<ondisk2::Dataset> dataset)
    : DatasetAccess(dataset)
{
    if (sys::access(str::joinpath(dataset->path, "index.sqlite"), F_OK))
    {
        unique_ptr<index::RIndex> idx(new index::RIndex(m_dataset));
        idx->open();
        m_idx = idx.release();
    }
}

Reader::~Reader()
{
}

std::string Reader::type() const { return "ondisk2"; }

core::Interval Reader::get_stored_time_interval()
{
    throw std::runtime_error("ondisk2::Reader::get_stored_time_interval not yet implemented");
}

}
}
}
