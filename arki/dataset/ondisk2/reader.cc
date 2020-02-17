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

Reader::Reader(std::shared_ptr<ondisk2::Dataset> config)
    : m_config(config)
{
    if (sys::access(str::joinpath(config->path, "index.sqlite"), F_OK))
    {
        unique_ptr<index::RIndex> idx(new index::RIndex(m_config));
        idx->open();
        m_idx = idx.release();
    }
}

Reader::~Reader()
{
}

std::string Reader::type() const { return "ondisk2"; }

}
}
}
