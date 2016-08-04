#include "config.h"
#include "reader.h"
#include "arki/dataset/index/contents.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {

Reader::Reader(std::shared_ptr<const ondisk2::Config> config)
    : m_config(config)
{
    if (sys::access(str::joinpath(config->path, "index.sqlite"), F_OK))
    {
        unique_ptr<index::RContents> idx(new index::RContents(m_config));
        idx->open();
        m_idx = idx.release();
    }
}

Reader::~Reader()
{
}

std::string Reader::type() const { return "ondisk2"; }

std::string ShardingReader::type() const { return "ondisk2"; }

}
}
}
