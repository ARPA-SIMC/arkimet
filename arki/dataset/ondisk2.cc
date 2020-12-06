#include "ondisk2.h"
#include "ondisk2/reader.h"
#include "ondisk2/writer.h"
#include "ondisk2/checker.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::indexed::Dataset(session, cfg),
      summary_cache_pathname(str::joinpath(path, ".summaries")),
      indexfile(cfg.value("indexfile")),
      index(cfg.value("index")),
      unique(cfg.value("unique"))
{
    if (indexfile.empty())
        indexfile = "index.sqlite";

    if (indexfile != ":memory:")
        index_pathname = str::joinpath(path, indexfile);
    else
        index_pathname = indexfile;

    if (index.empty())
        index = "origin, product, level, timerange, area, proddef, run";
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<ondisk2::Reader>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Writer> Dataset::create_writer()
{
    return std::make_shared<ondisk2::Writer>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Checker> Dataset::create_checker()
{
    return std::make_shared<ondisk2::Checker>(static_pointer_cast<Dataset>(shared_from_this()));
}

}
}
}
