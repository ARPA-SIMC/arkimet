#include "ondisk2.h"
#include "ondisk2/reader.h"
#include "ondisk2/writer.h"
#include "ondisk2/checker.h"
#include "arki/utils/string.h"
#include "step.h"

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

std::unique_ptr<dataset::Reader> Dataset::create_reader() const
{
    auto cfg = dynamic_pointer_cast<const ondisk2::Dataset>(shared_from_this());
    return std::unique_ptr<dataset::Reader>(new ondisk2::Reader(cfg));
}

std::unique_ptr<dataset::Writer> Dataset::create_writer() const
{
    auto cfg = dynamic_pointer_cast<const ondisk2::Dataset>(shared_from_this());
    return std::unique_ptr<dataset::Writer>(new ondisk2::Writer(cfg));
}

std::unique_ptr<dataset::Checker> Dataset::create_checker() const
{
    auto cfg = dynamic_pointer_cast<const ondisk2::Dataset>(shared_from_this());
    return std::unique_ptr<dataset::Checker>(new ondisk2::Checker(cfg));
}

}
}
}
