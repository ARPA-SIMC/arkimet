#include "iseg.h"
#include "iseg/reader.h"
#include "iseg/writer.h"
#include "iseg/checker.h"
#include "step.h"
#include "arki/defs.h"
#include "arki/dataset/index/base.h"
#include "arki/utils/string.h"
#include "arki/metadata.h"
#include "arki/types/reftime.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace iseg {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : segmented::Dataset(session, cfg),
      format(cfg.value("format")),
      index(index::parseMetadataBitmask(cfg.value("index"))),
      unique(index::parseMetadataBitmask(cfg.value("unique"))),
      summary_cache_pathname(str::joinpath(path, ".summaries")),
      trace_sql(cfg.value_bool("trace_sql"))
{
    if (format.empty())
        throw std::runtime_error("Dataset " + name + " misses format= configuration");

    unique.erase(TYPE_REFTIME);
}

std::unique_ptr<dataset::Reader> Dataset::create_reader() const
{
    auto cfg = dynamic_pointer_cast<const iseg::Dataset>(shared_from_this());
    return std::unique_ptr<dataset::Reader>(new iseg::Reader(cfg));
}

std::unique_ptr<dataset::Writer> Dataset::create_writer() const
{
    auto cfg = dynamic_pointer_cast<const iseg::Dataset>(shared_from_this());
    return std::unique_ptr<dataset::Writer>(new iseg::Writer(cfg));
}

std::unique_ptr<dataset::Checker> Dataset::create_checker() const
{
    auto cfg = dynamic_pointer_cast<const iseg::Dataset>(shared_from_this());
    return std::unique_ptr<dataset::Checker>(new iseg::Checker(cfg));
}

}
}
}


