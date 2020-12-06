#include "iseg.h"
#include "iseg/reader.h"
#include "iseg/writer.h"
#include "iseg/checker.h"
#include "arki/defs.h"
#include "arki/dataset/session.h"
#include "arki/dataset/index/base.h"
#include "arki/utils/string.h"

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
        throw std::runtime_error("Dataset " + name() + " misses format= configuration");

    unique.erase(TYPE_REFTIME);
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<iseg::Reader>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Writer> Dataset::create_writer()
{
    return std::make_shared<iseg::Writer>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Checker> Dataset::create_checker()
{
    return std::make_shared<iseg::Checker>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<segment::Reader> Dataset::segment_reader(const std::string& relpath, std::shared_ptr<core::Lock> lock)
{
    return session->segment_reader(format, path, relpath, lock);
}

}
}
}
