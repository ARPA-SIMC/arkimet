#include "iseg.h"
#include "iseg/reader.h"
#include "iseg/writer.h"
#include "iseg/checker.h"
#include "arki/defs.h"
#include "arki/types.h"
#include "arki/dataset/session.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace iseg {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : segmented::Dataset(session, cfg),
      iseg{
          cfg.value("format"),
          types::parse_code_names(cfg.value("index")),
          types::parse_code_names(cfg.value("unique")),
          cfg.value_bool("trace_sql"),
          eatmydata,
      },
      summary_cache_pathname{path / ".summaries"}
{
    if (iseg.format.empty())
        throw std::runtime_error("Dataset " + name() + " misses format= configuration");

    iseg.unique.erase(TYPE_REFTIME);
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

std::shared_ptr<segment::data::Reader> Dataset::segment_reader(const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock)
{
    return session->segment_reader(iseg.format, path, relpath, lock);
}

}
}
}
