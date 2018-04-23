#include "ondisk2.h"
#include "ondisk2/reader.h"
#include "ondisk2/writer.h"
#include "ondisk2/checker.h"
#include "arki/utils/string.h"
#include "arki/configfile.h"
#include "step.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {

Config::Config(const ConfigFile& cfg)
    : dataset::IndexedConfig(cfg),
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

std::shared_ptr<const Config> Config::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}

std::unique_ptr<dataset::Reader> Config::create_reader() const
{
    auto cfg = dynamic_pointer_cast<const ondisk2::Config>(shared_from_this());
    return std::unique_ptr<dataset::Reader>(new ondisk2::Reader(cfg));
}

std::unique_ptr<dataset::Writer> Config::create_writer() const
{
    auto cfg = dynamic_pointer_cast<const ondisk2::Config>(shared_from_this());
    return std::unique_ptr<dataset::Writer>(new ondisk2::Writer(cfg));
}

std::unique_ptr<dataset::Checker> Config::create_checker() const
{
    auto cfg = dynamic_pointer_cast<const ondisk2::Config>(shared_from_this());
    return std::unique_ptr<dataset::Checker>(new ondisk2::Checker(cfg));
}

}
}
}
