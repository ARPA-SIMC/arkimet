#include "ondisk2.h"
#include "ondisk2/reader.h"
#include "ondisk2/writer.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {

Config::Config(const ConfigFile& cfg)
    : dataset::IndexedConfig(cfg),
      smallfiles(ConfigFile::boolValue(cfg.value("smallfiles"))),
      summary_cache_pathname(str::joinpath(path, ".summaries")),
      index_pathname(cfg.value("indexfile")),
      index(cfg.value("index")),
      unique(cfg.value("unique"))
{
    if (index_pathname.empty())
        index_pathname = "index.sqlite";

    if (index_pathname != ":memory:")
        index_pathname = str::joinpath(path, index_pathname);

    if (index.empty())
        index = "origin, product, level, timerange, area, proddef, run";
}

dataset::Reader* Config::create_reader() const { return new ondisk2::Reader(dynamic_pointer_cast<const ondisk2::Config>(shared_from_this())); }
dataset::Writer* Config::create_writer() const { return new ondisk2::Writer(dynamic_pointer_cast<const ondisk2::Config>(shared_from_this())); }
dataset::Checker* Config::create_checker() const { return new ondisk2::Checker(dynamic_pointer_cast<const ondisk2::Config>(shared_from_this())); }

}
}
}
