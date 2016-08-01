#include "ondisk2.h"
#include "ondisk2/reader.h"
#include "ondisk2/writer.h"
#include "arki/utils/string.h"
#include "step.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {

Config::Config(const ConfigFile& cfg)
    : sharded::Config<dataset::IndexedConfig>(cfg),
      smallfiles(ConfigFile::boolValue(cfg.value("smallfiles"))),
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

std::shared_ptr<const dataset::Config> Config::create_shard(const core::Time& time) const
{
    std::string shard_path = shard_step->shard_path(time);
    std::unique_ptr<Config> cfg(new Config(*this));
    cfg->to_shard(shard_path, shard_step->substep(time));
    cfg->summary_cache_pathname = str::joinpath(cfg->path, ".summaries");
    if (cfg->indexfile != ":memory:")
        cfg->index_pathname = str::joinpath(cfg->path, cfg->indexfile);
    else
        cfg->index_pathname = cfg->indexfile;
    return std::shared_ptr<const dataset::Config>(cfg.release());
}

std::shared_ptr<const Config> Config::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}

std::unique_ptr<dataset::Reader> Config::create_reader() const
{
    auto cfg = dynamic_pointer_cast<const ondisk2::Config>(shared_from_this());
    if (sharded)
        return std::unique_ptr<dataset::Reader>(new ondisk2::ShardingReader(cfg));
    else
        return std::unique_ptr<dataset::Reader>(new ondisk2::Reader(cfg));
}

std::unique_ptr<dataset::Writer> Config::create_writer() const
{
    auto cfg = dynamic_pointer_cast<const ondisk2::Config>(shared_from_this());
    if (sharded)
        return std::unique_ptr<dataset::Writer>(new ondisk2::ShardingWriter(cfg));
    else
        return std::unique_ptr<dataset::Writer>(new ondisk2::Writer(cfg));
}

std::unique_ptr<dataset::Checker> Config::create_checker() const
{
    auto cfg = dynamic_pointer_cast<const ondisk2::Config>(shared_from_this());
    if (sharded)
        return std::unique_ptr<dataset::Checker>(new ondisk2::ShardingChecker(cfg));
    else
        return std::unique_ptr<dataset::Checker>(new ondisk2::Checker(cfg));
}

}
}
}
