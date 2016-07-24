#include "simple.h"
#include "simple/reader.h"
#include "simple/writer.h"
#include "step.h"
#include "arki/utils/string.h"
#include "arki/metadata.h"
#include "arki/types/reftime.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

Config::Config(const ConfigFile& cfg)
    : dataset::IndexedConfig(cfg), sharding(cfg),
      index_type(cfg.value("index_type"))
{
}

std::shared_ptr<const Config> Config::create_shard(const core::Time& time) const
{
    std::string shard_path = sharding.step->shard_path(time);
    std::unique_ptr<Config> cfg(new Config(*this));
    cfg->sharding.active = false;
    cfg->sharding.step = std::shared_ptr<ShardStep>();
    cfg->to_shard(shard_path, sharding.step->substep(time));
    return std::shared_ptr<const Config>(cfg.release());
}

std::shared_ptr<const Config> Config::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}

std::unique_ptr<dataset::Reader> Config::create_reader() const
{
    auto cfg = dynamic_pointer_cast<const simple::Config>(shared_from_this());
    if (sharding.active)
        return std::unique_ptr<dataset::Reader>(new simple::ShardingReader(cfg));
    else
        return std::unique_ptr<dataset::Reader>(new simple::Reader(cfg));
}

std::unique_ptr<dataset::Writer> Config::create_writer() const
{
    auto cfg = dynamic_pointer_cast<const simple::Config>(shared_from_this());
    if (sharding.active)
        return std::unique_ptr<dataset::Writer>(new simple::ShardingWriter(cfg));
    else
        return std::unique_ptr<dataset::Writer>(new simple::Writer(cfg));
}

std::unique_ptr<dataset::Checker> Config::create_checker() const
{
    auto cfg = dynamic_pointer_cast<const simple::Config>(shared_from_this());
    if (sharding.active)
        return std::unique_ptr<dataset::Checker>(new simple::ShardingChecker(cfg));
    else
        return std::unique_ptr<dataset::Checker>(new simple::Checker(cfg));
}

}
}
}

