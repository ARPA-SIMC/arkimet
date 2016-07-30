#include "sharded.h"
#include "step.h"
#include "simple.h"
#include "ondisk2.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/types/reftime.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

namespace sharded {

template<typename Base>
Config<Base>::Config(const ConfigFile& cfg)
    : Base(cfg), sharded(!cfg.value("shard").empty())
{
    std::string shard = cfg.value("shard");
    shard_step = ShardStep::create(shard, cfg.value("step"));
}


template<typename Config>
Reader<Config>::Reader(std::shared_ptr<const Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

template<typename Config>
Reader<Config>::~Reader() {}

template<typename Config>
void Reader<Config>::query_data(const dataset::DataQuery& q, metadata_dest_func dest) { throw std::runtime_error("not implemented"); }

template<typename Config>
void Reader<Config>::query_summary(const Matcher& matcher, Summary& summary) { throw std::runtime_error("not implemented"); }

template<typename Config>
void Reader<Config>::expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) { throw std::runtime_error("not implemented"); }


template<typename Config>
Writer<Config>::Writer(std::shared_ptr<const Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

template<typename Config>
Writer<Config>::~Writer()
{
    for (auto& i: shards)
        delete i.second;
}

template<typename Config>
dataset::Writer& Writer<Config>::shard(const core::Time& time)
{
    std::string shard_path = config().shard_step->shard_path(time);
    auto res = shards.find(shard_path);
    if (res == shards.end())
    {
        std::shared_ptr<const dataset::Config> shard_cfg = m_config->create_shard(time);
        auto writer = shard_cfg->create_writer();
        auto i = shards.emplace(make_pair(shard_path, writer.release()));
        return *i.first->second;
    } else
        return *res->second;
}

template<typename Config>
dataset::Writer::AcquireResult Writer<Config>::acquire(Metadata& md, dataset::Writer::ReplaceStrategy replace)
{
    const core::Time& time = md.get<types::reftime::Position>()->time;
    return shard(time).acquire(md, replace);
}

template<typename Config>
void Writer<Config>::remove(Metadata& md)
{
    for (auto& i: shards)
        delete i.second;
    const core::Time& time = md.get<types::reftime::Position>()->time;
    return shard(time).remove(md);
}

template<typename Config>
void Writer<Config>::flush()
{
    // Flush all shards
    for (auto& i: shards)
        i.second->flush();

    // Deallocate all cached shards
    for (auto& i: shards)
        delete i.second;
    shards.clear();
}


template<typename Config>
Checker<Config>::Checker(std::shared_ptr<const Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

template<typename Config>
Checker<Config>::~Checker() {}

template<typename Config>
void Checker<Config>::removeAll(dataset::Reporter& reporter, bool writable) { throw std::runtime_error("not implemented"); }

template<typename Config>
void Checker<Config>::repack(dataset::Reporter& reporter, bool writable) { throw std::runtime_error("not implemented"); }

template<typename Config>
void Checker<Config>::check(dataset::Reporter& reporter, bool fix, bool quick) { throw std::runtime_error("not implemented"); }


template class Config<dataset::IndexedConfig>;
template class Reader<simple::Config>;
//template class Reader<ondisk2::Config>;
template class Writer<simple::Config>;
//template class Writer<ondisk2::Config>;
template class Checker<simple::Config>;
//template class Checker<ondisk2::Config>;

}
}
}
