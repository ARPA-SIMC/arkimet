#include "sharded.h"
#include "arki/configfile.h"
#include "step.h"
#include "simple.h"
#include "ondisk2.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

ShardingConfig::ShardingConfig(const ConfigFile& cfg)
    : active(!cfg.value("shard").empty())
{
    std::string shard = cfg.value("shard");
    step = ShardStep::create(shard, cfg.value("step"));
}

namespace sharded {

template<typename Config>
Reader<Config>::Reader(std::shared_ptr<const Config> config)
    : m_config(config), sharding(config->sharding)
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
Writer<Config>::~Writer() {}

template<typename Config>
dataset::Writer::AcquireResult Writer<Config>::acquire(Metadata& md, dataset::Writer::ReplaceStrategy replace) { throw std::runtime_error("not implemented"); }

template<typename Config>
void Writer<Config>::remove(Metadata& md) { throw std::runtime_error("not implemented"); }

template<typename Config>
void Writer<Config>::flush() { throw std::runtime_error("not implemented"); }


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


template class Reader<simple::Config>;
template class Reader<ondisk2::Config>;
template class Writer<simple::Config>;
template class Writer<ondisk2::Config>;
template class Checker<simple::Config>;
template class Checker<ondisk2::Config>;

}
}
}
