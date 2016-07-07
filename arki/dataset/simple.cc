#include "simple.h"
#include "simple/reader.h"
#include "simple/writer.h"

using namespace std;

namespace arki {
namespace dataset {
namespace simple {

Config::Config(const ConfigFile& cfg)
    : dataset::IndexedConfig(cfg),
      index_type(cfg.value("index_type"))
{
}

std::shared_ptr<const Config> Config::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}

std::unique_ptr<dataset::Reader> Config::create_reader() const { return std::unique_ptr<dataset::Reader>(new simple::Reader(dynamic_pointer_cast<const simple::Config>(shared_from_this()))); }
std::unique_ptr<dataset::Writer> Config::create_writer() const { return std::unique_ptr<dataset::Writer>(new simple::Writer(dynamic_pointer_cast<const simple::Config>(shared_from_this()))); }
std::unique_ptr<dataset::Checker> Config::create_checker() const { return std::unique_ptr<dataset::Checker>(new simple::Checker(dynamic_pointer_cast<const simple::Config>(shared_from_this()))); }

}
}
}

