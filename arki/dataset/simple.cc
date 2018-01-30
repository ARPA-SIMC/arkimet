#include "simple.h"
#include "simple/reader.h"
#include "simple/writer.h"
#include "simple/checker.h"
#include "step.h"
#include "arki/utils/string.h"
#include "arki/metadata.h"
#include "arki/types/reftime.h"
#include "arki/configfile.h"

using namespace std;
using namespace arki::utils;

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

std::unique_ptr<dataset::Reader> Config::create_reader() const
{
    auto cfg = dynamic_pointer_cast<const simple::Config>(shared_from_this());
    return std::unique_ptr<dataset::Reader>(new simple::Reader(cfg));
}

std::unique_ptr<dataset::Writer> Config::create_writer() const
{
    auto cfg = dynamic_pointer_cast<const simple::Config>(shared_from_this());
    return std::unique_ptr<dataset::Writer>(new simple::Writer(cfg));
}

std::unique_ptr<dataset::Checker> Config::create_checker() const
{
    auto cfg = dynamic_pointer_cast<const simple::Config>(shared_from_this());
    return std::unique_ptr<dataset::Checker>(new simple::Checker(cfg));
}

}
}
}

