#include "iseg.h"
#include "iseg/reader.h"
#include "iseg/writer.h"
#include "step.h"
#include "arki/defs.h"
#include "arki/dataset/index/base.h"
#include "arki/utils/string.h"
#include "arki/metadata.h"
#include "arki/types/reftime.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace iseg {

Config::Config(const ConfigFile& cfg)
    : segmented::Config(cfg),
      format(cfg.value("format")),
      smallfiles(ConfigFile::boolValue(cfg.value("smallfiles"))),
      index(index::parseMetadataBitmask(cfg.value("index"))),
      unique(index::parseMetadataBitmask(cfg.value("unique")))
{
    if (format.empty())
        throw std::runtime_error("Dataset " + name + " misses format= configuration");

    unique.erase(TYPE_REFTIME);
}

std::shared_ptr<const Config> Config::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}

std::unique_ptr<dataset::Reader> Config::create_reader() const
{
    auto cfg = dynamic_pointer_cast<const iseg::Config>(shared_from_this());
    return std::unique_ptr<dataset::Reader>(new iseg::Reader(cfg));
}

std::unique_ptr<dataset::Writer> Config::create_writer() const
{
    auto cfg = dynamic_pointer_cast<const iseg::Config>(shared_from_this());
    return std::unique_ptr<dataset::Writer>(new iseg::Writer(cfg));
}

std::unique_ptr<dataset::Checker> Config::create_checker() const
{
    auto cfg = dynamic_pointer_cast<const iseg::Config>(shared_from_this());
    return std::unique_ptr<dataset::Checker>(new iseg::Checker(cfg));
}

}
}
}


