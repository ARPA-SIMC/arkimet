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

dataset::Reader* Config::create_reader() const { return new simple::Reader(dynamic_pointer_cast<const simple::Config>(shared_from_this())); }
dataset::Writer* Config::create_writer() const { return new simple::Writer(dynamic_pointer_cast<const simple::Config>(shared_from_this())); }
dataset::Checker* Config::create_checker() const { return new simple::Checker(dynamic_pointer_cast<const simple::Config>(shared_from_this())); }

}
}
}

