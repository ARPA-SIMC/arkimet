#include "fromfunction.h"
#include "arki/metadata.h"
#include <ostream>

using namespace std;

namespace arki {
namespace dataset {
namespace fromfunction {

Config::Config(const ConfigFile& cfg)
    : dataset::Config(cfg)
{
}

std::shared_ptr<const Config> Config::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}

std::unique_ptr<dataset::Reader> Config::create_reader() const { return std::unique_ptr<dataset::Reader>(new Reader(shared_from_this())); }


Reader::Reader(std::shared_ptr<const dataset::Config> config) : m_config(config) {}
Reader::~Reader() {}

bool Reader::query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest)
{
    return generator([&](std::unique_ptr<Metadata> md) {
        if (!q.matcher(*md))
            return true;
        return dest(std::move(md));
    });
}

}
}
}
