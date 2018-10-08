#include "config.h"
#include "empty.h"
#include "arki/metadata/collection.h"
#include <ostream>

using namespace std;

namespace arki {
namespace dataset {
namespace empty {

Config::Config(const ConfigFile& cfg)
    : dataset::Config(cfg)
{
}

std::shared_ptr<const Config> Config::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}

std::unique_ptr<dataset::Reader> Config::create_reader() const { return std::unique_ptr<dataset::Reader>(new Reader(shared_from_this())); }
std::unique_ptr<dataset::Writer> Config::create_writer() const { return std::unique_ptr<dataset::Writer>(new Writer(shared_from_this())); }
std::unique_ptr<dataset::Checker> Config::create_checker() const { return std::unique_ptr<dataset::Checker>(new Checker(shared_from_this())); }


Reader::Reader(std::shared_ptr<const dataset::Config> config) : m_config(config) {}
Reader::~Reader() {}


WriterAcquireResult Writer::acquire(Metadata& md, const AcquireConfig& cfg)
{
    return ACQ_OK;
}

void Writer::acquire_batch(WriterBatch& batch, const AcquireConfig& cfg)
{
    for (auto& e: batch)
    {
        e->result = ACQ_OK;
        e->dataset_name = name();
    }
}

void Writer::test_acquire(const ConfigFile& cfg, WriterBatch& batch, std::ostream& out)
{
    std::shared_ptr<const empty::Config> config(new empty::Config(cfg));
    for (auto& e: batch)
    {
        e->result = ACQ_OK;
        e->dataset_name = config->name;
    }
}

}
}
}
