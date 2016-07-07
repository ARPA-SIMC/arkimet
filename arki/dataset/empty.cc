#include "config.h"

#include <arki/dataset/empty.h>

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


Writer::AcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    return ACQ_OK;
}

Writer::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    out << "Resetting dataset information to mark that the message has been discarded" << endl;
    return ACQ_OK;
}


}
}
}
