#include "outbound.h"
#include "step.h"
#include "empty.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/types/reftime.h"
#include "arki/dataset/segment.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <sys/stat.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace outbound {

Config::Config(const ConfigFile& cfg)
    : segmented::Config(cfg)
{
}

std::shared_ptr<const Config> Config::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}

std::unique_ptr<dataset::Reader> Config::create_reader() const { return std::unique_ptr<dataset::Reader>(new empty::Reader(shared_from_this())); }
std::unique_ptr<dataset::Writer> Config::create_writer() const { return std::unique_ptr<dataset::Writer>(new Writer(dynamic_pointer_cast<const Config>(shared_from_this()))); }


Writer::Writer(std::shared_ptr<const segmented::Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

Writer::~Writer()
{
}

std::string Writer::type() const { return "outbound"; }

void Writer::storeBlob(Metadata& md, const std::string& reldest)
{
    // Write using segment::Writer
    Segment* w = file(md, md.source().format);
    w->append(md);
}

Writer::AcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    const core::Time& time = md.get<types::reftime::Position>()->time;
    string reldest = config().step()(time);
    string dest = path() + "/" + reldest;

    sys::makedirs(str::dirname(dest));

    try {
        storeBlob(md, reldest);
        return ACQ_OK;
    } catch (std::exception& e) {
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return ACQ_ERROR;
    }

    // This should never be reached, but we throw an exception to avoid a
    // warning from the compiler
    throw std::runtime_error("this code path should never be reached (it is here to appease a compiler warning)");
}

void Writer::remove(Metadata&)
{
    throw std::runtime_error("cannot remove data from outbound dataset: dataset does not support removing items");
}

void Writer::removeAll(std::ostream& log, bool writable)
{
    log << name() << ": cleaning dataset not implemented" << endl;
}

Writer::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    const core::Time& time = md.get<types::reftime::Position>()->time;
    auto tf = Step::create(cfg.value("step"));
    string dest = cfg.value("path") + "/" + (*tf)(time) + "." + md.source().format;
    out << "Assigning to dataset " << cfg.value("name") << " in file " << dest << endl;
    return ACQ_OK;
}

}
}
}
