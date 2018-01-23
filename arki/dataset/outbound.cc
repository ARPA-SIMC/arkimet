#include "outbound.h"
#include "step.h"
#include "empty.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
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
    auto w = file(md, md.source().format);
    w->append(md);
}

WriterAcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    auto age_check = config().check_acquire_age(md);
    if (age_check.first) return age_check.second;

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

void Writer::acquire_batch(std::vector<std::shared_ptr<WriterBatchElement>>& batch, ReplaceStrategy replace)
{
    for (auto& e: batch)
    {
        e->dataset_name.clear();
        e->result = acquire(e->md, replace);
        if (e->result == ACQ_OK)
            e->dataset_name = name();
    }
}

void Writer::remove(Metadata&)
{
    throw std::runtime_error("cannot remove data from outbound dataset: dataset does not support removing items");
}

void Writer::removeAll(std::ostream& log, bool writable)
{
    log << name() << ": cleaning dataset not implemented" << endl;
}

void Writer::test_acquire(const ConfigFile& cfg, std::vector<std::shared_ptr<WriterBatchElement>>& batch, std::ostream& out)
{
    std::shared_ptr<const outbound::Config> config(new outbound::Config(cfg));
    for (auto& e: batch)
    {
        auto age_check = config->check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = config->name;
            else
                e->dataset_name.clear();
            continue;
        }

        const core::Time& time = e->md.get<types::reftime::Position>()->time;
        auto tf = Step::create(cfg.value("step"));
        string dest = cfg.value("path") + "/" + (*tf)(time) + "." + e->md.source().format;
        out << "Assigning to dataset " << cfg.value("name") << " in file " << dest << endl;
        e->result = ACQ_OK;
        e->dataset_name = config->name;
    }
}

}
}
}
