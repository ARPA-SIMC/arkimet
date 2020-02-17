#include "outbound.h"
#include "step.h"
#include "empty.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/nag.h"
#include <sys/stat.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace outbound {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : segmented::Dataset(session, cfg)
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader() { return std::make_shared<empty::Reader>(static_pointer_cast<Dataset>(shared_from_this())); }
std::shared_ptr<dataset::Writer> Dataset::create_writer() { return std::make_shared<outbound::Writer>(static_pointer_cast<Dataset>(shared_from_this())); }


Writer::Writer(std::shared_ptr<segmented::Dataset> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

Writer::~Writer()
{
}

std::string Writer::type() const { return "outbound"; }

void Writer::storeBlob(Metadata& md, const std::string& reldest, bool drop_cached_data_on_commit)
{
    // Write using segment::Writer
    auto w = file(md, md.source().format);
    w->append(md, drop_cached_data_on_commit);
}

WriterAcquireResult Writer::acquire(Metadata& md, const AcquireConfig& cfg)
{
    acct::acquire_single_count.incr();
    auto age_check = config().check_acquire_age(md);
    if (age_check.first) return age_check.second;

    const core::Time& time = md.get<types::reftime::Position>()->time;
    string reldest = dataset().step()(time);
    string dest = dataset().path + "/" + reldest;

    sys::makedirs(str::dirname(dest));

    try {
        storeBlob(md, reldest, cfg.drop_cached_data_on_commit);
        return ACQ_OK;
    } catch (std::exception& e) {
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return ACQ_ERROR;
    }

    // This should never be reached, but we throw an exception to avoid a
    // warning from the compiler
    throw std::runtime_error("this code path should never be reached (it is here to appease a compiler warning)");
}

void Writer::acquire_batch(WriterBatch& batch, const AcquireConfig& cfg)
{
    acct::acquire_batch_count.incr();
    for (auto& e: batch)
    {
        e->dataset_name.clear();
        e->result = acquire(e->md, cfg);
        if (e->result == ACQ_OK)
            e->dataset_name = name();
    }
}

void Writer::remove(Metadata&)
{
    throw std::runtime_error("cannot remove data from outbound dataset: dataset does not support removing items");
}

void Writer::test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch)
{
    std::shared_ptr<const outbound::Dataset> config(new outbound::Dataset(session, cfg));
    for (auto& e: batch)
    {
        auto age_check = config->check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = config->name();
            else
                e->dataset_name.clear();
            continue;
        }

        const core::Time& time = e->md.get<types::reftime::Position>()->time;
        auto tf = Step::create(cfg.value("step"));
        string dest = cfg.value("path") + "/" + (*tf)(time) + "." + e->md.source().format;
        nag::verbose("Assigning to dataset %s in file %s", cfg.value("name").c_str(), dest.c_str());
        e->result = ACQ_OK;
        e->dataset_name = config->name();
    }
}

}
}
}
