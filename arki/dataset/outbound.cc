#include "outbound.h"
#include "step.h"
#include "empty.h"
#include "session.h"
#include "arki/metadata.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/nag.h"

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace outbound {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : segmented::Dataset(session, std::make_shared<segment::Session>(cfg.value("path")), cfg)
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader() { return std::make_shared<empty::Reader>(static_pointer_cast<Dataset>(shared_from_this())); }
std::shared_ptr<dataset::Writer> Dataset::create_writer() { return std::make_shared<outbound::Writer>(static_pointer_cast<Dataset>(shared_from_this())); }


Writer::Writer(std::shared_ptr<Dataset> dataset)
    : DatasetAccess(dataset)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);
}

Writer::~Writer()
{
}

std::string Writer::type() const { return "outbound"; }

void Writer::storeBlob(const segment::data::WriterConfig& writer_config, Metadata& md, const std::filesystem::path& reldest)
{
    // Write using segment::Writer
    core::Time time = md.get<types::reftime::Position>()->get_Position();
    auto relpath = sys::with_suffix(dataset().step()(time), "."s + format_name(md.source().format));
    auto w = dataset().segment_session->segment_writer(writer_config, md.source().format, dataset().path, relpath);
    w->append(md);
}

WriterAcquireResult Writer::acquire(Metadata& md, const AcquireConfig& cfg)
{
    acct::acquire_single_count.incr();
    auto age_check = dataset().check_acquire_age(md);
    if (age_check.first) return age_check.second;

    core::Time time = md.get<types::reftime::Position>()->get_Position();
    string reldest = dataset().step()(time);
    auto dest = dataset().path / reldest;

    std::filesystem::create_directories(dest.parent_path());

    segment::data::WriterConfig writer_config;
    writer_config.drop_cached_data_on_commit = cfg.drop_cached_data_on_commit;
    writer_config.eatmydata = dataset().eatmydata;

    try {
        storeBlob(writer_config, md, reldest);
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

        core::Time time = e->md.get<types::reftime::Position>()->get_Position();
        auto tf = Step::create(cfg.value("step"));
        auto dest = std::filesystem::path(cfg.value("path")) / sys::with_suffix((*tf)(time), "."s + format_name(e->md.source().format));
        nag::verbose("Assigning to dataset %s in file %s", cfg.value("name").c_str(), dest.c_str());
        e->result = ACQ_OK;
        e->dataset_name = config->name();
    }
}

}
}
}
