#include "outbound.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "empty.h"
#include "session.h"
#include "step.h"

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace outbound {

Dataset::Dataset(std::shared_ptr<Session> session,
                 const core::cfg::Section& cfg)
    : segmented::Dataset(session, std::make_shared<segment::Session>(cfg), cfg)
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<empty::Reader>(
        static_pointer_cast<Dataset>(shared_from_this()));
}
std::shared_ptr<dataset::Writer> Dataset::create_writer()
{
    return std::make_shared<outbound::Writer>(
        static_pointer_cast<Dataset>(shared_from_this()));
}

Writer::Writer(std::shared_ptr<Dataset> dataset) : DatasetAccess(dataset)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);
}

Writer::~Writer() {}

std::string Writer::type() const { return "outbound"; }

metadata::Inbound::Result Writer::acquire(Metadata& md,
                                          const AcquireConfig& cfg)
{
    acct::acquire_single_count.incr();
    auto age_check = dataset().check_acquire_age(md);
    if (age_check.first)
        return age_check.second;

    core::Time time = md.get<types::reftime::Position>()->get_Position();
    auto segment = dataset().segment_session->segment_from_relpath_and_format(
        dataset().step()(time), md.source().format);
    std::filesystem::create_directories(segment->abspath().parent_path());

    segment::WriterConfig writer_config;
    writer_config.drop_cached_data_on_commit = cfg.drop_cached_data_on_commit;

    try
    {
        auto w = dataset().segment_session->segment_data_writer(segment,
                                                                writer_config);
        w->append(md);
        return metadata::Inbound::Result::OK;
    }
    catch (std::exception& e)
    {
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return metadata::Inbound::Result::ERROR;
    }

    // This should never be reached, but we throw an exception to avoid a
    // warning from the compiler
    throw std::runtime_error("this code path should never be reached (it is "
                             "here to appease a compiler warning)");
}

void Writer::acquire_batch(metadata::InboundBatch& batch,
                           const AcquireConfig& cfg)
{
    acct::acquire_batch_count.incr();
    for (auto& e : batch)
    {
        e->destination.clear();
        e->result = acquire(*e->md, cfg);
        if (e->result == metadata::Inbound::Result::OK)
            e->destination = name();
    }
}

void Writer::test_acquire(std::shared_ptr<Session> session,
                          const core::cfg::Section& cfg,
                          metadata::InboundBatch& batch)
{
    std::shared_ptr<const outbound::Dataset> config(
        new outbound::Dataset(session, cfg));
    for (auto& e : batch)
    {
        auto age_check = config->check_acquire_age(*e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == metadata::Inbound::Result::OK)
                e->destination = config->name();
            else
                e->destination.clear();
            continue;
        }

        core::Time time =
            e->md->get<types::reftime::Position>()->get_Position();
        auto tf = Step::create(cfg.value("step"));
        auto dest =
            std::filesystem::path(cfg.value("path")) /
            sys::with_suffix((*tf)(time),
                             "."s + format_name(e->md->source().format));
        nag::verbose("Assigning to dataset %s in file %s",
                     cfg.value("name").c_str(), dest.c_str());
        e->result      = metadata::Inbound::Result::OK;
        e->destination = config->name();
    }
}

} // namespace outbound
} // namespace dataset
} // namespace arki
