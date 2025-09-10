#include "outbound.h"
#include "arki/dataset/lock.h"
#include "arki/metadata.h"
#include "arki/nag.h"
#include "arki/segment/scan.h"
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

namespace arki::dataset::outbound {

std::shared_ptr<segment::Writer>
SegmentSession::segment_writer(std::shared_ptr<const Segment> segment,
                               std::shared_ptr<core::AppendLock> lock) const
{
    return std::make_shared<segment::scan::Writer>(segment, lock);
}

Dataset::Dataset(std::shared_ptr<Session> session,
                 const core::cfg::Section& cfg)
    : segmented::Dataset(session, std::make_shared<SegmentSession>(cfg), cfg)
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

std::shared_ptr<core::AppendLock> Dataset::append_lock_dataset() const
{
    return std::make_shared<DatasetAppendLock>(*this);
}

/*
 * Writer
 */

Writer::Writer(std::shared_ptr<Dataset> dataset) : DatasetAccess(dataset)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);
}

Writer::~Writer() {}

std::string Writer::type() const { return "outbound"; }

void Writer::acquire_batch(metadata::InboundBatch& batch,
                           const AcquireConfig& cfg)
{
    acct::acquire_batch_count.incr();

    segment::WriterConfig writer_config{dataset().name()};
    writer_config.drop_cached_data_on_commit = cfg.drop_cached_data_on_commit;

    std::map<std::string, metadata::InboundBatch> by_segment =
        batch_by_segment(batch);

    // Import data grouped by segment
    auto lock = dataset().append_lock_dataset();
    for (auto& s : by_segment)
    {
        auto segment = dataset().segment_session->segment_from_relpath(s.first);
        auto writer  = segment->writer(lock);
        writer->acquire(s.second, writer_config);
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

} // namespace arki::dataset::outbound
