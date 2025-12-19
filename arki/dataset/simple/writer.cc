#include "arki/dataset/simple/writer.h"
#include "arki/dataset/lock.h"
#include "arki/dataset/session.h"
#include "arki/dataset/simple/manifest.h"
#include "arki/dataset/step.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/types/source/blob.h"
#include "arki/utils/accounting.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki::dataset::simple {

Writer::Writer(std::shared_ptr<simple::Dataset> dataset)
    : DatasetAccess(dataset), manifest(dataset->path, dataset->eatmydata)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!manifest::exists(dataset->path))
        files::createDontpackFlagfile(dataset->path);
}

Writer::~Writer() { flush(); }

std::string Writer::type() const { return "simple"; }

void Writer::invalidate_summary()
{
    std::filesystem::remove(dataset().path / "summary");
}

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
    manifest.reread();
    bool changed = false;
    for (auto& s : by_segment)
    {
        auto segment = dataset().segment_session->segment_from_relpath(s.first);
        auto writer  = segment->writer(lock);
        auto res     = writer->acquire(s.second, writer_config);
        if (res.count_ok > 0)
        {
            manifest.set(segment->relpath(), res.segment_mtime,
                         res.data_timespan);
            invalidate_summary();
            changed = true;
        }
    }
    if (changed)
        manifest.flush();
}

void Writer::test_acquire(std::shared_ptr<Session> session,
                          const core::cfg::Section& cfg,
                          metadata::InboundBatch& batch)
{
    std::shared_ptr<const simple::Dataset> dataset(
        new simple::Dataset(session, cfg));
    for (auto& e : batch)
    {
        auto age_check = dataset->check_acquire_age(*e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == metadata::Inbound::Result::OK)
                e->destination = dataset->name();
            else
                e->destination.clear();
        }
        else
        {
            // Acquire on simple datasets always succeeds except in case of
            // envrionment issues like I/O errors and full disks
            e->result      = metadata::Inbound::Result::OK;
            e->destination = dataset->name();
        }
    }
}

} // namespace arki::dataset::simple
