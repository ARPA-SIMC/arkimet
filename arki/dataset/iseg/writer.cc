#include "arki/dataset/iseg/writer.h"
#include "arki/segment/iseg/index.h"
#include "arki/dataset/step.h"
#include "arki/dataset/lock.h"
#include "arki/dataset/session.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/utils/accounting.h"
#include "arki/scan.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/nag.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;
using arki::segment::iseg::AIndex;

namespace arki {
namespace dataset {
namespace iseg {

Writer::Writer(std::shared_ptr<iseg::Dataset> dataset)
    : DatasetAccess(dataset), scache(dataset->summary_cache_pathname)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);
    scache.openRW();
}

Writer::~Writer()
{
    flush();
}

std::string Writer::type() const { return "iseg"; }

void Writer::acquire_batch(metadata::InboundBatch& batch, const AcquireConfig& cfg)
{
    acct::acquire_batch_count.incr();

    if (batch.empty()) return;
    if (batch[0]->md->source().format != dataset().iseg_segment_session->format)
    {
        batch.set_all_error("cannot acquire into dataset " + name() + ": data is in format " + format_name(batch[0]->md->source().format) + " but the dataset only accepts " + format_name(dataset().iseg_segment_session->format));
        return;
    }

    segment::WriterConfig writer_config{dataset().name()};
    writer_config.replace_strategy = cfg.replace == ReplaceStrategy::DEFAULT ? dataset().default_replace_strategy : cfg.replace;
    writer_config.drop_cached_data_on_commit = cfg.drop_cached_data_on_commit;

    // Import segment by segment
    std::map<std::string, metadata::InboundBatch> by_segment = batch_by_segment(batch);
    for (auto& s: by_segment)
    {
        auto segment = dataset().segment_session->segment_from_relpath_and_format(s.first, dataset().iseg_segment_session->format);
        std::filesystem::create_directories(segment->abspath().parent_path());
        std::shared_ptr<core::AppendLock> lock(dataset().append_lock_segment(segment->relpath()));
        auto writer = segment->writer(lock);
        writer->acquire(s.second, writer_config);
        scache.invalidate(s.second);
    }
}

void Writer::test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, metadata::InboundBatch& batch)
{
    std::shared_ptr<const iseg::Dataset> dataset(new iseg::Dataset(session, cfg));
    for (auto& e: batch)
    {
        auto age_check = dataset->check_acquire_age(*e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == metadata::Inbound::Result::OK)
                e->destination = dataset->name();
            else
                e->destination.clear();
        } else {
            // TODO: check for duplicates
            e->result = metadata::Inbound::Result::OK;
            e->destination = dataset->name();
        }
    }
}

}
}
}
