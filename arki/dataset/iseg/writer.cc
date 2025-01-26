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

class AppendSegment
{
public:
    std::shared_ptr<iseg::Dataset> dataset;
    std::shared_ptr<core::AppendLock> append_lock;
    std::shared_ptr<const Segment> segment;
    std::shared_ptr<segment::data::Writer> data_writer;
    std::shared_ptr<AIndex> idx;

    AppendSegment(std::shared_ptr<iseg::Dataset> dataset, std::shared_ptr<core::AppendLock> append_lock, std::shared_ptr<segment::data::Writer> data_writer)
        : dataset(dataset), append_lock(append_lock), segment(data_writer->segment().shared_from_this()), data_writer(data_writer), idx(dataset->iseg_segment_session->append_index(data_writer->segment().shared_from_this(), append_lock))
    {
    }

    void acquire_batch_replace_never(metadata::InboundBatch& batch)
    {
        Pending p_idx = idx->begin_transaction();

        try {
            for (auto& e: batch)
            {
                e->destination.clear();

                if (std::unique_ptr<types::source::Blob> old = idx->index(*e->md, data_writer->next_offset()))
                {
                    e->md->add_note("Failed to store in dataset '" + dataset->name() + "' because the dataset already has the data in " + segment->relpath().native() + ":" + std::to_string(old->offset));
                    e->result = metadata::Inbound::Result::DUPLICATE;
                    continue;
                }

                data_writer->append(*e->md);
                e->result = metadata::Inbound::Result::OK;
                e->destination = dataset->name();
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            batch.set_all_error("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return;
        }

        data_writer->commit();
        p_idx.commit();
    }

    void acquire_batch_replace_always(metadata::InboundBatch& batch)
    {
        Pending p_idx = idx->begin_transaction();

        try {
            for (auto& e: batch)
            {
                e->destination.clear();
                idx->replace(*e->md, data_writer->next_offset());
                data_writer->append(*e->md);
                e->result = metadata::Inbound::Result::OK;
                e->destination = dataset->name();
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            batch.set_all_error("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return;
        }

        data_writer->commit();
        p_idx.commit();
    }

    void acquire_batch_replace_higher_usn(metadata::InboundBatch& batch)
    {
        Pending p_idx = idx->begin_transaction();

        try {
            for (auto& e: batch)
            {
                e->destination.clear();

                // Try to acquire without replacing
                if (std::unique_ptr<types::source::Blob> old = idx->index(*e->md, data_writer->next_offset()))
                {
                    // Duplicate detected

                    // Read the update sequence number of the new BUFR
                    int new_usn;
                    if (!scan::Scanner::update_sequence_number(*e->md, new_usn))
                    {
                        e->md->add_note("Failed to store in dataset '" + dataset->name() + "' because the dataset already has the data in " + segment->relpath().native() + ":" + std::to_string(old->offset) + " and there is no Update Sequence Number to compare");
                        e->result = metadata::Inbound::Result::DUPLICATE;
                        continue;
                    }

                    // Read the update sequence number of the old BUFR
                    auto reader = segment->data_reader(append_lock);
                    old->lock(reader);
                    int old_usn;
                    if (!scan::Scanner::update_sequence_number(*old, old_usn))
                    {
                        e->md->add_note("Failed to store in dataset '" + dataset->name() + "': a similar element exists, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");
                        e->result = metadata::Inbound::Result::ERROR;
                        continue;
                    }

                    // If the new element has no higher Update Sequence Number, report a duplicate
                    if (old_usn > new_usn)
                    {
                        e->md->add_note("Failed to store in dataset '" + dataset->name() + "' because the dataset already has the data in " + segment->relpath().native() + ":" + std::to_string(old->offset) + " with a higher Update Sequence Number");
                        e->result = metadata::Inbound::Result::DUPLICATE;
                        continue;
                    }

                    // Replace, reusing the pending datafile transaction from earlier
                    idx->replace(*e->md, data_writer->next_offset());
                }
                data_writer->append(*e->md);
                e->result = metadata::Inbound::Result::OK;
                e->destination = dataset->name();
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            batch.set_all_error("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return;
        }

        data_writer->commit();
        p_idx.commit();
    }
};


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

std::filesystem::path Writer::get_relpath(const Metadata& md)
{
    core::Time time = md.get<types::reftime::Position>()->get_Position();
    return sys::with_suffix(dataset().step()(time), "."s + format_name(dataset().iseg_segment_session->format));
}

std::unique_ptr<AppendSegment> Writer::file(const segment::WriterConfig& writer_config, const Metadata& md)
{
    return file(writer_config, get_relpath(md));
}

std::unique_ptr<AppendSegment> Writer::file(const segment::WriterConfig& writer_config, const std::filesystem::path& relpath)
{
    std::filesystem::create_directories((dataset().path / relpath).parent_path());
    std::shared_ptr<core::AppendLock> append_lock(dataset().append_lock_segment(relpath));
    auto segment = dataset().segment_session->segment_from_relpath_and_format(relpath, dataset().iseg_segment_session->format);
    auto data_writer = dataset().segment_session->segment_data_writer(segment, writer_config);
    return std::unique_ptr<AppendSegment>(new AppendSegment(m_dataset, append_lock, data_writer));
}

void Writer::acquire_batch(metadata::InboundBatch& batch, const AcquireConfig& cfg)
{
    acct::acquire_batch_count.incr();
    ReplaceStrategy replace = cfg.replace == REPLACE_DEFAULT ? dataset().default_replace_strategy : cfg.replace;

    if (batch.empty()) return;
    if (batch[0]->md->source().format != dataset().iseg_segment_session->format)
    {
        batch.set_all_error("cannot acquire into dataset " + name() + ": data is in format " + format_name(batch[0]->md->source().format) + " but the dataset only accepts " + format_name(dataset().iseg_segment_session->format));
        return;
    }

    segment::WriterConfig writer_config;
    writer_config.drop_cached_data_on_commit = cfg.drop_cached_data_on_commit;

    std::map<std::string, metadata::InboundBatch> by_segment = batch_by_segment(batch);

    // Import segment by segment
    for (auto& s: by_segment)
    {
        auto segment = file(writer_config, s.first);
        switch (replace)
        {
            case REPLACE_NEVER:
                segment->acquire_batch_replace_never(s.second);
                scache.invalidate(s.second);
                break;
            case REPLACE_ALWAYS:
                segment->acquire_batch_replace_always(s.second);
                scache.invalidate(s.second);
                break;
            case REPLACE_HIGHER_USN:
                segment->acquire_batch_replace_higher_usn(s.second);
                scache.invalidate(s.second);
                break;
            default: throw std::runtime_error("programming error: unsupported replace value " + std::to_string(replace));
        }
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
