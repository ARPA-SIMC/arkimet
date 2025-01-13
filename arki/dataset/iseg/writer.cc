#include "arki/dataset/iseg/writer.h"
#include "arki/segment/index/iseg.h"
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
using arki::segment::index::iseg::AIndex;

namespace arki {
namespace dataset {
namespace iseg {

class AppendSegment
{
public:
    std::shared_ptr<iseg::Dataset> dataset;
    std::shared_ptr<dataset::AppendLock> append_lock;
    std::shared_ptr<segment::data::Writer> segment;
    AIndex idx;

    AppendSegment(std::shared_ptr<iseg::Dataset> dataset, std::shared_ptr<dataset::AppendLock> append_lock, std::shared_ptr<segment::data::Writer> segment)
        : dataset(dataset), append_lock(append_lock), segment(segment), idx(dataset->iseg, dataset->segment_session, segment, append_lock)
    {
    }

    WriterAcquireResult acquire_replace_never(Metadata& md, index::SummaryCache& scache)
    {
        Pending p_idx = idx.begin_transaction();

        try {
            if (std::unique_ptr<types::source::Blob> old = idx.index(md, segment->next_offset()))
            {
                md.add_note("Failed to store in dataset '" + dataset->name() + "' because the dataset already has the data in " + segment->segment().relpath().native() + ":" + std::to_string(old->offset));
                return ACQ_ERROR_DUPLICATE;
            }
            // Invalidate the summary cache for this month
            scache.invalidate(md);
            segment->append(md);
            segment->commit();
            p_idx.commit();
            return ACQ_OK;
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            md.add_note("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return ACQ_ERROR;
        }
    }

    WriterAcquireResult acquire_replace_always(Metadata& md, index::SummaryCache& scache)
    {
        Pending p_idx = idx.begin_transaction();

        try {
            idx.replace(md, segment->next_offset());
            // Invalidate the summary cache for this month
            scache.invalidate(md);
            segment->append(md);
            segment->commit();
            p_idx.commit();
            return ACQ_OK;
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            md.add_note("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return ACQ_ERROR;
        }
    }

    WriterAcquireResult acquire_replace_higher_usn(Metadata& md, index::SummaryCache& scache)
    {
        Pending p_idx = idx.begin_transaction();

        try {
            // Try to acquire without replacing
            if (std::unique_ptr<types::source::Blob> old = idx.index(md, segment->next_offset()))
            {
                // Duplicate detected

                // Read the update sequence number of the new BUFR
                int new_usn;
                if (!scan::Scanner::update_sequence_number(md, new_usn))
                    return ACQ_ERROR_DUPLICATE;

                // Read the update sequence number of the old BUFR
                auto reader = dataset->segment_session->segment_reader(dataset->iseg.format, old->filename, append_lock);
                old->lock(reader);
                int old_usn;
                if (!scan::Scanner::update_sequence_number(*old, old_usn))
                {
                    md.add_note("Failed to store in dataset '" + dataset->name() + "': a similar element exists, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");
                    return ACQ_ERROR;
                }

                // If the new element has no higher Update Sequence Number, report a duplicate
                if (old_usn > new_usn)
                    return ACQ_ERROR_DUPLICATE;

                // Replace, reusing the pending datafile transaction from earlier
                idx.replace(md, segment->next_offset());
                segment->append(md);
                segment->commit();
                p_idx.commit();
                return ACQ_OK;
            } else {
                segment->append(md);
                // Invalidate the summary cache for this month
                scache.invalidate(md);
                segment->commit();
                p_idx.commit();
                return ACQ_OK;
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            md.add_note("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return ACQ_ERROR;
        }
    }

    void acquire_batch_replace_never(WriterBatch& batch, index::SummaryCache& scache)
    {
        Pending p_idx = idx.begin_transaction();

        try {
            for (auto& e: batch)
            {
                e->dataset_name.clear();

                if (std::unique_ptr<types::source::Blob> old = idx.index(e->md, segment->next_offset()))
                {
                    e->md.add_note("Failed to store in dataset '" + dataset->name() + "' because the dataset already has the data in " + segment->segment().relpath().native() + ":" + std::to_string(old->offset));
                    e->result = ACQ_ERROR_DUPLICATE;
                    continue;
                }

                // Invalidate the summary cache for this month
                scache.invalidate(e->md);
                segment->append(e->md);
                e->result = ACQ_OK;
                e->dataset_name = dataset->name();
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            batch.set_all_error("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return;
        }

        segment->commit();
        p_idx.commit();
    }

    void acquire_batch_replace_always(WriterBatch& batch, index::SummaryCache& scache)
    {
        Pending p_idx = idx.begin_transaction();

        try {
            for (auto& e: batch)
            {
                e->dataset_name.clear();
                idx.replace(e->md, segment->next_offset());
                // Invalidate the summary cache for this month
                scache.invalidate(e->md);
                segment->append(e->md);
                e->result = ACQ_OK;
                e->dataset_name = dataset->name();
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            batch.set_all_error("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return;
        }

        segment->commit();
        p_idx.commit();
    }

    void acquire_batch_replace_higher_usn(WriterBatch& batch, index::SummaryCache& scache)
    {
        Pending p_idx = idx.begin_transaction();

        try {
            for (auto& e: batch)
            {
                e->dataset_name.clear();

                // Try to acquire without replacing
                if (std::unique_ptr<types::source::Blob> old = idx.index(e->md, segment->next_offset()))
                {
                    // Duplicate detected

                    // Read the update sequence number of the new BUFR
                    int new_usn;
                    if (!scan::Scanner::update_sequence_number(e->md, new_usn))
                    {
                        e->md.add_note("Failed to store in dataset '" + dataset->name() + "' because the dataset already has the data in " + segment->segment().relpath().native() + ":" + std::to_string(old->offset) + " and there is no Update Sequence Number to compare");
                        e->result = ACQ_ERROR_DUPLICATE;
                        continue;
                    }

                    // Read the update sequence number of the old BUFR
                    auto reader = dataset->segment_session->segment_reader(dataset->iseg.format, old->filename, append_lock);
                    old->lock(reader);
                    int old_usn;
                    if (!scan::Scanner::update_sequence_number(*old, old_usn))
                    {
                        e->md.add_note("Failed to store in dataset '" + dataset->name() + "': a similar element exists, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");
                        e->result = ACQ_ERROR;
                        continue;
                    }

                    // If the new element has no higher Update Sequence Number, report a duplicate
                    if (old_usn > new_usn)
                    {
                        e->md.add_note("Failed to store in dataset '" + dataset->name() + "' because the dataset already has the data in " + segment->segment().relpath().native() + ":" + std::to_string(old->offset) + " with a higher Update Sequence Number");
                        e->result = ACQ_ERROR_DUPLICATE;
                        continue;
                    }

                    // Replace, reusing the pending datafile transaction from earlier
                    idx.replace(e->md, segment->next_offset());
                    segment->append(e->md);
                    e->result = ACQ_OK;
                    e->dataset_name = dataset->name();
                    continue;
                } else {
                    // Invalidate the summary cache for this month
                    scache.invalidate(e->md);
                    segment->append(e->md);
                    e->result = ACQ_OK;
                    e->dataset_name = dataset->name();
                }
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            batch.set_all_error("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return;
        }

        segment->commit();
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
    return sys::with_suffix(dataset().step()(time), "."s + format_name(dataset().iseg.format));
}

std::unique_ptr<AppendSegment> Writer::file(const segment::data::WriterConfig& writer_config, const Metadata& md)
{
    return file(writer_config, get_relpath(md));
}

std::unique_ptr<AppendSegment> Writer::file(const segment::data::WriterConfig& writer_config, const std::filesystem::path& relpath)
{
    std::filesystem::create_directories((dataset().path / relpath).parent_path());
    std::shared_ptr<dataset::AppendLock> append_lock(dataset().append_lock_segment(relpath));
    auto segment = dataset().segment_session->segment_writer(writer_config, dataset().iseg.format, relpath);
    return std::unique_ptr<AppendSegment>(new AppendSegment(m_dataset, append_lock, segment));
}

WriterAcquireResult Writer::acquire(Metadata& md, const AcquireConfig& cfg)
{
    acct::acquire_single_count.incr();
    if (md.source().format != dataset().iseg.format)
        throw std::runtime_error("cannot acquire into dataset " + name() + ": data is in format " + format_name(md.source().format) + " but the dataset only accepts " + format_name(dataset().iseg.format));

    auto age_check = dataset().check_acquire_age(md);
    if (age_check.first) return age_check.second;

    ReplaceStrategy replace = cfg.replace == REPLACE_DEFAULT ? dataset().default_replace_strategy : cfg.replace;

    segment::data::WriterConfig writer_config;
    writer_config.drop_cached_data_on_commit = cfg.drop_cached_data_on_commit;
    writer_config.eatmydata = dataset().eatmydata;

    auto segment = file(writer_config, md);
    switch (replace)
    {
        case REPLACE_NEVER: return segment->acquire_replace_never(md, scache);
        case REPLACE_ALWAYS: return segment->acquire_replace_always(md, scache);
        case REPLACE_HIGHER_USN: return segment->acquire_replace_higher_usn(md, scache);
        default:
        {
            stringstream ss;
            ss << "cannot acquire into dataset " << name() << ": replace strategy " << (int)replace << " is not supported";
            throw runtime_error(ss.str());
        }
    }
}

void Writer::acquire_batch(WriterBatch& batch, const AcquireConfig& cfg)
{
    acct::acquire_batch_count.incr();
    ReplaceStrategy replace = cfg.replace == REPLACE_DEFAULT ? dataset().default_replace_strategy : cfg.replace;

    if (batch.empty()) return;
    if (batch[0]->md.source().format != dataset().iseg.format)
    {
        batch.set_all_error("cannot acquire into dataset " + name() + ": data is in format " + format_name(batch[0]->md.source().format) + " but the dataset only accepts " + format_name(dataset().iseg.format));
        return;
    }

    segment::data::WriterConfig writer_config;
    writer_config.drop_cached_data_on_commit = cfg.drop_cached_data_on_commit;
    writer_config.eatmydata = dataset().eatmydata;

    std::map<std::string, WriterBatch> by_segment = batch_by_segment(batch);

    // Import segment by segment
    for (auto& s: by_segment)
    {
        auto segment = file(writer_config, s.first);
        switch (replace)
        {
            case REPLACE_NEVER:
                segment->acquire_batch_replace_never(s.second, scache);
                break;
            case REPLACE_ALWAYS:
                segment->acquire_batch_replace_always(s.second, scache);
                break;
            case REPLACE_HIGHER_USN:
                segment->acquire_batch_replace_higher_usn(s.second, scache);
                break;
            default: throw std::runtime_error("programming error: unsupported replace value " + std::to_string(replace));
        }
    }
}

void Writer::test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch)
{
    std::shared_ptr<const iseg::Dataset> dataset(new iseg::Dataset(session, cfg));
    for (auto& e: batch)
    {
        auto age_check = dataset->check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = dataset->name();
            else
                e->dataset_name.clear();
        } else {
            // TODO: check for duplicates
            e->result = ACQ_OK;
            e->dataset_name = dataset->name();
        }
    }
}

}
}
}
