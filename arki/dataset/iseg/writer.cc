#include "arki/dataset/iseg/writer.h"
#include "arki/dataset/iseg/index.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/step.h"
#include "arki/dataset/time.h"
#include "arki/dataset/lock.h"
#include "arki/types/source/blob.h"
#include "arki/reader.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/utils/files.h"
#include "arki/scan/any.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/utils/compress.h"
#include <system_error>
#include <algorithm>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace iseg {

struct AppendSegment
{
    std::shared_ptr<const iseg::Config> config;
    std::shared_ptr<dataset::AppendLock> append_lock;
    std::shared_ptr<segment::Writer> segment;
    AIndex idx;

    AppendSegment(std::shared_ptr<const iseg::Config> config, std::shared_ptr<dataset::AppendLock> append_lock, std::shared_ptr<segment::Writer> segment)
        : config(config), append_lock(append_lock), segment(segment), idx(config, segment, append_lock)
    {
    }

    WriterAcquireResult acquire_replace_never(Metadata& md, index::SummaryCache& scache)
    {
        Pending p_idx = idx.begin_transaction();

        try {
            if (std::unique_ptr<types::source::Blob> old = idx.index(md, segment->next_offset()))
            {
                md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + segment->relname + ":" + std::to_string(old->offset));
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
            md.add_note("Failed to store in dataset '" + config->name + "': " + e.what());
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
            md.add_note("Failed to store in dataset '" + config->name + "': " + e.what());
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
                if (!scan::update_sequence_number(md, new_usn))
                    return ACQ_ERROR_DUPLICATE;

                // Read the update sequence number of the old BUFR
                auto reader = arki::Reader::create_new(old->absolutePathname(), append_lock);
                old->lock(reader);
                int old_usn;
                if (!scan::update_sequence_number(*old, old_usn))
                {
                    md.add_note("Failed to store in dataset '" + config->name + "': a similar element exists, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");
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
            md.add_note("Failed to store in dataset '" + config->name + "': " + e.what());
            return ACQ_ERROR;
        }
    }

    void acquire_batch_replace_never(std::vector<std::shared_ptr<WriterBatchElement>>& batch, index::SummaryCache& scache)
    {
        Pending p_idx = idx.begin_transaction();

        for (auto& e: batch)
        {
            e->dataset_name.clear();

            if (std::unique_ptr<types::source::Blob> old = idx.index(e->md, segment->next_offset()))
            {
                e->md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + segment->relname + ":" + std::to_string(old->offset));
                e->result = ACQ_ERROR_DUPLICATE;
                continue;
            }

            // Invalidate the summary cache for this month
            scache.invalidate(e->md);
            segment->append(e->md);
            e->result = ACQ_OK;
            e->dataset_name = config->name;
        }

        segment->commit();
        p_idx.commit();
    }

    void acquire_batch_replace_always(std::vector<std::shared_ptr<WriterBatchElement>>& batch, index::SummaryCache& scache)
    {
        Pending p_idx = idx.begin_transaction();

        for (auto& e: batch)
        {
            e->dataset_name.clear();
            idx.replace(e->md, segment->next_offset());
            // Invalidate the summary cache for this month
            scache.invalidate(e->md);
            segment->append(e->md);
            e->result = ACQ_OK;
            e->dataset_name = config->name;
        }

        segment->commit();
        p_idx.commit();
    }

    void acquire_batch_replace_higher_usn(std::vector<std::shared_ptr<WriterBatchElement>>& batch, index::SummaryCache& scache)
    {
        Pending p_idx = idx.begin_transaction();

        for (auto& e: batch)
        {
            e->dataset_name.clear();

            // Try to acquire without replacing
            if (std::unique_ptr<types::source::Blob> old = idx.index(e->md, segment->next_offset()))
            {
                // Duplicate detected

                // Read the update sequence number of the new BUFR
                int new_usn;
                if (!scan::update_sequence_number(e->md, new_usn))
                {
                    e->md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + segment->relname + ":" + std::to_string(old->offset) + " and there is no Update Sequence Number to compare");
                    e->result = ACQ_ERROR_DUPLICATE;
                    continue;
                }

                // Read the update sequence number of the old BUFR
                auto reader = arki::Reader::create_new(old->absolutePathname(), append_lock);
                old->lock(reader);
                int old_usn;
                if (!scan::update_sequence_number(*old, old_usn))
                {
                    e->md.add_note("Failed to store in dataset '" + config->name + "': a similar element exists, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");
                    e->result = ACQ_ERROR;
                    continue;
                }

                // If the new element has no higher Update Sequence Number, report a duplicate
                if (old_usn > new_usn)
                {
                    e->md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + segment->relname + ":" + std::to_string(old->offset) + " with a higher Update Sequence Number");
                    e->result = ACQ_ERROR_DUPLICATE;
                    continue;
                }

                // Replace, reusing the pending datafile transaction from earlier
                idx.replace(e->md, segment->next_offset());
                segment->append(e->md);
                e->result = ACQ_OK;
                e->dataset_name = config->name;
                continue;
            } else {
                // Invalidate the summary cache for this month
                scache.invalidate(e->md);
                segment->append(e->md);
                e->result = ACQ_OK;
                e->dataset_name = config->name;
            }
        }

        segment->commit();
        p_idx.commit();
    }
};


Writer::Writer(std::shared_ptr<const iseg::Config> config)
    : m_config(config), scache(config->summary_cache_pathname)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
    scache.openRW();
}

Writer::~Writer()
{
    flush();
}

std::string Writer::type() const { return "iseg"; }

std::unique_ptr<AppendSegment> Writer::file(const Metadata& md)
{
    const core::Time& time = md.get<types::reftime::Position>()->time;
    string relname = config().step()(time) + "." + config().format;
    return file(relname);
}

std::unique_ptr<AppendSegment> Writer::file(const std::string& relname)
{
    sys::makedirs(str::dirname(str::joinpath(config().path, relname)));
    std::shared_ptr<dataset::AppendLock> append_lock(config().append_lock_segment(relname));
    auto segment = segment_manager().get_writer(config().format, relname);
    return std::unique_ptr<AppendSegment>(new AppendSegment(m_config, append_lock, segment));
}

WriterAcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    if (md.source().format != config().format)
        throw std::runtime_error("cannot acquire into dataset " + name() + ": data is in format " + md.source().format + " but the dataset only accepts " + config().format);

    auto age_check = config().check_acquire_age(md);
    if (age_check.first) return age_check.second;

    if (replace == REPLACE_DEFAULT) replace = config().default_replace_strategy;

    auto segment = file(md);
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

void Writer::acquire_batch(std::vector<std::shared_ptr<WriterBatchElement>>& batch, ReplaceStrategy replace)
{
    if (replace == REPLACE_DEFAULT) replace = config().default_replace_strategy;

    // Clear dataset names, pre-process items that do not need further
    // dispatching, and divide the rest of the batch by segment
    std::map<std::string, std::vector<std::shared_ptr<dataset::WriterBatchElement>>> by_segment;
    for (auto& e: batch)
    {
        e->dataset_name.clear();

        switch (replace)
        {
            case REPLACE_NEVER:
            case REPLACE_ALWAYS:
            case REPLACE_HIGHER_USN: break;
            default:
            {
                e->md.add_note("cannot acquire into dataset " + name() + ": replace strategy " + std::to_string(replace) + " is not supported");
                e->result = ACQ_ERROR;
                continue;
            }
        }

        if (e->md.source().format != config().format)
        {
            e->md.add_note("cannot acquire into dataset " + name() + ": data is in format " + e->md.source().format + " but the dataset only accepts " + config().format);
            e->result = ACQ_ERROR;
            continue;
        }

        auto age_check = config().check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = name();
            continue;
        }

        const core::Time& time = e->md.get<types::reftime::Position>()->time;
        string relname = config().step()(time) + "." + config().format;
        by_segment[relname].push_back(e);
    }

    // Import segment by segment
    for (auto& s: by_segment)
    {
        auto segment = file(s.first);
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
            default: throw std::runtime_error("programmign error: replace value has changed since previous check");
        }
    }
}

void Writer::remove(Metadata& md)
{
    auto segment = file(md);

    const types::source::Blob* source = md.has_source_blob();
    if (!source)
        throw std::runtime_error("cannot remove metadata from dataset, because it has no Blob source");

    if (source->basedir != config().path)
        throw std::runtime_error("cannot remove metadata from dataset: its basedir is " + source->basedir + " but this dataset is at " + config().path);

    // TODO: refuse if md is in the archive

    // Delete from DB, and get file name
    Pending p_del = segment->idx.begin_transaction();
    segment->idx.remove(source->offset);

    // Commit delete from DB
    p_del.commit();

    // reset source and dataset in the metadata
    md.unset_source();
    md.unset(TYPE_ASSIGNEDDATASET);

    scache.invalidate(md);
}

void Writer::test_acquire(const ConfigFile& cfg, std::vector<std::shared_ptr<WriterBatchElement>>& batch, std::ostream& out)
{
    std::shared_ptr<const iseg::Config> config(new iseg::Config(cfg));
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
        } else {
            // TODO: check for duplicates
            e->result = ACQ_OK;
            e->dataset_name = config->name;
        }
    }
}

}
}
}
