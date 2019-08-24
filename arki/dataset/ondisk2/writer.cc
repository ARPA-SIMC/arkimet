#include "arki/dataset/ondisk2/writer.h"
#include "arki/exceptions.h"
#include "arki/dataset/archive.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/step.h"
#include "arki/dataset/lock.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/scan.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "index.h"
#include <system_error>

using namespace std;
using namespace arki::utils;
using namespace arki::types;
using arki::core::Time;

namespace arki {
namespace dataset {
namespace ondisk2 {

struct AppendSegment
{
    std::shared_ptr<const ondisk2::Config> config;
    std::shared_ptr<dataset::AppendLock> lock;
    index::WIndex idx;
    std::shared_ptr<segment::Writer> segment;

    AppendSegment(std::shared_ptr<const ondisk2::Config> config, std::shared_ptr<dataset::AppendLock> lock, std::shared_ptr<segment::Writer> segment)
        : config(config), lock(lock), idx(config), segment(segment)
    {
        idx.lock = lock;
        idx.open();
    }
    ~AppendSegment()
    {
        idx.flush();
    }

    WriterAcquireResult acquire_replace_never(Metadata& md, bool drop_cached_data_on_commit)
    {
        auto p_idx = idx.begin_transaction();
        try {
            if (std::unique_ptr<types::source::Blob> old = idx.index(md, segment->segment().relpath, segment->next_offset()))
            {
                md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + old->filename + ":" + std::to_string(old->offset));
                return ACQ_ERROR_DUPLICATE;
            }
            segment->append(md, drop_cached_data_on_commit);
            segment->commit();
            p_idx.commit();
            return ACQ_OK;
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            md.add_note("Failed to store in dataset '" + config->name + "': " + e.what());
            return ACQ_ERROR;
        }
    }

    WriterAcquireResult acquire_replace_always(Metadata& md, bool drop_cached_data_on_commit)
    {
        auto p_idx = idx.begin_transaction();
        try {
            idx.replace(md, segment->segment().relpath, segment->next_offset());
            segment->append(md, drop_cached_data_on_commit);
            // In a replace, we necessarily replace inside the same file,
            // as it depends on the metadata reftime
            //createPackFlagfile(df->pathname);
            segment->commit();
            p_idx.commit();
            return ACQ_OK;
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            md.add_note("Failed to store in dataset '" + config->name + "': " + e.what());
            return ACQ_ERROR;
        }
    }

    WriterAcquireResult acquire_replace_higher_usn(Metadata& md, SegmentManager& segs, bool drop_cached_data_on_commit)
    {
        auto p_idx = idx.begin_transaction();

        try {
            // Try to acquire without replacing
            if (std::unique_ptr<types::source::Blob> old = idx.index(md, segment->segment().relpath, segment->next_offset()))
            {
                // Duplicate detected

                // Read the update sequence number of the new BUFR
                int new_usn;
                if (!scan::Scanner::update_sequence_number(md, new_usn))
                    return ACQ_ERROR_DUPLICATE;

                // Read the update sequence number of the old BUFR
                auto reader = segs.get_reader(scan::Scanner::format_from_filename(old->filename), old->filename, lock);
                old->lock(reader);
                int old_usn;
                if (!scan::Scanner::update_sequence_number(*old, old_usn))
                {
                    md.add_note("Failed to store in dataset '" + config->name + "': a similar element exists, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");
                    return ACQ_ERROR;
                }

                // If the new element has no higher Update Sequence Number, report a duplicate
                if (old_usn > new_usn)
                    return ACQ_ERROR_DUPLICATE;

                // Replace, reusing the pending datafile transaction from earlier
                idx.replace(md, segment->segment().relpath, segment->next_offset());
                segment->append(md, drop_cached_data_on_commit);
                segment->commit();
                p_idx.commit();
                return ACQ_OK;
            } else {
                segment->append(md, drop_cached_data_on_commit);
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


    void acquire_batch_replace_never(WriterBatch& batch, bool drop_cached_data_on_commit)
    {
        auto p_idx = idx.begin_transaction();

        try {
            for (auto& e: batch)
            {
                e->dataset_name.clear();

                if (std::unique_ptr<types::source::Blob> old = idx.index(e->md, segment->segment().relpath, segment->next_offset()))
                {
                    e->md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + old->filename + ":" + std::to_string(old->offset));
                    e->result = ACQ_ERROR_DUPLICATE;
                    continue;
                }
                segment->append(e->md, drop_cached_data_on_commit);
                e->result = ACQ_OK;
                e->dataset_name = config->name;
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            batch.set_all_error("Failed to store in dataset '" + config->name + "': " + e.what());
            return;
        }

        segment->commit();
        p_idx.commit();
    }

    void acquire_batch_replace_always(WriterBatch& batch, bool drop_cached_data_on_commit)
    {
        auto p_idx = idx.begin_transaction();

        try {
            for (auto& e: batch)
            {
                e->dataset_name.clear();
                idx.replace(e->md, segment->segment().relpath, segment->next_offset());
                segment->append(e->md, drop_cached_data_on_commit);
                e->result = ACQ_OK;
                e->dataset_name = config->name;
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            batch.set_all_error("Failed to store in dataset '" + config->name + "': " + e.what());
            return;
        }

        segment->commit();
        p_idx.commit();
    }

    void acquire_batch_replace_higher_usn(WriterBatch& batch, SegmentManager& segs, bool drop_cached_data_on_commit)
    {
        auto p_idx = idx.begin_transaction();

        try {
            for (auto& e: batch)
            {
                e->dataset_name.clear();

                // Try to acquire without replacing
                if (std::unique_ptr<types::source::Blob> old = idx.index(e->md, segment->segment().relpath, segment->next_offset()))
                {
                    // Duplicate detected

                    // Read the update sequence number of the new BUFR
                    int new_usn;
                    if (!scan::Scanner::update_sequence_number(e->md, new_usn))
                    {
                        e->md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + segment->segment().relpath + ":" + std::to_string(old->offset) + " and there is no Update Sequence Number to compare");
                        e->result = ACQ_ERROR_DUPLICATE;
                        continue;
                    }

                    // Read the update sequence number of the old BUFR
                    auto reader = segs.get_reader(segment->segment().format, old->filename, lock);
                    old->lock(reader);
                    int old_usn;
                    if (!scan::Scanner::update_sequence_number(*old, old_usn))
                    {
                        e->md.add_note("Failed to store in dataset '" + config->name + "': a similar element exists, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");
                        e->result = ACQ_ERROR;
                        continue;
                    }

                    // If the new element has no higher Update Sequence Number, report a duplicate
                    if (old_usn > new_usn)
                    {
                        e->md.add_note("Failed to store in dataset '" + config->name + "' because the dataset already has the data in " + segment->segment().relpath + ":" + std::to_string(old->offset) + " with a higher Update Sequence Number");
                        e->result = ACQ_ERROR_DUPLICATE;
                        continue;
                    }

                    // Replace, reusing the pending datafile transaction from earlier
                    idx.replace(e->md, segment->segment().relpath, segment->next_offset());
                    segment->append(e->md, drop_cached_data_on_commit);
                    e->result = ACQ_OK;
                    e->dataset_name = config->name;
                    continue;
                } else {
                    segment->append(e->md, drop_cached_data_on_commit);
                    e->result = ACQ_OK;
                    e->dataset_name = config->name;
                }
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            batch.set_all_error("Failed to store in dataset '" + config->name + "': " + e.what());
            return;
        }

        segment->commit();
        p_idx.commit();
    }
};

Writer::Writer(std::shared_ptr<const ondisk2::Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    bool dir_created = sys::makedirs(config->path);

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!dir_created and !sys::exists(config->index_pathname))
        files::createDontpackFlagfile(config->path);
}

Writer::~Writer()
{
    flush();
}

std::string Writer::type() const { return "ondisk2"; }

std::unique_ptr<AppendSegment> Writer::segment(const Metadata& md, const std::string& format)
{
    auto lock = config().append_lock_dataset();
    return std::unique_ptr<AppendSegment>(new AppendSegment(m_config, lock, segmented::Writer::file(md, format)));
}

std::unique_ptr<AppendSegment> Writer::segment(const std::string& relpath)
{
    sys::makedirs(str::dirname(str::joinpath(config().path, relpath)));
    auto lock = config().append_lock_dataset();
    auto segment = segment_manager().get_writer(scan::Scanner::format_from_filename(relpath), relpath);
    return std::unique_ptr<AppendSegment>(new AppendSegment(m_config, lock, segment));
}

WriterAcquireResult Writer::acquire(Metadata& md, const AcquireConfig& cfg)
{
    acct::acquire_single_count.incr();
    auto age_check = config().check_acquire_age(md);
    if (age_check.first) return age_check.second;

    ReplaceStrategy replace = cfg.replace == REPLACE_DEFAULT ? config().default_replace_strategy : cfg.replace;

    auto w = segment(md, md.source().format);

    switch (replace)
    {
        case REPLACE_NEVER: return w->acquire_replace_never(md, cfg.drop_cached_data_on_commit);
        case REPLACE_ALWAYS: return w->acquire_replace_always(md, cfg.drop_cached_data_on_commit);
        case REPLACE_HIGHER_USN: return w->acquire_replace_higher_usn(md, segment_manager(), cfg.drop_cached_data_on_commit);
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
    ReplaceStrategy replace = cfg.replace == REPLACE_DEFAULT ? config().default_replace_strategy : cfg.replace;

    std::map<std::string, WriterBatch> by_segment = batch_by_segment(batch);

    // Import segment by segment
    for (auto& s: by_segment)
    {
        auto seg = segment(s.first);
        switch (replace)
        {
            case REPLACE_NEVER:
                seg->acquire_batch_replace_never(s.second, cfg.drop_cached_data_on_commit);
                break;
            case REPLACE_ALWAYS:
                seg->acquire_batch_replace_always(s.second, cfg.drop_cached_data_on_commit);
                break;
            case REPLACE_HIGHER_USN:
                seg->acquire_batch_replace_higher_usn(s.second, segment_manager(), cfg.drop_cached_data_on_commit);
                break;
            default: throw std::runtime_error("programming error: unsupported replace value " + std::to_string(replace));
        }
    }
}

void Writer::remove(Metadata& md)
{
    const types::source::Blob* source = md.has_source_blob();
    if (!source)
        throw std::runtime_error("cannot remove metadata from dataset, because it has no Blob source");

    if (source->basedir != config().path)
        throw std::runtime_error("cannot remove metadata from dataset: its basedir is " + source->basedir + " but this dataset is at " + config().path);

    // TODO: refuse if md is in the archive

    auto lock = config().append_lock_dataset();

    // Delete from DB, and get file name
    index::WIndex idx(m_config);
    idx.open();
    idx.lock = lock;
    auto p_del = idx.begin_transaction();
    idx.remove(source->filename, source->offset);

    // Create flagfile
    //createPackFlagfile(str::joinpath(config().path, file));

    // Commit delete from DB
    p_del.commit();

    // reset source and dataset in the metadata
    md.unset_source();
    md.unset(TYPE_ASSIGNEDDATASET);
}

void Writer::test_acquire(const core::cfg::Section& cfg, WriterBatch& batch)
{
    ReplaceStrategy replace;
    string repl = cfg.value("replace");
    if (repl == "yes" || repl == "true" || repl == "always")
        replace = REPLACE_ALWAYS;
    else if (repl == "USN")
        replace = REPLACE_HIGHER_USN;
    else if (repl == "" || repl == "no" || repl == "never")
        replace = REPLACE_NEVER;
    else
        throw std::runtime_error("Replace strategy '" + repl + "' is not recognised in dataset configuration");

    // Refuse if md is before "archive age"
    std::shared_ptr<const ondisk2::Config> config(new ondisk2::Config(cfg));
    auto segs = config->create_segment_manager();
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

        if (replace == REPLACE_ALWAYS)
        {
            e->result = ACQ_OK;
            e->dataset_name = config->name;
            continue;
        }

        auto lock = config->read_lock_dataset();

        index::RIndex idx(config);
        if (!idx.exists())
        {
            // If there is no index, there can be no duplicates
            e->result = ACQ_OK;
            e->dataset_name = config->name;
            continue;
        }
        idx.lock = lock;
        idx.open();

        std::unique_ptr<types::source::Blob> old = idx.get_current(e->md);
        if (!old)
        {
            e->result = ACQ_OK;
            e->dataset_name = config->name;
            continue;
        }

        if (replace == REPLACE_NEVER) {
            e->result = ACQ_ERROR_DUPLICATE;
            e->dataset_name.clear();
            continue;
        }

        // We are left with the case of REPLACE_HIGHER_USN

        // Read the update sequence number of the new BUFR
        int new_usn;
        if (!scan::Scanner::update_sequence_number(e->md, new_usn))
        {
            e->result = ACQ_ERROR_DUPLICATE;
            e->dataset_name.clear();
            continue;
        }

        // Read the update sequence number of the old BUFR
        auto reader = segs->get_reader(scan::Scanner::format_from_filename(old->filename), old->filename, lock);
        old->lock(reader);
        int old_usn;
        if (!scan::Scanner::update_sequence_number(*old, old_usn))
        {
            nag::warning("cannot acquire into dataset: insert reported a conflict, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");
            e->result = ACQ_ERROR;
            e->dataset_name.clear();
        } else if (old_usn > new_usn) {
            // If there is no new Update Sequence Number, report a duplicate
            e->result = ACQ_ERROR_DUPLICATE;
            e->dataset_name.clear();
        } else {
            e->result = ACQ_OK;
            e->dataset_name = config->name;
        }
    }
}


}
}
}
