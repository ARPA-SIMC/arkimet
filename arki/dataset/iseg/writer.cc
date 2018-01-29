#include "arki/dataset/iseg/writer.h"
#include "arki/dataset/iseg/index.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/step.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/time.h"
#include "arki/dataset/lock.h"
#include "arki/types/source/blob.h"
#include "arki/reader.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/matcher.h"
#include "arki/metadata/collection.h"
#include "arki/utils/files.h"
#include "arki/scan/any.h"
#include "arki/postprocess.h"
#include "arki/sort.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/utils/compress.h"
#include "arki/binary.h"
#include <system_error>
#include <algorithm>
#include <ctime>
#include <cstring>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

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


namespace {

/// Create unique strings from metadata
struct IDMaker
{
    std::set<types::Code> components;

    IDMaker(const std::set<types::Code>& components) : components(components) {}

    vector<uint8_t> make_string(const Metadata& md) const
    {
        vector<uint8_t> res;
        BinaryEncoder enc(res);
        for (set<types::Code>::const_iterator i = components.begin(); i != components.end(); ++i)
            if (const Type* t = md.get(*i))
                t->encodeBinary(enc);
        return res;
    }
};

}


class CheckerSegment : public segmented::CheckerSegment
{
public:
    Checker& checker;
    CIndex* m_idx = nullptr;

    CheckerSegment(Checker& checker, const std::string& relpath)
        : CheckerSegment(checker, relpath, checker.config().check_lock_segment(relpath)) {}
    CheckerSegment(Checker& checker, const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
        : segmented::CheckerSegment(lock), checker(checker)
    {
        segment = checker.segment_manager().get_checker(checker.config().format, relpath);
    }
    ~CheckerSegment()
    {
        delete m_idx;
    }

    CIndex& idx()
    {
        if (!m_idx)
            m_idx = new CIndex(checker.m_config, path_relative(), lock);
        return *m_idx;
    }
    std::string path_relative() const override { return segment->relname; }
    const iseg::Config& config() const override { return checker.config(); }
    dataset::ArchivesChecker& archives() const { return checker.archive(); }

    segmented::SegmentState scan(dataset::Reporter& reporter, bool quick=true) override
    {
        if (!segment->exists_on_disk())
            return segmented::SegmentState(SEGMENT_MISSING);

        if (!sys::stat(str::joinpath(checker.config().path, segment->relname + ".index")))
        {
            //bool untrusted_index = files::hasDontpackFlagfile(checker.config().path);
            reporter.segment_info(checker.name(), segment->relname, "segment found on disk with no associated index data");
            //return segmented::SegmentState(untrusted_index ? SEGMENT_UNALIGNED : SEGMENT_DELETED);
            return segmented::SegmentState(SEGMENT_UNALIGNED);
        }

#if 0
        /**
         * Although iseg could detect if the data of a segment is newer than its
         * index, the timestamp of the index is updated by various kinds of sqlite
         * operations, making the test rather useless, because it's likely that the
         * index timestamp would get updated before the mismatch is detected.
         */
        string abspath = str::joinpath(config().path, relpath);
        if (sys::timestamp(abspath) > sys::timestamp(abspath + ".index"))
        {
            segments_state.insert(make_pair(relpath, segmented::SegmentState(SEGMENT_UNALIGNED)));
            return;
        }
#endif

        metadata::Collection mds;
        idx().scan(mds.inserter_func(), "reftime, offset");
        segment::State state = SEGMENT_OK;

        // Compute the span of reftimes inside the segment
        unique_ptr<core::Time> md_begin;
        unique_ptr<core::Time> md_until;
        if (mds.empty())
        {
            reporter.segment_info(checker.name(), segment->relname, "index knows of this segment but contains no data for it");
            md_begin.reset(new core::Time(0, 0, 0));
            md_until.reset(new core::Time(0, 0, 0));
            state = SEGMENT_DELETED;
        } else {
            if (!mds.expand_date_range(md_begin, md_until))
            {
                reporter.segment_info(checker.name(), segment->relname, "index data for this segment has no reference time information");
                state = SEGMENT_CORRUPTED;
                md_begin.reset(new core::Time(0, 0, 0));
                md_until.reset(new core::Time(0, 0, 0));
            } else {
                // Ensure that the reftime span fits inside the segment step
                core::Time seg_begin;
                core::Time seg_until;
                if (checker.config().step().path_timespan(segment->relname, seg_begin, seg_until))
                {
                    if (*md_begin < seg_begin || *md_until > seg_until)
                    {
                        reporter.segment_info(checker.name(), segment->relname, "segment contents do not fit inside the step of this dataset");
                        state = SEGMENT_CORRUPTED;
                    }
                    // Expand segment timespan to the full possible segment timespan
                    *md_begin = seg_begin;
                    *md_until = seg_until;
                } else {
                    reporter.segment_info(checker.name(), segment->relname, "segment name does not fit the step of this dataset");
                    state = SEGMENT_CORRUPTED;
                }
            }
        }

        if (state.is_ok())
            state = segment->check(reporter, checker.name(), mds, quick);

        auto res = segmented::SegmentState(state, *md_begin, *md_until);
        res.check_age(segment->relname, checker.config(), reporter);
        return res;
    }

    size_t repack(unsigned test_flags=0) override
    {
        // Lock away writes and reads
        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        metadata::Collection mds;
        idx().scan(mds.inserter_func(), "reftime, offset");

        auto res = reorder_segment_backend(p, mds, test_flags);

        //reporter.operation_progress(checker.name(), "repack", "running VACUUM ANALYZE on segment " + segment->relname);
        idx().vacuum();

        return res;
    }

    size_t reorder(metadata::Collection& mds, unsigned test_flags) override
    {
        // Lock away writes and reads
        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        return reorder_segment_backend(p, mds, test_flags);
    }

    size_t reorder_segment_backend(Pending& p, metadata::Collection& mds, unsigned test_flags)
    {
        // Make a copy of the file with the right data in it, sorted by
        // reftime, and update the offsets in the index
        Pending p_repack = segment->repack(checker.config().path, mds, test_flags);

        // Reindex mds
        idx().reset();
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            if (idx().index(**i, source.offset))
                throw std::runtime_error("duplicate detected while reordering segment");
        }

        size_t size_pre = sys::isdir(segment->absname) ? 0 : sys::size(segment->absname);

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->absname + ".metadata";
        if (sys::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        // Commit the changes in the file system
        p_repack.commit();

        // Commit the changes in the database
        p.commit();

        size_t size_post = sys::isdir(segment->absname) ? 0 : sys::size(segment->absname);

        return size_pre - size_post;
    }

    size_t remove(bool with_data) override
    {
        string idx_abspath = str::joinpath(segment->absname + ".index");
        size_t res = 0;
        if (sys::exists(idx_abspath))
        {
            res = sys::size(idx_abspath);
            sys::unlink(idx_abspath);
        }
        if (!with_data) return res;
        return res + segment->remove();
    }

    void index(metadata::Collection&& mds) override
    {
        // Add to index
        auto write_lock = lock->write_lock();
        Pending p_idx = idx().begin_transaction();
        for (auto& md: mds)
            if (idx().index(*md, md->sourceBlob().offset))
                throw std::runtime_error("duplicate detected while reordering segment");
        p_idx.commit();

        // Remove .metadata and .summary files
        sys::unlink_ifexists(segment->absname + ".metadata");
        sys::unlink_ifexists(segment->absname + ".summary");
    }

    void rescan() override
    {
        // Temporarily uncompress the file for scanning
        unique_ptr<utils::compress::TempUnzip> tu;
        if (scan::isCompressed(segment->absname))
            tu.reset(new utils::compress::TempUnzip(segment->absname));

        // Collect the scan results in a metadata::Collector
        metadata::Collection mds;
        if (!scan::scan(segment->absname, lock, mds.inserter_func()))
            throw std::runtime_error("cannot rescan " + segment->absname + ": file format unknown");
        //fprintf(stderr, "SCANNED %s: %zd\n", pathname.c_str(), mds.size());

        // Lock away writes and reads
        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        // Remove from the index all data about the file
        idx().reset();

        // Scan the list of metadata, looking for duplicates and marking all
        // the duplicates except the last one as deleted
        IDMaker id_maker(idx().unique_codes());

        map<vector<uint8_t>, const Metadata*> finddupes;
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            vector<uint8_t> id = id_maker.make_string(**i);
            if (id.empty())
                continue;
            auto dup = finddupes.find(id);
            if (dup == finddupes.end())
                finddupes.insert(make_pair(id, *i));
            else
                dup->second = *i;
        }
        // cerr << " DUPECHECKED " << pathname << ": " << finddupes.size() << endl;

        // Send the remaining metadata to the reindexer
        std::string basename = str::basename(segment->relname);
        for (const auto& i: finddupes)
        {
            const Metadata& md = *i.second;
            const source::Blob& blob = md.sourceBlob();
            try {
                if (str::basename(blob.filename) != basename)
                    throw std::runtime_error("cannot rescan " + segment->relname + ": metadata points to the wrong file: " + blob.filename);
                if (std::unique_ptr<types::source::Blob> old = idx().index(md, blob.offset))
                {
                    stringstream ss;
                    ss << "cannot reindex " << basename << ": data item at offset " << blob.offset << " has a duplicate at offset " << old->offset << ": manual fix is required";
                    throw runtime_error(ss.str());
                }
            } catch (std::exception& e) {
                stringstream ss;
                ss << "cannot reindex " << basename << ": failed to reindex data item at offset " << blob.offset << ": " << e.what();
                throw runtime_error(ss.str());
                // sqlite will take care of transaction consistency
            }
        }
        // cerr << " REINDEXED " << pathname << endl;

        // TODO: if scan fails, remove all info from the index and rename the
        // file to something like .broken

        // Commit the changes on the database
        p.commit();
        // cerr << " COMMITTED" << endl;

        // TODO: remove relevant summary
    }

    void release(const std::string& destpath)
    {
        sys::unlink_ifexists(str::joinpath(checker.config().path, segment->relname + ".index"));
        segmented::CheckerSegment::release(destpath);
    }
};


Checker::Checker(std::shared_ptr<const iseg::Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

std::string Checker::type() const { return "iseg"; }

void Checker::list_segments(std::function<void(const std::string& relpath)> dest)
{
    list_segments(Matcher(), dest);
}

void Checker::list_segments(const Matcher& matcher, std::function<void(const std::string& relpath)> dest)
{
    vector<string> seg_relpaths;
    config().step().list_segments(config().path, config().format + ".index", matcher, [&](std::string&& s) {
        s.resize(s.size() - 6);
        seg_relpaths.emplace_back(move(s));
    });
    std::sort(seg_relpaths.begin(), seg_relpaths.end());
    for (const auto& relpath: seg_relpaths)
        dest(relpath);
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment(const std::string& relpath)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath));
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath, lock));
}

void Checker::segments(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    segments_filtered(Matcher(), dest);
}

void Checker::segments_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    list_segments(matcher, [&](const std::string& relpath) {
        CheckerSegment segment(*this, relpath);
        dest(segment);
    });
}

void Checker::segments_untracked(std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    segments_untracked_filtered(Matcher(), dest);
}

void Checker::segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    config().step().list_segments(config().path, config().format, matcher, [&](std::string&& relpath) {
        if (sys::stat(str::joinpath(config().path, relpath + ".index"))) return;
        CheckerSegment segment(*this, relpath);
        dest(segment);
    });
}

void Checker::check_issue51(dataset::Reporter& reporter, bool fix)
{
    // Broken metadata for each segment
    std::map<string, metadata::Collection> broken_mds;

    // Iterate all segments
    list_segments([&](const std::string& relpath) {
        auto lock = config().check_lock_segment(relpath);
        CIndex idx(m_config, relpath, lock);
        metadata::Collection mds;
        idx.scan(mds.inserter_func(), "reftime, offset");
        if (mds.empty()) return;
        File datafile(str::joinpath(config().path, relpath), O_RDONLY);
        // Iterate all metadata in the segment
        unsigned count_otherformat = 0;
        unsigned count_ok = 0;
        unsigned count_issue51 = 0;
        unsigned count_corrupted = 0;
        for (const auto& md: mds) {
            const auto& blob = md->sourceBlob();
            // Keep only segments with grib or bufr files
            if (blob.format != "grib" && blob.format != "bufr")
            {
                ++count_otherformat;
                continue;
            }
            // Read the last 4 characters
            char tail[4];
            if (datafile.pread(tail, 4, blob.offset + blob.size - 4) != 4)
            {
                reporter.segment_info(name(), relpath, "cannot read 4 bytes at position " + std::to_string(blob.offset + blob.size - 4));
                return;
            }
            // Check if it ends with 7777
            if (memcmp(tail, "7777", 4) == 0)
            {
                ++count_ok;
                continue;
            }
            // If it instead ends with 777?, take note of it
            if (memcmp(tail, "777", 3) == 0)
            {
                ++count_issue51;
                broken_mds[relpath].push_back(*md);
            } else {
                ++count_corrupted;
                reporter.segment_info(name(), relpath, "end marker 7777 or 777? not found at position " + std::to_string(blob.offset + blob.size - 4));
            }
        }
        nag::verbose("Checked %s:%s: %u ok, %u other formats, %u issue51, %u corrupted", name().c_str(), relpath.c_str(), count_ok, count_otherformat, count_issue51, count_corrupted);
    });

    if (!fix)
    {
        for (const auto& i: broken_mds)
            reporter.segment_issue51(name(), i.first, "segment contains data with corrupted terminator signature");
    } else {
        for (const auto& i: broken_mds)
        {
            // Make a backup copy with .issue51 extension, if it doesn't already exist
            std::string abspath = str::joinpath(config().path, i.first);
            std::string backup = abspath + ".issue51";
            if (!sys::exists(backup))
            {
                File src(abspath, O_RDONLY);
                File dst(backup, O_WRONLY | O_CREAT | O_EXCL, 0666);
                std::array<char, 40960> buffer;
                while (true)
                {
                    size_t sz = src.read(buffer.data(), buffer.size());
                    if (!sz) break;
                    dst.write_all_or_throw(buffer.data(), sz);
                }
            }

            // Fix the file
            File datafile(abspath, O_RDWR);
            for (const auto& md: i.second) {
                const auto& blob = md->sourceBlob();
                // Keep only segments with grib or bufr files
                if (blob.format != "grib" && blob.format != "bufr")
                    return;
                datafile.pwrite("7", 1, blob.offset + blob.size - 1);
            }
            reporter.segment_issue51(name(), i.first, "fixed corrupted terminator signatures");
        }
    }

    return segmented::Checker::check_issue51(reporter, fix);
}

size_t Checker::vacuum(dataset::Reporter& reporter)
{
    return 0;
}

void Checker::test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_config, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(mds.inserter_func());
    segment_manager().get_checker(relpath)->test_make_overlap(mds, overlap_size, data_idx);
    idx.test_make_overlap(overlap_size, data_idx);
}

void Checker::test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_config, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(mds.inserter_func());
    segment_manager().get_checker(relpath)->test_make_hole(mds, hole_size, data_idx);
    idx.test_make_hole(hole_size, data_idx);
}

void Checker::test_corrupt_data(const std::string& relpath, unsigned data_idx)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_config, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(mds.inserter_func());
    segment_manager().get_checker(relpath)->test_corrupt(mds, data_idx);
}

void Checker::test_truncate_data(const std::string& relpath, unsigned data_idx)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_config, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(mds.inserter_func());
    segment_manager().get_checker(relpath)->test_truncate(mds, data_idx);
}

void Checker::test_swap_data(const std::string& relpath, unsigned d1_idx, unsigned d2_idx)
{
    auto lock = config().check_lock_segment(relpath);
    metadata::Collection mds;
    {
        CIndex idx(m_config, relpath, lock);
        idx.scan(mds.inserter_func(), "offset");
        std::swap(mds[d1_idx], mds[d2_idx]);
    }
    segment_prelocked(relpath, lock)->reorder(mds);
}

void Checker::test_rename(const std::string& relpath, const std::string& new_relpath)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    string abspath = str::joinpath(config().path, relpath);
    string new_abspath = str::joinpath(config().path, new_relpath);
    sys::rename(abspath, new_abspath);
    sys::rename(abspath + ".index", new_abspath + ".index");
}

void Checker::test_change_metadata(const std::string& relpath, Metadata& md, unsigned data_idx)
{
    auto lock = config().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_config, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(mds.inserter_func());
    md.set_source(std::unique_ptr<arki::types::Source>(mds[data_idx].source().clone()));
    md.sourceBlob().unlock();
    mds[data_idx] = md;

    // Reindex mds
    idx.reset();
    for (auto& m: mds)
    {
        const source::Blob& source = m->sourceBlob();
        if (idx.index(*m, source.offset))
            throw std::runtime_error("duplicate detected in test_change_metadata");
    }

    md = mds[data_idx];
}

void Checker::test_remove_index(const std::string& relpath)
{
    sys::unlink_ifexists(str::joinpath(config().path, relpath + ".index"));
}

}
}
}
