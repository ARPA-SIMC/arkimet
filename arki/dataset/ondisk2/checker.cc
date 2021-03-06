#include "arki/dataset/ondisk2/checker.h"
#include "arki/core/binary.h"
#include "arki/dataset/session.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/step.h"
#include "arki/dataset/lock.h"
#include "arki/dataset/archive.h"
#include "arki/types/source/blob.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/utils/files.h"
#include "arki/summary.h"
#include "arki/scan.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <system_error>
#include <unistd.h>
#include <cerrno>

using namespace std;
using namespace arki::utils;
using namespace arki::types;
using arki::core::Time;

namespace arki {
namespace dataset {
namespace ondisk2 {

namespace {

/// Create unique strings from metadata
struct IDMaker
{
    std::set<types::Code> components;

    IDMaker(const std::set<types::Code>& components) : components(components) {}

    vector<uint8_t> make_string(const Metadata& md) const
    {
        vector<uint8_t> res;
        core::BinaryEncoder enc(res);
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

    CheckerSegment(Checker& checker, const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
        : segmented::CheckerSegment(lock), checker(checker)
    {
        segment = checker.dataset().session->segment_checker(scan::Scanner::format_from_filename(relpath), checker.dataset().path, relpath);
    }

    std::string path_relative() const override { return segment->segment().relpath; }
    const ondisk2::Dataset& dataset() const override { return checker.dataset(); }
    ondisk2::Dataset& dataset() override { return checker.dataset(); }
    std::shared_ptr<dataset::archive::Checker> archives() override { return dynamic_pointer_cast<dataset::archive::Checker>(checker.archive()); }

    void get_metadata(std::shared_ptr<core::Lock> lock, metadata::Collection& mds) override
    {
        checker.idx->scan_file(segment->segment().relpath, mds.inserter_func(), "reftime, offset");
    }

    segmented::SegmentState scan(dataset::Reporter& reporter, bool quick=true) override
    {
        if (!segment->exists_on_disk())
            return segmented::SegmentState(segment::SEGMENT_MISSING);

        if (!checker.m_idx->has_segment(segment->segment().relpath))
        {
            if (segment->is_empty())
            {
                reporter.segment_info(checker.name(), segment->segment().relpath, "empty segment found on disk with no associated index data");
                return segmented::SegmentState(segment::SEGMENT_DELETED);
            } else {
                bool untrusted_index = files::hasDontpackFlagfile(checker.dataset().path);
                reporter.segment_info(checker.name(), segment->segment().relpath, "segment found on disk with no associated index data");
                return segmented::SegmentState(untrusted_index ? segment::SEGMENT_UNALIGNED : segment::SEGMENT_DELETED);
            }
        }

        metadata::Collection mds;
        checker.idx->scan_file(segment->segment().relpath, mds.inserter_func(), "m.file, m.reftime, m.offset");

        segment::State state = segment::SEGMENT_OK;
        bool untrusted_index = files::hasDontpackFlagfile(checker.dataset().path);

        // Compute the span of reftimes inside the segment
        core::Interval segment_interval;
        if (mds.empty())
        {
            reporter.segment_info(checker.name(), segment->segment().relpath, "index knows of this segment but contains no data for it");
            state = untrusted_index ? segment::SEGMENT_UNALIGNED : segment::SEGMENT_DELETED;
        } else {
            if (!mds.expand_date_range(segment_interval))
            {
                reporter.segment_info(checker.name(), segment->segment().relpath, "index data for this segment has no reference time information");
                state = segment::SEGMENT_CORRUPTED;
            } else {
                // Ensure that the reftime span fits inside the segment step
                core::Interval interval;
                if (checker.dataset().step().path_timespan(segment->segment().relpath, interval))
                {
                    if (segment_interval.begin < interval.begin || segment_interval.end > interval.end)
                    {
                        reporter.segment_info(checker.name(), segment->segment().relpath, "segment contents do not fit inside the step of this dataset");
                        state = segment::SEGMENT_CORRUPTED;
                    }
                    // Expand segment timespan to the full possible segment timespan
                    segment_interval = interval;
                } else {
                    reporter.segment_info(checker.name(), segment->segment().relpath, "segment name does not fit the step of this dataset");
                    state = segment::SEGMENT_CORRUPTED;
                }
            }
        }

        if (state.is_ok())
            state = segment->check([&](const std::string& msg) { reporter.segment_info(checker.name(), segment->segment().relpath, msg); }, mds, quick);

        // Scenario: the index has been deleted, and some data has been imported
        // and appended to an existing segment, recreating an empty index.
        // That segment would show in the index as DIRTY, because it has a gap of
        // data not indexed. Since the needs-check-do-not-pack file is present,
        // however, mark that file for rescanning instead of repacking.
        if (untrusted_index && state.has(segment::SEGMENT_DIRTY))
            state = state - segment::SEGMENT_DIRTY + segment::SEGMENT_UNALIGNED;

        auto res = segmented::SegmentState(state, segment_interval);
        res.check_age(segment->segment().relpath, checker.dataset(), reporter);
        return res;
    }

    size_t repack(unsigned test_flags) override
    {
        auto lock = checker.lock->write_lock();
        metadata::Collection mds;
        checker.idx->scan_file(segment->segment().relpath, mds.inserter_func(), "reftime, offset");
        return reorder(mds, test_flags);
    }

    size_t reorder(metadata::Collection& mds, unsigned test_flags) override
    {
        // Lock away writes and reads
        auto p = checker.idx->begin_transaction();

        segment::RepackConfig repack_config;
        repack_config.gz_group_size = dataset().gz_group_size;
        repack_config.test_flags = test_flags;
        auto p_repack = segment->repack(checker.dataset().path, mds, repack_config);

        // Reindex mds
        checker.idx->reset(segment->segment().relpath);
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            checker.idx->index(**i, source.filename, source.offset);
        }

        size_t size_pre = segment->size();

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->segment().abspath + ".metadata";
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

        size_t size_post = segment->size();

        return size_pre - size_post;
    }

    void tar() override
    {
        if (sys::exists(segment->segment().abspath + ".tar"))
            return;

        auto lock = checker.lock->write_lock();
        auto p = checker.idx->begin_transaction();

        // Rescan file
        metadata::Collection mds;
        checker.idx->scan_file(segment->segment().relpath, mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        segment = segment->tar(mds);

        // Reindex the new metadata
        checker.idx->reset(segment->segment().relpath);
        for (auto& md: mds)
        {
            const source::Blob& source = md->sourceBlob();
            checker.idx->index(*md, segment->segment().relpath, source.offset);
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->segment().abspath + ".metadata";
        if (sys::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        p.commit();
    }

    void zip() override
    {
        if (sys::exists(segment->segment().abspath + ".zip"))
            return;

        auto lock = checker.lock->write_lock();
        auto p = checker.idx->begin_transaction();

        // Rescan file
        metadata::Collection mds;
        checker.idx->scan_file(segment->segment().relpath, mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        segment = segment->zip(mds);

        // Reindex the new metadata
        checker.idx->reset(segment->segment().relpath);
        for (auto& md: mds)
        {
            const source::Blob& source = md->sourceBlob();
            checker.idx->index(*md, segment->segment().relpath, source.offset);
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->segment().abspath + ".metadata";
        if (sys::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        p.commit();
    }

    size_t compress(unsigned groupsize) override
    {
        if (sys::exists(segment->segment().abspath + ".gz") || sys::exists(segment->segment().abspath + ".gz.idx"))
            return 0;

        auto lock = checker.lock->write_lock();
        auto p = checker.idx->begin_transaction();

        // Rescan file
        metadata::Collection mds;
        checker.idx->scan_file(segment->segment().relpath, mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        size_t old_size = segment->size();
        segment = segment->compress(mds, groupsize);
        size_t new_size = segment->size();

        // Reindex the new metadata
        checker.idx->reset(segment->segment().relpath);
        for (auto& md: mds)
        {
            const source::Blob& source = md->sourceBlob();
            checker.idx->index(*md, segment->segment().relpath, source.offset);
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->segment().abspath + ".metadata";
        if (sys::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        p.commit();

        if (old_size > new_size)
            return old_size - new_size;
        else
            return 0;
    }

    size_t remove(bool with_data) override
    {
        checker.idx->reset(segment->segment().relpath);
        // TODO: also remove .metadata and .summary files
        if (!with_data) return 0;
        return segment->remove();
    }

    void rescan(dataset::Reporter& reporter) override
    {
        metadata::Collection mds;
        segment->rescan_data(
                [&](const std::string& msg) { reporter.segment_info(checker.name(), segment->segment().relpath, msg); },
                lock, mds.inserter_func());

        // Lock away writes and reads
        auto p = checker.idx->begin_transaction();

        // Remove from the index all data about the file
        checker.idx->reset(segment->segment().relpath);

        // Scan the list of metadata, looking for duplicates and marking all
        // the duplicates except the last one as deleted
        IDMaker id_maker(checker.idx->unique_codes());

        map<vector<uint8_t>, const Metadata*> finddupes;
        for (const auto& md: mds)
        {
            vector<uint8_t> id = id_maker.make_string(*md);
            if (id.empty())
                continue;
            auto dup = finddupes.find(id);
            if (dup == finddupes.end())
                finddupes.insert(make_pair(id, md.get()));
            else
                dup->second = md.get();
        }

        // Send the remaining metadata to the reindexer
        std::string basename = str::basename(segment->segment().relpath);
        for (const auto& i: finddupes)
        {
            const Metadata& md = *i.second;
            const source::Blob& blob = md.sourceBlob();
            try {
                if (str::basename(blob.filename) != basename)
                    throw std::runtime_error("cannot rescan " + segment->segment().relpath + ": metadata points to the wrong file: " + blob.filename);
                if (std::unique_ptr<types::source::Blob> old = checker.idx->index(md, segment->segment().relpath, blob.offset))
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

        // TODO: if scan fails, remove all info from the index and rename the
        // file to something like .broken

        // Commit the changes on the database
        p.commit();

        // TODO: remove relevant summary
    }

    void index(metadata::Collection&& contents) override
    {
        // Add to index
        auto p_idx = checker.idx->begin_transaction();
        for (auto& md: contents)
            if (checker.idx->index(*md, segment->segment().relpath, md->sourceBlob().offset))
                throw std::runtime_error("duplicate detected while reordering segment");
        p_idx.commit();

        // Remove .metadata and .summary files
        sys::unlink_ifexists(segment->segment().abspath + ".metadata");
        sys::unlink_ifexists(segment->segment().abspath + ".summary");
    }

    void release(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) override
    {
        // Rebuild the metadata
        metadata::Collection mds;
        checker.idx->scan_file(segment->segment().relpath, mds.inserter_func());
        mds.writeAtomically(segment->segment().abspath + ".metadata");

        // Remove from index
        checker.idx->reset(segment->segment().relpath);

        segment = segment->move(new_root, new_relpath, new_abspath);
    }
};


Checker::Checker(std::shared_ptr<ondisk2::Dataset> dataset)
    : DatasetAccess(dataset), idx(new index::WIndex(dataset))
{
    m_idx = idx;

    // Create the directory if it does not exist
    bool dir_created = sys::makedirs(dataset->path);

    lock = dataset->check_lock_dataset();
    m_idx->lock = lock;

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!dir_created && !sys::exists(dataset->index_pathname))
        files::createDontpackFlagfile(dataset->path);

    idx->open();
}

Checker::~Checker()
{
}

std::string Checker::type() const { return "ondisk2"; }

void Checker::check(CheckerConfig& opts)
{
    indexed::Checker::check(opts);

    if (!idx->checkSummaryCache(*this, *opts.reporter) && !opts.readonly)
    {
        opts.reporter->operation_progress(name(), "check", "rebuilding summary cache");
        idx->rebuildSummaryCache();
    }
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment(const std::string& relpath)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath, lock));
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment_prelocked(const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath, lock));
}

void Checker::segments_tracked(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    m_idx->list_segments([&](const std::string& relpath) {
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    });
}

void Checker::segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    m_idx->list_segments_filtered(matcher, [&](const std::string& relpath) {
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    });
}

void Checker::segments_untracked(std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    scan_dir(dataset().path, [&](const std::string& relpath) {
        if (m_idx->has_segment(relpath)) return;
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    });
}

void Checker::segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    if (matcher.empty()) return segments_untracked(dest);
    auto m = matcher.get(TYPE_REFTIME);
    if (!m) return segments_untracked(dest);

    scan_dir(dataset().path, [&](const std::string& relpath) {
        if (m_idx->has_segment(relpath)) return;
        if (!dataset().step().pathMatches(relpath, *m)) return;
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    });
}


size_t Checker::vacuum(dataset::Reporter& reporter)
{
    reporter.operation_progress(name(), "repack", "running VACUUM ANALYZE on the dataset index");
    size_t size_pre = 0, size_post = 0;
    if (sys::size(idx->pathname(), 0) > 0)
    {
        size_pre = sys::size(idx->pathname(), 0)
                 + sys::size(idx->pathname() + "-journal", 0);
        idx->vacuum();
        size_post = sys::size(idx->pathname(), 0)
                  + sys::size(idx->pathname() + "-journal", 0);
    }

    // Rebuild the cached summaries, if needed
    if (!sys::exists(str::joinpath(dataset().path, ".summaries/all.summary")))
    {
        reporter.operation_progress(name(), "repack", "rebuilding the summary cache");
        Summary s;
        idx->summaryForAll(s);
    }

    return size_pre > size_post ? size_pre - size_post : 0;
}

std::shared_ptr<Metadata> Checker::test_change_metadata(const std::string& relpath, std::shared_ptr<Metadata> md, unsigned data_idx)
{
    metadata::Collection mds;
    idx->query_segment(relpath, mds.inserter_func());
    md->set_source(std::unique_ptr<arki::types::Source>(mds[data_idx].source().clone()));
    md->sourceBlob().unlock();
    mds.replace(data_idx, md);

    // Reindex mds
    idx->reset(relpath);
    for (auto& m: mds)
    {
        const source::Blob& source = m->sourceBlob();
        idx->index(*m, source.filename, source.offset);
    }

    return mds.get(data_idx);
}

void Checker::test_delete_from_index(const std::string& relpath)
{
    m_idx->test_deindex(relpath);
}

void Checker::test_invalidate_in_index(const std::string& relpath)
{
    m_idx->test_deindex(relpath);
    files::createDontpackFlagfile(dataset().path);
}


}
}
}

