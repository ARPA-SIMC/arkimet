#include "arki/dataset/ondisk2/checker.h"
#include "arki/exceptions.h"
#include "arki/binary.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/archive.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/step.h"
#include "arki/dataset/lock.h"
#include "arki/reader.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/scan/any.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/compress.h"
#include "arki/summary.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <system_error>
#include <sstream>
#include <unistd.h>
#include <cerrno>
#include <cassert>

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

    CheckerSegment(Checker& checker, const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
        : segmented::CheckerSegment(lock), checker(checker)
    {
        segment = checker.segment_manager().get_checker(relpath);
    }

    std::string path_relative() const override { return segment->relpath; }
    const ondisk2::Config& config() const override { return checker.config(); }
    dataset::ArchivesChecker& archives() const { return checker.archive(); }

    void get_metadata(std::shared_ptr<core::Lock> lock, metadata::Collection& mds) override
    {
        checker.idx->scan_file(segment->relpath, mds.inserter_func(), "reftime, offset");
    }

    segmented::SegmentState scan(dataset::Reporter& reporter, bool quick=true) override
    {
        if (!segment->exists_on_disk())
            return segmented::SegmentState(segment::SEGMENT_MISSING);

        if (!checker.m_idx->has_segment(segment->relpath))
        {
            bool untrusted_index = files::hasDontpackFlagfile(checker.config().path);
            reporter.segment_info(checker.name(), segment->relpath, "segment found on disk with no associated index data");
            return segmented::SegmentState(untrusted_index ? segment::SEGMENT_UNALIGNED : segment::SEGMENT_DELETED);
        }

        metadata::Collection mds;
        checker.idx->scan_file(segment->relpath, mds.inserter_func(), "m.file, m.reftime, m.offset");

        segment::State state = segment::SEGMENT_OK;
        bool untrusted_index = files::hasDontpackFlagfile(checker.config().path);

        // Compute the span of reftimes inside the segment
        unique_ptr<Time> md_begin;
        unique_ptr<Time> md_until;
        if (mds.empty())
        {
            reporter.segment_info(checker.name(), segment->relpath, "index knows of this segment but contains no data for it");
            md_begin.reset(new Time(0, 0, 0));
            md_until.reset(new Time(0, 0, 0));
            state = untrusted_index ? segment::SEGMENT_UNALIGNED : segment::SEGMENT_DELETED;
        } else {
            if (!mds.expand_date_range(md_begin, md_until))
            {
                reporter.segment_info(checker.name(), segment->relpath, "index data for this segment has no reference time information");
                state = segment::SEGMENT_CORRUPTED;
                md_begin.reset(new Time(0, 0, 0));
                md_until.reset(new Time(0, 0, 0));
            } else {
                // Ensure that the reftime span fits inside the segment step
                Time seg_begin;
                Time seg_until;
                if (checker.config().step().path_timespan(segment->relpath, seg_begin, seg_until))
                {
                    if (*md_begin < seg_begin || *md_until > seg_until)
                    {
                        reporter.segment_info(checker.name(), segment->relpath, "segment contents do not fit inside the step of this dataset");
                        state = segment::SEGMENT_CORRUPTED;
                    }
                    // Expand segment timespan to the full possible segment timespan
                    *md_begin = seg_begin;
                    *md_until = seg_until;
                } else {
                    reporter.segment_info(checker.name(), segment->relpath, "segment name does not fit the step of this dataset");
                    state = segment::SEGMENT_CORRUPTED;
                }
            }
        }

        if (state.is_ok())
            state = segment->check([&](const std::string& msg) { reporter.segment_info(checker.name(), segment->relpath, msg); }, mds, quick);

        // Scenario: the index has been deleted, and some data has been imported
        // and appended to an existing segment, recreating an empty index.
        // That segment would show in the index as DIRTY, because it has a gap of
        // data not indexed. Since the needs-check-do-not-pack file is present,
        // however, mark that file for rescanning instead of repacking.
        if (untrusted_index && state.has(segment::SEGMENT_DIRTY))
            state = state - segment::SEGMENT_DIRTY + segment::SEGMENT_UNALIGNED;

        auto res = segmented::SegmentState(state, *md_begin, *md_until);
        res.check_age(segment->relpath, checker.config(), reporter);
        return res;
    }

    size_t repack(unsigned test_flags) override
    {
        auto lock = checker.lock->write_lock();
        metadata::Collection mds;
        checker.idx->scan_file(segment->relpath, mds.inserter_func(), "reftime, offset");
        return reorder(mds, test_flags);
    }

    size_t reorder(metadata::Collection& mds, unsigned test_flags) override
    {
        // Lock away writes and reads
        Pending p = checker.idx->begin_transaction();

        Pending p_repack = segment->repack(checker.config().path, mds, test_flags);

        // Reindex mds
        checker.idx->reset(segment->relpath);
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            checker.idx->index(**i, source.filename, source.offset);
        }

        size_t size_pre = sys::isdir(segment->abspath) ? 0 : sys::size(segment->abspath);

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->abspath + ".metadata";
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

        size_t size_post = sys::isdir(segment->abspath) ? 0 : sys::size(segment->abspath);

        return size_pre - size_post;
    }

    void tar() override
    {
        if (sys::exists(segment->abspath + ".tar"))
            return;

        auto lock = checker.lock->write_lock();
        Pending p = checker.idx->begin_transaction();

        // Rescan file
        metadata::Collection mds;
        checker.idx->scan_file(segment->relpath, mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        segment = segment->tar(mds);

        // Reindex the new metadata
        checker.idx->reset(segment->relpath);
        for (auto& md: mds)
        {
            const source::Blob& source = md->sourceBlob();
            checker.idx->index(*md, segment->relpath, source.offset);
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->abspath + ".metadata";
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
        if (sys::exists(segment->abspath + ".zip"))
            return;

        auto lock = checker.lock->write_lock();
        Pending p = checker.idx->begin_transaction();

        // Rescan file
        metadata::Collection mds;
        checker.idx->scan_file(segment->relpath, mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        segment = segment->zip(mds);

        // Reindex the new metadata
        checker.idx->reset(segment->relpath);
        for (auto& md: mds)
        {
            const source::Blob& source = md->sourceBlob();
            checker.idx->index(*md, segment->relpath, source.offset);
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->abspath + ".metadata";
        if (sys::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        p.commit();
    }

    size_t compress() override
    {
        if (sys::exists(segment->abspath + ".gz") || sys::exists(segment->abspath + ".gz.idx"))
            return 0;

        auto lock = checker.lock->write_lock();
        Pending p = checker.idx->begin_transaction();

        // Rescan file
        metadata::Collection mds;
        checker.idx->scan_file(segment->relpath, mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        size_t old_size = segment->size();
        segment = segment->compress(mds);
        size_t new_size = segment->size();

        // Reindex the new metadata
        checker.idx->reset(segment->relpath);
        for (auto& md: mds)
        {
            const source::Blob& source = md->sourceBlob();
            checker.idx->index(*md, segment->relpath, source.offset);
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        string mdpathname = segment->abspath + ".metadata";
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
        checker.idx->reset(segment->relpath);
        // TODO: also remove .metadata and .summary files
        if (!with_data) return 0;
        return segment->remove();
    }

    void rescan() override
    {
        // Temporarily uncompress the file for scanning
        unique_ptr<utils::compress::TempUnzip> tu;
        if (scan::isCompressed(segment->abspath))
            tu.reset(new utils::compress::TempUnzip(segment->abspath));

        // Collect the scan results in a metadata::Collector
        metadata::Collection mds;
        if (!scan::scan(segment->abspath, lock, mds.inserter_func()))
            throw std::runtime_error("cannot rescan " + segment->abspath + ": file format unknown");
        //fprintf(stderr, "SCANNED %s: %zd\n", pathname.c_str(), mds.size());

        // Lock away writes and reads
        Pending p = checker.idx->begin_transaction();
        // cerr << "LOCK" << endl;

        // Remove from the index all data about the file
        checker.idx->reset(segment->relpath);
        // cerr << " RESET " << file << endl;

        // Scan the list of metadata, looking for duplicates and marking all
        // the duplicates except the last one as deleted
        IDMaker id_maker(checker.idx->unique_codes());

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
        std::string basename = str::basename(segment->relpath);
        for (const auto& i: finddupes)
        {
            const Metadata& md = *i.second;
            const source::Blob& blob = md.sourceBlob();
            try {
                if (str::basename(blob.filename) != basename)
                    throw std::runtime_error("cannot rescan " + segment->relpath + ": metadata points to the wrong file: " + blob.filename);
                if (std::unique_ptr<types::source::Blob> old = checker.idx->index(md, segment->relpath, blob.offset))
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

    void index(metadata::Collection&& contents) override
    {
        // Add to index
        Pending p_idx = checker.idx->begin_transaction();
        for (auto& md: contents)
            if (checker.idx->index(*md, segment->relpath, md->sourceBlob().offset))
                throw std::runtime_error("duplicate detected while reordering segment");
        p_idx.commit();

        // Remove .metadata and .summary files
        sys::unlink_ifexists(segment->abspath + ".metadata");
        sys::unlink_ifexists(segment->abspath + ".summary");
    }

    void release(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) override
    {
        // Rebuild the metadata
        metadata::Collection mds;
        checker.idx->scan_file(segment->relpath, mds.inserter_func());
        mds.writeAtomically(segment->abspath + ".metadata");

        // Remove from index
        checker.idx->reset(segment->relpath);

        segment->move(new_root, new_relpath, new_abspath);
    }
};


Checker::Checker(std::shared_ptr<const ondisk2::Config> config)
    : m_config(config), idx(new index::WIndex(config))
{
    m_idx = idx;

    // Create the directory if it does not exist
    bool dir_created = sys::makedirs(config->path);

    lock = config->check_lock_dataset();
    m_idx->lock = lock;

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!dir_created && !sys::exists(config->index_pathname))
        files::createDontpackFlagfile(config->path);

    idx->open();
}

Checker::~Checker()
{
}

std::string Checker::type() const { return "ondisk2"; }

void Checker::check(CheckerConfig& opts)
{
    IndexedChecker::check(opts);

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
    segment_manager().scan_dir([&](const std::string& relpath) {
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

    segment_manager().scan_dir([&](const std::string& relpath) {
        if (m_idx->has_segment(relpath)) return;
        if (!config().step().pathMatches(relpath, *m)) return;
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
    if (!sys::exists(str::joinpath(config().path, ".summaries/all.summary")))
    {
        reporter.operation_progress(name(), "repack", "rebuilding the summary cache");
        Summary s;
        idx->summaryForAll(s);
    }

    return size_pre > size_post ? size_pre - size_post : 0;
}

void Checker::test_change_metadata(const std::string& relpath, Metadata& md, unsigned data_idx)
{
    metadata::Collection mds;
    idx->query_segment(relpath, mds.inserter_func());
    md.set_source(std::unique_ptr<arki::types::Source>(mds[data_idx].source().clone()));
    md.sourceBlob().unlock();
    mds[data_idx] = md;

    // Reindex mds
    idx->reset(relpath);
    for (auto& m: mds)
    {
        const source::Blob& source = m->sourceBlob();
        idx->index(*m, source.filename, source.offset);
    }

    md = mds[data_idx];
}

void Checker::test_remove_index(const std::string& relpath)
{
    m_idx->test_deindex(relpath);
}


}
}
}

