#include "arki/dataset/simple/checker.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/step.h"
#include "arki/dataset/lock.h"
#include "arki/dataset/session.h"
#include "arki/types/source/blob.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/matcher.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/sort.h"
#include "arki/scan.h"
#include "arki/nag.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <ctime>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace dataset {
namespace simple {

namespace {

struct RepackSort : public metadata::sort::Compare
{
    int compare(const Metadata& a, const Metadata& b) const override
    {
        const types::Type* rta = a.get(TYPE_REFTIME);
        const types::Type* rtb = b.get(TYPE_REFTIME);
        if (rta && !rtb) return 1;
        if (!rta && rtb) return -1;
        if (rta && rtb)
            if (int res = rta->compare(*rtb)) return res;
        if (a.sourceBlob().offset > b.sourceBlob().offset) return 1;
        if (b.sourceBlob().offset > a.sourceBlob().offset) return -1;
        return 0;
    }

    std::string toString() const override { return "reftime,offset"; }
};

}


class CheckerSegment : public segmented::CheckerSegment
{
public:
    Checker& checker;

    CheckerSegment(Checker& checker, const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
        : segmented::CheckerSegment(lock), checker(checker)
    {
        segment = checker.config().session->segment_checker(scan::Scanner::format_from_filename(relpath), config().path, relpath);
    }

    std::string path_relative() const override { return segment->segment().relpath; }
    const simple::Dataset& config() const override { return checker.config(); }
    std::shared_ptr<dataset::archive::Checker> archives() override { return checker.archive(); }

    void get_metadata(std::shared_ptr<core::Lock> lock, metadata::Collection& mds) override
    {
        auto reader = segment->segment().reader(lock);
        reader->scan(mds.inserter_func());
    }

    segmented::SegmentState scan(dataset::Reporter& reporter, bool quick=true) override
    {
        if (!segment->exists_on_disk())
        {
            reporter.segment_info(checker.name(), segment->segment().relpath, "segment found in index but not on disk");
            return segmented::SegmentState(segment::SEGMENT_MISSING);
        }

        if (!checker.m_idx->has_segment(segment->segment().relpath))
        {
            if (segment->is_empty())
            {
                reporter.segment_info(checker.name(), segment->segment().relpath, "empty segment found on disk with no associated index data");
                return segmented::SegmentState(segment::SEGMENT_DELETED);
            } else {
                //bool untrusted_index = files::hasDontpackFlagfile(checker.config().path);
                reporter.segment_info(checker.name(), segment->segment().relpath, "segment found on disk with no associated index data");
                //return segmented::SegmentState(untrusted_index ? segment::SEGMENT_UNALIGNED : segment::SEGMENT_DELETED);
                return segmented::SegmentState(segment::SEGMENT_UNALIGNED);
            }
        }

        // TODO: replace with a method of Segment
        time_t ts_data = segment->segment().timestamp();
        time_t ts_md = sys::timestamp(segment->segment().abspath + ".metadata", 0);
        time_t ts_sum = sys::timestamp(segment->segment().abspath + ".summary", 0);
        time_t ts_idx = checker.m_mft->segment_mtime(segment->segment().relpath);

        segment::State state = segment::SEGMENT_OK;

        // Check timestamp consistency
        if (ts_idx != ts_data || ts_md < ts_data || ts_sum < ts_md)
        {
            if (ts_idx != ts_data)
                nag::verbose("%s: %s has a timestamp (%ld) different than the one in the index (%ld)",
                        checker.config().path.c_str(), segment->segment().relpath.c_str(), (long int)ts_data, (long int)ts_idx);
            if (ts_md < ts_data)
                nag::verbose("%s: %s has a timestamp (%ld) newer that its metadata (%ld)",
                        checker.config().path.c_str(), segment->segment().relpath.c_str(), (long int)ts_data, (long int)ts_md);
            if (ts_md < ts_data)
                nag::verbose("%s: %s metadata has a timestamp (%ld) newer that its summary (%ld)",
                        checker.config().path.c_str(), segment->segment().relpath.c_str(), (long int)ts_md, (long int)ts_sum);
            state = segment::SEGMENT_UNALIGNED;
        }

        // Read metadata of segment contents

        //scan_file(m_path, i.file, state, v);
        //void scan_file(const std::string& root, const std::string& relpath, segment::State state, segment::contents_func dest)

        // TODO: turn this into a Segment::exists/Segment::scan
        metadata::Collection contents;
        if (sys::exists(segment->segment().abspath + ".metadata"))
        {
            Metadata::read_file(metadata::ReadContext(segment->segment().abspath + ".metadata", checker.config().path), [&](std::shared_ptr<Metadata> md) {
                // Tweak Blob sources replacing the file name with segment->relpath
                if (const source::Blob* s = md->has_source_blob())
                    md->set_source(Source::createBlobUnlocked(s->format, checker.config().path, segment->segment().relpath, s->offset, s->size));
                contents.acquire(md);
                return true;
            });
        }
        else
            // The index knows about the file, so instead of saying segment::SEGMENT_DELETED
            // because we have data without metadata, we say segment::SEGMENT_UNALIGNED
            // because the metadata needs to be regenerated
            state += segment::SEGMENT_UNALIGNED;

        RepackSort cmp;
        contents.sort(cmp); // Sort by reftime and by offset

        // Compute the span of reftimes inside the segment
        unique_ptr<Time> md_begin;
        unique_ptr<Time> md_until;
        if (contents.empty())
        {
            reporter.segment_info(checker.name(), segment->segment().relpath, "index knows of this segment but contains no data for it");
            md_begin.reset(new Time(0, 0, 0));
            md_until.reset(new Time(0, 0, 0));
            state = segment::SEGMENT_UNALIGNED;
        } else {
            if (!contents.expand_date_range(md_begin, md_until))
            {
                reporter.segment_info(checker.name(), segment->segment().relpath, "index data for this segment has no reference time information");
                state = segment::SEGMENT_CORRUPTED;
                md_begin.reset(new Time(0, 0, 0));
                md_until.reset(new Time(0, 0, 0));
            } else {
                // Ensure that the reftime span fits inside the segment step
                Time seg_begin;
                Time seg_until;
                if (checker.config().step().path_timespan(segment->segment().relpath, seg_begin, seg_until))
                {
                    if (*md_begin < seg_begin || *md_until > seg_until)
                    {
                        reporter.segment_info(checker.name(), segment->segment().relpath, "segment contents do not fit inside the step of this dataset");
                        state = segment::SEGMENT_CORRUPTED;
                    }
                    // Expand segment timespan to the full possible segment timespan
                    *md_begin = seg_begin;
                    *md_until = seg_until;
                } else {
                    reporter.segment_info(checker.name(), segment->segment().relpath, "segment name does not fit the step of this dataset");
                    state = segment::SEGMENT_CORRUPTED;
                }
            }
        }

        if (state.is_ok())
            state = segment->check([&](const std::string& msg) { reporter.segment_info(checker.name(), segment->segment().relpath, msg); }, contents, quick);

        auto res = segmented::SegmentState(state, *md_begin, *md_until);
        res.check_age(segment->segment().relpath, checker.config(), reporter);
        return res;
    }

    size_t repack(unsigned test_flags) override
    {
        auto lock = checker.lock->write_lock();

        // Read the metadata
        metadata::Collection mds;
        get_metadata(lock, mds);

        // Sort by reference time and offset
        RepackSort cmp;
        mds.sort(cmp);

        return reorder(mds, test_flags);
    }

    size_t reorder(metadata::Collection& mds, unsigned test_flags) override
    {
        // Write out the data with the new order
        segment::RepackConfig repack_config;
        repack_config.gz_group_size = config().gz_group_size;
        repack_config.test_flags = test_flags;
        auto p_repack = segment->repack(checker.config().path, mds, repack_config);

        // Strip paths from mds sources
        mds.strip_source_paths();

        // Remove existing cached metadata, since we scramble their order
        sys::unlink_ifexists(segment->segment().abspath + ".metadata");
        sys::unlink_ifexists(segment->segment().abspath + ".summary");

        size_t size_pre = segment->size();

        p_repack.commit();

        size_t size_post = segment->size();

        // Write out the new metadata
        mds.writeAtomically(segment->segment().abspath + ".metadata");

        // Regenerate the summary. It is unchanged, really, but its timestamp
        // has become obsolete by now
        Summary sum;
        mds.add_to_summary(sum);
        sum.writeAtomically(segment->segment().abspath + ".summary");


        // Reindex with the new file information
        time_t mtime = segment->segment().timestamp();
        checker.m_mft->acquire(segment->segment().relpath, mtime, sum);

        return size_pre - size_post;
    }

    size_t remove(bool with_data) override
    {
        checker.m_mft->remove(segment->segment().relpath);
        sys::unlink_ifexists(segment->segment().abspath + ".metadata");
        sys::unlink_ifexists(segment->segment().abspath + ".summary");
        if (!with_data) return 0;
        return segment->remove();
    }

    void tar() override
    {
        if (sys::exists(segment->segment().abspath + ".tar"))
            return;

        auto lock = checker.lock->write_lock();

        metadata::Collection mds;
        get_metadata(lock, mds);

        // Remove existing cached metadata, since we scramble their order
        sys::unlink_ifexists(segment->segment().abspath + ".metadata");
        sys::unlink_ifexists(segment->segment().abspath + ".summary");

        // Create the .tar segment
        segment = segment->tar(mds);

        // Write out the new metadata
        mds.writeAtomically(segment->segment().abspath + ".metadata");

        // Regenerate the summary. It is unchanged, really, but its timestamp
        // has become obsolete by now
        Summary sum;
        mds.add_to_summary(sum);
        sum.writeAtomically(segment->segment().abspath + ".summary");

        // Reindex with the new file information
        time_t mtime = segment->segment().timestamp();
        checker.m_mft->acquire(segment->segment().relpath, mtime, sum);
    }

    void zip() override
    {
        if (sys::exists(segment->segment().abspath + ".zip"))
            return;

        auto lock = checker.lock->write_lock();

        metadata::Collection mds;
        get_metadata(lock, mds);

        // Remove existing cached metadata, since we scramble their order
        sys::unlink_ifexists(segment->segment().abspath + ".metadata");
        sys::unlink_ifexists(segment->segment().abspath + ".summary");

        // Create the .tar segment
        segment = segment->zip(mds);

        // Write out the new metadata
        mds.writeAtomically(segment->segment().abspath + ".metadata");

        // Regenerate the summary. It is unchanged, really, but its timestamp
        // has become obsolete by now
        Summary sum;
        mds.add_to_summary(sum);
        sum.writeAtomically(segment->segment().abspath + ".summary");

        // Reindex with the new file information
        time_t mtime = segment->segment().timestamp();
        checker.m_mft->acquire(segment->segment().relpath, mtime, sum);
    }

    size_t compress(unsigned groupsize) override
    {
        if (sys::exists(segment->segment().abspath + ".gz") || sys::exists(segment->segment().abspath + ".gz.idx"))
            return 0;

        auto lock = checker.lock->write_lock();

        metadata::Collection mds;
        get_metadata(lock, mds);

        // Remove existing cached metadata, since we scramble their order
        sys::unlink_ifexists(segment->segment().abspath + ".metadata");
        sys::unlink_ifexists(segment->segment().abspath + ".summary");

        // Create the .tar segment
        size_t old_size = segment->size();
        segment = segment->compress(mds, groupsize);
        size_t new_size = segment->size();

        // Write out the new metadata
        mds.writeAtomically(segment->segment().abspath + ".metadata");

        // Regenerate the summary. It is unchanged, really, but its timestamp
        // has become obsolete by now
        Summary sum;
        mds.add_to_summary(sum);
        sum.writeAtomically(segment->segment().abspath + ".summary");

        // Reindex with the new file information
        time_t mtime = segment->segment().timestamp();
        checker.m_mft->acquire(segment->segment().relpath, mtime, sum);

        if (old_size > new_size)
            return old_size - new_size;
        else
            return 0;
    }

    void index(metadata::Collection&& mds) override
    {
        time_t mtime = segment->segment().timestamp();

        // Iterate the metadata, computing the summary and making the data
        // paths relative
        mds.strip_source_paths();
        Summary sum;
        mds.add_to_summary(sum);

        // Regenerate .metadata
        mds.writeAtomically(segment->segment().abspath + ".metadata");

        // Regenerate .summary
        sum.writeAtomically(segment->segment().abspath + ".summary");

        // Add to manifest
        checker.m_mft->acquire(segment->segment().relpath, mtime, sum);
        checker.m_mft->flush();
    }

    void rescan(dataset::Reporter& reporter) override
    {
        // Delete cached info to force a full rescan
        sys::unlink_ifexists(segment->segment().abspath + ".metadata");
        sys::unlink_ifexists(segment->segment().abspath + ".summary");

        std::string dirname(str::dirname(segment->segment().abspath));
        std::string basename(str::basename(segment->segment().abspath));

        metadata::Collection mds;
        segment->rescan_data(
                [&](const std::string& msg) { reporter.segment_info(checker.name(), segment->segment().relpath, msg); },
                lock, [&](std::shared_ptr<Metadata> md) {
                    auto& source = md->sourceBlob();
                    md->set_source(Source::createBlobUnlocked(segment->segment().format, dirname, basename, source.offset, source.size));
                    mds.acquire(md);
                    return true;
                });

        Summary sum;
        for (const auto& md: mds)
            sum.add(*md);

        // Regenerate .metadata and .summary
        mds.writeAtomically(segment->segment().abspath + ".metadata");
        sum.writeAtomically(segment->segment().abspath + ".summary");

        // Add to manifest
        checker.m_mft->acquire(segment->segment().relpath, segment->segment().timestamp(), sum);
        checker.m_mft->flush();
    }

    void release(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) override
    {
        checker.m_mft->remove(segment->segment().relpath);
        segment = segment->move(new_root, new_relpath, new_abspath);
    }
};


Checker::Checker(std::shared_ptr<simple::Dataset> config)
    : m_config(config), m_mft(0)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);

    lock = config->check_lock_dataset();

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(config->path))
        files::createDontpackFlagfile(config->path);

    unique_ptr<index::Manifest> mft = index::Manifest::create(config->path, config->lock_policy, config->index_type);
    m_mft = mft.release();
    m_mft->lock = lock;
    m_mft->openRW();
    m_idx = m_mft;
}

Checker::~Checker()
{
    m_mft->flush();
}

std::string Checker::type() const { return "simple"; }

void Checker::repack(CheckerConfig& opts, unsigned test_flags)
{
    indexed::Checker::repack(opts, test_flags);
    m_mft->flush();
}

void Checker::check(CheckerConfig& opts)
{
    indexed::Checker::check(opts);
    m_mft->flush();
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
    std::vector<std::string> names;
    m_idx->list_segments([&](const std::string& relpath) { names.push_back(relpath); });

    for (const auto& relpath: names)
    {
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    }
}

void Checker::segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    // TODO: implement filtering
    std::vector<std::string> names;
    m_idx->list_segments_filtered(matcher, [&](const std::string& relpath) { names.push_back(relpath); });

    for (const auto& relpath: names)
    {
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    }
}

void Checker::segments_untracked(std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    scan_dir(config().path, [&](const std::string& relpath) {
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

    scan_dir(config().path, [&](const std::string& relpath) {
        if (m_idx->has_segment(relpath)) return;
        if (!config().step().pathMatches(relpath, *m)) return;
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    });
}

size_t Checker::vacuum(dataset::Reporter& reporter)
{
    reporter.operation_progress(name(), "repack", "running VACUUM ANALYZE on the dataset index, if applicable");
    return m_mft->vacuum();
}

void Checker::test_delete_from_index(const std::string& relpath)
{
    m_idx->test_deindex(relpath);
    string pathname = str::joinpath(config().path, relpath);
    sys::unlink_ifexists(pathname + ".metadata");
    sys::unlink_ifexists(pathname + ".summary");
}

void Checker::test_invalidate_in_index(const std::string& relpath)
{
    m_idx->test_deindex(relpath);
    sys::touch(str::joinpath(config().path, relpath + ".metadata"), 1496167200);
}

void Checker::test_change_metadata(const std::string& relpath, Metadata& md, unsigned data_idx)
{
    string md_pathname = str::joinpath(config().path, relpath) + ".metadata";
    metadata::Collection mds;
    mds.read_from_file(md_pathname);
    md.set_source(std::unique_ptr<arki::types::Source>(mds[data_idx].source().clone()));
    mds[data_idx] = md;
    mds.writeAtomically(md_pathname);
}

}
}
}

