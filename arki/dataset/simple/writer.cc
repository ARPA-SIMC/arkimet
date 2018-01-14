#include "arki/dataset/simple/writer.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/simple/datafile.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/step.h"
#include "arki/dataset/lock.h"
#include "arki/types/source/blob.h"
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
#include <ctime>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace dataset {
namespace simple {

Writer::Writer(std::shared_ptr<const simple::Config> config)
    : m_config(config), m_mft(0)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);

    lock = config->write_lock_dataset();

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(config->path))
        files::createDontpackFlagfile(config->path);

    unique_ptr<index::Manifest> mft = index::Manifest::create(config->path, config->lock_policy, config->index_type);
    m_mft = mft.release();
    m_mft->openRW();
    m_idx = m_mft;

    lock.reset();
}

Writer::~Writer()
{
    flush();
}

std::string Writer::type() const { return "simple"; }

std::shared_ptr<segment::Writer> Writer::file(const Metadata& md, const std::string& format)
{
    auto writer = segmented::Writer::file(md, format);
    if (!writer->payload)
        writer->payload = new datafile::MdBuf(writer->absname, config().lock_policy);
    return writer;
}

Writer::AcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    auto age_check = config().check_acquire_age(md);
    if (age_check.first) return age_check.second;

    if (!lock) lock = config().write_lock_dataset();
    // TODO: refuse if md is before "archive age"
    auto writer = file(md, md.source().format);
    datafile::MdBuf* mdbuf = static_cast<datafile::MdBuf*>(writer->payload);

    // Try appending
    try {
        const types::source::Blob* new_source;
        auto p = writer->append(md, &new_source);
        mdbuf->add(md, *new_source);
        p.commit();
        time_t ts = scan::timestamp(mdbuf->pathname);
        if (ts == 0)
            nag::warning("simple dataset acquire: %s timestamp is 0", mdbuf->pathname.c_str());
        m_mft->acquire(writer->relname, ts, mdbuf->sum);
        return ACQ_OK;
    } catch (std::exception& e) {
        fprintf(stderr, "ERROR %s\n", e.what());
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + config().name + "': " + e.what());
        return ACQ_ERROR;
    }

    // After appending, keep updated info in-memory, and update manifest on
    // flush when the Datafile structures are deallocated
}

void Writer::remove(Metadata& md)
{
    // Nothing to do
    throw std::runtime_error("cannot remove data from simple dataset: dataset does not support removing items");
}

void Writer::flush()
{
    segmented::Writer::flush();
    m_mft->flush();
    lock.reset();
}

Pending Writer::test_writelock()
{
    return m_mft->test_writelock();
}

Writer::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    std::shared_ptr<const simple::Config> config(new simple::Config(cfg));
    Metadata tmp_md(md);
    auto age_check = config->check_acquire_age(tmp_md);
    if (age_check.first) return age_check.second;

    // Acquire on simple datasets always succeeds except in case of envrionment
    // issues like I/O errors and full disks
    return ACQ_OK;
}


namespace {

struct RepackSort : public sort::Compare
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

    CheckerSegment(Checker& checker, const std::string& relpath)
        : checker(checker)
    {
        segment = checker.segment_manager().get_checker(relpath);
    }

    std::string path_relative() const override { return segment->relname; }

    segmented::SegmentState scan(dataset::Reporter& reporter, bool quick=true) override
    {
        if (!segment->exists_on_disk())
            return segmented::SegmentState(SEGMENT_MISSING);

        if (!checker.m_idx->has_segment(segment->relname))
        {
            //bool untrusted_index = files::hasDontpackFlagfile(checker.config().path);
            reporter.segment_info(checker.name(), segment->relname, "segment found on disk with no associated index data");
            //return segmented::SegmentState(untrusted_index ? SEGMENT_UNALIGNED : SEGMENT_DELETED);
            return segmented::SegmentState(SEGMENT_UNALIGNED);
        }

        string abspath = str::joinpath(checker.config().path, segment->relname);

        // TODO: replace with a method of Segment
        time_t ts_data = scan::timestamp(abspath);
        time_t ts_md = sys::timestamp(abspath + ".metadata", 0);
        time_t ts_sum = sys::timestamp(abspath + ".summary", 0);
        time_t ts_idx = checker.m_mft->segment_mtime(segment->relname);

        segment::State state = SEGMENT_OK;

        // Check timestamp consistency
        if (ts_idx != ts_data || ts_md < ts_data || ts_sum < ts_md)
        {
            if (ts_idx != ts_data)
                nag::verbose("%s: %s has a timestamp (%d) different than the one in the index (%d)",
                        checker.config().path.c_str(), segment->relname.c_str(), ts_data, ts_idx);
            if (ts_md < ts_data)
                nag::verbose("%s: %s has a timestamp (%d) newer that its metadata (%d)",
                        checker.config().path.c_str(), segment->relname.c_str(), ts_data, ts_md);
            if (ts_md < ts_data)
                nag::verbose("%s: %s metadata has a timestamp (%d) newer that its summary (%d)",
                        checker.config().path.c_str(), segment->relname.c_str(), ts_md, ts_sum);
            state = SEGMENT_UNALIGNED;
        }

        // Read metadata of segment contents

        //scan_file(m_path, i.file, state, v);
        //void scan_file(const std::string& root, const std::string& relname, segment::State state, segment::contents_func dest)

#if 0
        // If the segment file is compressed, create a temporary uncompressed copy
        unique_ptr<utils::compress::TempUnzip> tu;
        if (!quick && scan::isCompressed(absname))
            tu.reset(new utils::compress::TempUnzip(absname));
#endif
        // TODO: turn this into a Segment::exists/Segment::scan
        metadata::Collection contents;
        if (sys::exists(abspath + ".metadata"))
        {
            Metadata::read_file(metadata::ReadContext(abspath + ".metadata", checker.config().path), [&](unique_ptr<Metadata> md) {
                // Tweak Blob sources replacing the file name with segment->relname
                if (const source::Blob* s = md->has_source_blob())
                    md->set_source(Source::createBlobUnlocked(s->format, checker.config().path, segment->relname, s->offset, s->size));
                contents.acquire(move(md));
                return true;
            });
        }
        else if (scan::exists(abspath))
            // The index knows about the file, so instead of saying SEGMENT_DELETED
            // because we have data without metadata, we say SEGMENT_UNALIGNED
            // because the metadata needs to be regenerated
            state += SEGMENT_UNALIGNED;
        else
            state += SEGMENT_MISSING;

        RepackSort cmp;
        contents.sort(cmp); // Sort by reftime and by offset

        // Compute the span of reftimes inside the segment
        unique_ptr<Time> md_begin;
        unique_ptr<Time> md_until;
        if (contents.empty())
        {
            reporter.segment_info(checker.name(), segment->relname, "index knows of this segment but contains no data for it");
            md_begin.reset(new Time(0, 0, 0));
            md_until.reset(new Time(0, 0, 0));
            state = SEGMENT_UNALIGNED;
        } else {
            if (!contents.expand_date_range(md_begin, md_until))
            {
                reporter.segment_info(checker.name(), segment->relname, "index data for this segment has no reference time information");
                state = SEGMENT_CORRUPTED;
                md_begin.reset(new Time(0, 0, 0));
                md_until.reset(new Time(0, 0, 0));
            } else {
                // Ensure that the reftime span fits inside the segment step
                Time seg_begin;
                Time seg_until;
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

        if (!checker.segment_manager().exists(segment->relname))
        {
            // The segment does not exist on disk
            reporter.segment_info(checker.name(), segment->relname, "segment found in index but not on disk");
            state = state - SEGMENT_UNALIGNED + SEGMENT_MISSING;
        }

        if (state.is_ok())
            state = segment->check(reporter, checker.name(), contents, quick);

        auto res = segmented::SegmentState(state, *md_begin, *md_until);
        res.check_age(segment->relname, checker.config(), reporter);
        return res;
    }

    size_t repack(unsigned test_flags) override
    {
        auto lock = checker.lock->write_lock();

        // Read the metadata
        metadata::Collection mds;
        scan::scan(segment->absname, checker.config().lock_policy, mds.inserter_func());

        // Sort by reference time and offset
        RepackSort cmp;
        mds.sort(cmp);

        return reorder(mds, test_flags);
    }

    size_t reorder(metadata::Collection& mds, unsigned test_flags) override
    {
        // Write out the data with the new order
        Pending p_repack = segment->repack(checker.config().path, mds, test_flags);

        // Strip paths from mds sources
        mds.strip_source_paths();

        // Remove existing cached metadata, since we scramble their order
        sys::unlink_ifexists(segment->absname + ".metadata");
        sys::unlink_ifexists(segment->absname + ".summary");

        size_t size_pre = sys::isdir(segment->absname) ? 0 : sys::size(segment->absname);

        p_repack.commit();

        size_t size_post = sys::isdir(segment->absname) ? 0 : sys::size(segment->absname);

        // Write out the new metadata
        mds.writeAtomically(segment->absname + ".metadata");

        // Regenerate the summary. It is unchanged, really, but its timestamp
        // has become obsolete by now
        Summary sum;
        mds.add_to_summary(sum);
        sum.writeAtomically(segment->absname + ".summary");

        // Reindex with the new file information
        time_t mtime = sys::timestamp(segment->absname);
        checker.m_mft->acquire(segment->relname, mtime, sum);

        return size_pre - size_post;
    }

    size_t remove(bool with_data)
    {
        checker.m_mft->remove(segment->relname);
        sys::unlink_ifexists(segment->absname + ".metadata");
        sys::unlink_ifexists(segment->absname + ".summary");
        if (!with_data) return 0;
        return segment->remove();
    }
};


Checker::Checker(std::shared_ptr<const simple::Config> config)
    : m_config(config), m_mft(0)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);

    lock = config->read_lock_dataset();

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(config->path))
        files::createDontpackFlagfile(config->path);

    unique_ptr<index::Manifest> mft = index::Manifest::create(config->path, config->lock_policy, config->index_type);
    m_mft = mft.release();
    m_mft->openRW();
    m_idx = m_mft;
}

Checker::~Checker()
{
    m_mft->flush();
}

std::string Checker::type() const { return "simple"; }

void Checker::remove_all(dataset::Reporter& reporter, bool writable)
{
    IndexedChecker::remove_all(reporter, writable);
}
void Checker::remove_all_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable)
{
    IndexedChecker::remove_all_filtered(matcher, reporter, writable);
}
void Checker::repack(dataset::Reporter& reporter, bool writable, unsigned test_flags)
{
    IndexedChecker::repack(reporter, writable, test_flags);
    m_mft->flush();
}
void Checker::repack_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool writable, unsigned test_flags)
{
    IndexedChecker::repack_filtered(matcher, reporter, writable, test_flags);
    m_mft->flush();
}
void Checker::check(dataset::Reporter& reporter, bool fix, bool quick) {
    IndexedChecker::check(reporter, fix, quick);
    m_mft->flush();
}
void Checker::check_filtered(const Matcher& matcher, dataset::Reporter& reporter, bool fix, bool quick)
{
    IndexedChecker::check_filtered(matcher, reporter, fix, quick);
    m_mft->flush();
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment(const std::string& relpath)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath));
}

void Checker::segments(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    std::vector<std::string> names;
    m_idx->list_segments([&](const std::string& relpath) { names.push_back(relpath); });

    for (const auto& relpath: names)
    {
        CheckerSegment segment(*this, relpath);
        dest(segment);
    }
}

void Checker::segments_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    // TODO: implement filtering
    std::vector<std::string> names;
    m_idx->list_segments_filtered(matcher, [&](const std::string& relpath) { names.push_back(relpath); });

    for (const auto& relpath: names)
    {
        CheckerSegment segment(*this, relpath);
        dest(segment);
    }
}

void Checker::segments_untracked(std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    segment_manager().scan_dir([&](const std::string& relpath) {
        if (m_idx->has_segment(relpath)) return;
        CheckerSegment segment(*this, relpath);
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
        CheckerSegment segment(*this, relpath);
        dest(segment);
    });
}

void Checker::indexSegment(const std::string& relname, metadata::Collection&& mds)
{
    string pathname = str::joinpath(config().path, relname);
    time_t mtime = scan::timestamp(pathname);
    if (mtime == 0)
        throw std::runtime_error("cannot acquire " + pathname + ": file does not exist");

    // Iterate the metadata, computing the summary and making the data
    // paths relative
    mds.strip_source_paths();
    Summary sum;
    mds.add_to_summary(sum);

    // Regenerate .metadata
    mds.writeAtomically(pathname + ".metadata");

    // Regenerate .summary
    sum.writeAtomically(pathname + ".summary");

    // Add to manifest
    m_mft->acquire(relname, mtime, sum);
    m_mft->flush();
}

void Checker::rescanSegment(const std::string& relpath)
{
    // Delete cached info to force a full rescan
    string pathname = str::joinpath(config().path, relpath);
    sys::unlink_ifexists(pathname + ".metadata");
    sys::unlink_ifexists(pathname + ".summary");

    m_mft->rescanSegment(config().path, relpath);
    m_mft->flush();
}

void Checker::releaseSegment(const std::string& relpath, const std::string& destpath)
{
    // Remove from index
    m_mft->remove(relpath);

    IndexedChecker::releaseSegment(relpath, destpath);
}

size_t Checker::vacuum(dataset::Reporter& reporter)
{
    reporter.operation_progress(name(), "repack", "running VACUUM ANALYZE on the dataset index, if applicable");
    return m_mft->vacuum();
}

void Checker::test_remove_index(const std::string& relpath)
{
    m_idx->test_deindex(relpath);
    string pathname = str::joinpath(config().path, relpath);
    sys::unlink_ifexists(pathname + ".metadata");
    sys::unlink_ifexists(pathname + ".summary");
}

void Checker::test_rename(const std::string& relpath, const std::string& new_relpath)
{
    IndexedChecker::test_rename(relpath, new_relpath);
    string abspath = str::joinpath(config().path, relpath);
    string new_abspath = str::joinpath(config().path, new_relpath);

    sys::rename(abspath + ".metadata", new_abspath + ".metadata");
    sys::rename(abspath + ".summary", new_abspath + ".summary");
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
