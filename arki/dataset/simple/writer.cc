#include "arki/dataset/simple/writer.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/simple/datafile.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/step.h"
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

    acquire_lock();

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(config->path))
        files::createDontpackFlagfile(config->path);

    unique_ptr<index::Manifest> mft = index::Manifest::create(config->path, config->index_type);
    m_mft = mft.release();
    m_mft->openRW();
    m_idx = m_mft;

    release_lock();
}

Writer::~Writer()
{
    flush();
    delete lock;
}

std::string Writer::type() const { return "simple"; }

void Writer::acquire_lock()
{
    if (!lock) lock = new LocalLock(config());
    lock->acquire();
}

void Writer::release_lock()
{
    if (!lock) lock = new LocalLock(config());
    lock->release();
}

std::shared_ptr<segment::Writer> Writer::file(const Metadata& md, const std::string& format)
{
    auto writer = segmented::Writer::file(md, format);
    if (!writer->payload)
        writer->payload = new datafile::MdBuf(writer->absname);
    return writer;
}

Writer::AcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    auto age_check = config().check_acquire_age(md);
    if (age_check.first) return age_check.second;

    acquire_lock();
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
    release_lock();
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


Checker::Checker(std::shared_ptr<const simple::Config> config)
    : m_config(config), m_mft(0)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);

    acquire_lock();

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(config->path))
        files::createDontpackFlagfile(config->path);

    unique_ptr<index::Manifest> mft = index::Manifest::create(config->path, config->index_type);
    m_mft = mft.release();
    m_mft->openRW();
    m_idx = m_mft;

    release_lock();
}

Checker::~Checker()
{
    m_mft->flush();
    delete lock;
}

std::string Checker::type() const { return "simple"; }

void Checker::acquire_lock()
{
    if (!lock) lock = new LocalLock(config());
    lock->acquire();
}

void Checker::release_lock()
{
    if (!lock) lock = new LocalLock(config());
    lock->release();
}

void Checker::removeAll(dataset::Reporter& reporter, bool writable) { acquire_lock(); IndexedChecker::removeAll(reporter, writable); release_lock(); }
void Checker::repack(dataset::Reporter& reporter, bool writable, unsigned test_flags)
{
    acquire_lock();
    IndexedChecker::repack(reporter, writable, test_flags);
    m_mft->flush();
    release_lock();
}
void Checker::check(dataset::Reporter& reporter, bool fix, bool quick) { acquire_lock(); IndexedChecker::check(reporter, fix, quick); release_lock(); }

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

segmented::SegmentState Checker::scan_segment(const std::string& relpath, dataset::Reporter& reporter, bool quick)
{
    string abspath = str::joinpath(config().path, relpath);

    // TODO: replace with a method of Segment
    time_t ts_data = scan::timestamp(abspath);
    time_t ts_md = sys::timestamp(abspath + ".metadata", 0);
    time_t ts_sum = sys::timestamp(abspath + ".summary", 0);
    time_t ts_idx = m_mft->segment_mtime(relpath);

    segment::State state = SEGMENT_OK;

    // Check timestamp consistency
    if (ts_idx != ts_data || ts_md < ts_data || ts_sum < ts_md)
    {
        if (ts_idx != ts_data)
            nag::verbose("%s: %s has a timestamp (%d) different than the one in the index (%d)",
                    config().path.c_str(), relpath.c_str(), ts_data, ts_idx);
        if (ts_md < ts_data)
            nag::verbose("%s: %s has a timestamp (%d) newer that its metadata (%d)",
                    config().path.c_str(), relpath.c_str(), ts_data, ts_md);
        if (ts_md < ts_data)
            nag::verbose("%s: %s metadata has a timestamp (%d) newer that its summary (%d)",
                    config().path.c_str(), relpath.c_str(), ts_md, ts_sum);
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
        Metadata::read_file(metadata::ReadContext(abspath + ".metadata", config().path), [&](unique_ptr<Metadata> md) {
            // Tweak Blob sources replacing the file name with relpath
            if (const source::Blob* s = md->has_source_blob())
                md->set_source(Source::createBlobUnlocked(s->format, config().path, relpath, s->offset, s->size));
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
        reporter.segment_info(name(), relpath, "index knows of this segment but contains no data for it");
        md_begin.reset(new Time(0, 0, 0));
        md_until.reset(new Time(0, 0, 0));
        state = SEGMENT_UNALIGNED;
    } else {
        if (!contents.expand_date_range(md_begin, md_until))
        {
            reporter.segment_info(name(), relpath, "index data for this segment has no reference time information");
            state = SEGMENT_CORRUPTED;
            md_begin.reset(new Time(0, 0, 0));
            md_until.reset(new Time(0, 0, 0));
        } else {
            // Ensure that the reftime span fits inside the segment step
            Time seg_begin;
            Time seg_until;
            if (config().step().path_timespan(relpath, seg_begin, seg_until))
            {
                if (*md_begin < seg_begin || *md_until > seg_until)
                {
                    reporter.segment_info(name(), relpath, "segment contents do not fit inside the step of this dataset");
                    state = SEGMENT_CORRUPTED;
                }
                // Expand segment timespan to the full possible segment timespan
                *md_begin = seg_begin;
                *md_until = seg_until;
            } else {
                reporter.segment_info(name(), relpath, "segment name does not fit the step of this dataset");
                state = SEGMENT_CORRUPTED;
            }
        }
    }

    if (!segment_manager().exists(relpath))
    {
        // The segment does not exist on disk
        reporter.segment_info(name(), relpath, "segment found in index but not on disk");
        state = state - SEGMENT_UNALIGNED + SEGMENT_MISSING;
    }

    if (state.is_ok())
        state = segment_manager().check(reporter, name(), relpath, contents, quick);

    auto res = segmented::SegmentState(state, *md_begin, *md_until);
    res.check_age(relpath, config(), reporter);
    return res;
}

void Checker::scan(dataset::Reporter& reporter, bool quick, std::function<void(const std::string& relpath, const segmented::SegmentState& state)> dest)
{
    // Populate segments_state with the contents of the index
    std::set<std::string> seen;
    m_idx->list_segments([&](const std::string& relpath) { seen.insert(relpath); });

    for (const auto& relpath : seen)
        dest(relpath, scan_segment(relpath, reporter, quick));

    // Add information from the state of files on disk
    segment_manager().scan_dir([&](const std::string& relpath) {
        if (seen.find(relpath) != seen.end()) return;
        reporter.segment_info(name(), relpath, "segment found on disk with no associated index data");
        dest(relpath, segmented::SegmentState(SEGMENT_UNALIGNED));
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

size_t Checker::repackSegment(const std::string& relpath, unsigned test_flags)
{
    string pathname = str::joinpath(config().path, relpath);

    // Read the metadata
    metadata::Collection mds;
    scan::scan(pathname, mds.inserter_func());

    // Sort by reference time and offset
    RepackSort cmp;
    mds.sort(cmp);

    return reorder_segment(relpath, mds, test_flags);
}

size_t Checker::reorder_segment(const std::string& relpath, metadata::Collection& mds, unsigned test_flags)
{
    string pathname = str::joinpath(config().path, relpath);

    // Write out the data with the new order
    Pending p_repack = segment_manager().repack(relpath, mds, test_flags);

    // Strip paths from mds sources
    mds.strip_source_paths();

    // Prevent reading the still open old file using the new offsets
    Metadata::flushDataReaders();

    // Remove existing cached metadata, since we scramble their order
    sys::unlink_ifexists(pathname + ".metadata");
    sys::unlink_ifexists(pathname + ".summary");

    size_t size_pre = sys::isdir(pathname) ? 0 : sys::size(pathname);

    p_repack.commit();

    size_t size_post = sys::isdir(pathname) ? 0 : sys::size(pathname);

    // Write out the new metadata
    mds.writeAtomically(pathname + ".metadata");

    // Regenerate the summary. It is unchanged, really, but its timestamp
    // has become obsolete by now
    Summary sum;
    mds.add_to_summary(sum);
    sum.writeAtomically(pathname + ".summary");

    // Reindex with the new file information
    time_t mtime = sys::timestamp(pathname);
    m_mft->acquire(relpath, mtime, sum);

    return size_pre - size_post;
}

size_t Checker::removeSegment(const std::string& relpath, bool withData)
{
    m_mft->remove(relpath);
    string pathname = str::joinpath(config().path, relpath);
    sys::unlink_ifexists(pathname + ".metadata");
    sys::unlink_ifexists(pathname + ".summary");
    return segmented::Checker::removeSegment(relpath, withData);
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
