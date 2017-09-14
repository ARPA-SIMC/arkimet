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
}

std::string Writer::type() const { return "simple"; }

Segment* Writer::file(const Metadata& md, const std::string& format)
{
    Segment* writer = segmented::Writer::file(md, format);
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
    Segment* writer = file(md, md.source().format);
    datafile::MdBuf* mdbuf = static_cast<datafile::MdBuf*>(writer->payload);

    // Try appending
    try {
        off_t offset = writer->append(md);
        auto source = types::source::Blob::create(md.source().format, config().path, writer->relname, offset, md.data_size());
        md.set_source(move(source));
        mdbuf->add(md);
        time_t ts = scan::timestamp(mdbuf->pathname);
        if (ts == 0)
            fprintf(stderr, "WARNING: %s timestamp is 0\n", mdbuf->pathname.c_str());
        m_mft->acquire(writer->relname, ts, mdbuf->sum);
        return ACQ_OK;
    } catch (std::exception& e) {
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
}

std::string Checker::type() const { return "simple"; }

void Checker::removeAll(dataset::Reporter& reporter, bool writable) { acquire_lock(); IndexedChecker::removeAll(reporter, writable); release_lock(); }
void Checker::repack(dataset::Reporter& reporter, bool writable, unsigned test_flags) { acquire_lock(); IndexedChecker::repack(reporter, writable, test_flags); release_lock(); }
void Checker::check(dataset::Reporter& reporter, bool fix, bool quick) { acquire_lock(); IndexedChecker::check(reporter, fix, quick); release_lock(); }

segmented::State Checker::scan(dataset::Reporter& reporter, bool quick)
{
    segmented::State segments_state;

    //
    // Populate segments_state with the contents of the index
    //

    m_idx->scan_files([&](const std::string& relpath, segment::State state, const metadata::Collection& mds) {
        // Compute the span of reftimes inside the segment
        unique_ptr<Time> md_begin;
        unique_ptr<Time> md_until;
        if (mds.empty())
        {
            reporter.segment_info(name(), relpath, "index knows of this segment but contains no data for it");
            md_begin.reset(new Time(0, 0, 0));
            md_until.reset(new Time(0, 0, 0));
            state = SEGMENT_UNALIGNED;
        } else {
            if (!mds.expand_date_range(md_begin, md_until))
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

        if (state.is_ok())
            state = segment_manager().check(reporter, name(), relpath, mds, quick);

        segments_state.insert(make_pair(relpath, segmented::SegmentState(state, *md_begin, *md_until)));
    });


    //
    // Add information from the state of files on disk
    //

    std::set<std::string> disk;
    segment_manager().scan_dir([&](const std::string& relpath) { disk.insert(relpath);; });

    // files: a, b, c,    e, f, g
    // index:       c, d, e, f, g

    for (auto& i: segments_state)
    {
        if (disk.erase(i.first) == 0)
        {
            // The file did not exist on disk
            reporter.segment_info(name(), i.first, "segment found in index but not on disk");
            i.second.state = i.second.state - SEGMENT_UNALIGNED + SEGMENT_MISSING;
        }
    }
    for (const auto& relpath : disk)
    {
        reporter.segment_info(name(), relpath, "segment found on disk with no associated index data");
        segments_state.insert(make_pair(relpath, segmented::SegmentState(SEGMENT_UNALIGNED)));
    }

    segments_state.check_age(config(), reporter);

    return segments_state;
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
    fprintf(stderr, "REINDEX MTIME %ld\n", mtime);
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
