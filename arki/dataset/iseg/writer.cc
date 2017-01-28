#include "arki/dataset/iseg/writer.h"
#include "arki/dataset/iseg/index.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/step.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/time.h"
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
#include <algorithm>
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

namespace arki {
namespace dataset {
namespace iseg {

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

Segment* Writer::file(const Metadata& md, const std::string& format)
{
    Segment* writer = segmented::Writer::file(md, format);
    if (!writer->payload)
        writer->payload = new WIndex(m_config, writer->relname);
    return writer;
}

Writer::AcquireResult Writer::acquire_replace_never(Metadata& md)
{
    Segment* writer = file(md, md.source().format);
    WIndex* idx = static_cast<WIndex*>(writer->payload);
    Pending p_idx = idx->begin_transaction();

    off_t ofs;
    Pending p_df = writer->append(md, &ofs);
    auto source = types::source::Blob::create(md.source().format, config().path, writer->relname, ofs, md.data_size());

    try {
        idx->index(md, ofs);
        // Invalidate the summary cache for this month
        scache.invalidate(md);
        p_df.commit();
        p_idx.commit();
        md.set_source(move(source));
        return ACQ_OK;
    } catch (utils::sqlite::DuplicateInsert& di) {
        md.add_note("Failed to store in dataset '" + name() + "' because the dataset already has the data: " + di.what());
        return ACQ_ERROR_DUPLICATE;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return ACQ_ERROR;
    }
}

Writer::AcquireResult Writer::acquire_replace_always(Metadata& md)
{
    Segment* writer = file(md, md.source().format);
    WIndex* idx = static_cast<WIndex*>(writer->payload);
    Pending p_idx = idx->begin_transaction();

    off_t ofs;
    Pending p_df = writer->append(md, &ofs);
    auto source = types::source::Blob::create(md.source().format, config().path, writer->relname, ofs, md.data_size());

    try {
        idx->replace(md, ofs);
        // Invalidate the summary cache for this month
        scache.invalidate(md);
        p_df.commit();
        p_idx.commit();
        md.set_source(move(source));
        return ACQ_OK;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return ACQ_ERROR;
    }
}

Writer::AcquireResult Writer::acquire_replace_higher_usn(Metadata& md)
{
    Segment* writer = file(md, md.source().format);
    WIndex* idx = static_cast<WIndex*>(writer->payload);
    Pending p_idx = idx->begin_transaction();

    off_t ofs;
    Pending p_df = writer->append(md, &ofs);
    auto source = types::source::Blob::create(md.source().format, config().path, writer->relname, ofs, md.data_size());

    try {
        // Try to acquire without replacing
        idx->index(md, ofs);
        // Invalidate the summary cache for this month
        scache.invalidate(md);
        p_df.commit();
        p_idx.commit();
        md.set_source(move(source));
        return ACQ_OK;
    } catch (utils::sqlite::DuplicateInsert& di) {
        // It already exists, so we keep p_df uncommitted and check Update Sequence Numbers
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return ACQ_ERROR;
    }

    // Read the update sequence number of the new BUFR
    int new_usn;
    if (!scan::update_sequence_number(md, new_usn))
        return ACQ_ERROR_DUPLICATE;

    // Read the metadata of the existing BUFR
    throw std::runtime_error("iseg::Writer::acquire_replace_higher_usn not yet implemented");
#if 0
    Metadata old_md;
    if (!idx->get_current(md, old_md))
    {
        stringstream ss;
        ss << "cannot acquire into dataset " << name() << ": insert reported a conflict, the index failed to find the original version";
        throw runtime_error(ss.str());
    }

    // Read the update sequence number of the old BUFR
    int old_usn;
    if (!scan::update_sequence_number(old_md, old_usn))
    {
        stringstream ss;
        ss << "cannot acquire into dataset " << name() << ": insert reported a conflict, the new element has an Update Sequence Number but the old one does not, so they cannot be compared";
        throw runtime_error(ss.str());
    }

    // If there is no new Update Sequence Number, report a duplicate
    if (old_usn > new_usn)
        return ACQ_ERROR_DUPLICATE;

    // Replace, reusing the pending datafile transaction from earlier
    try {
        idx->replace(md, ofs);
        p_df.commit();
        p_idx.commit();
        md.set_source(move(source));
        return ACQ_OK;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + name() + "': " + e.what());
        return ACQ_ERROR;
    }
#endif
}

Writer::AcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    auto age_check = check_acquire_age(md);
    if (age_check.first) return age_check.second;

    acquire_lock();

    if (replace == REPLACE_DEFAULT) replace = config().default_replace_strategy;

    switch (replace)
    {
        case REPLACE_NEVER: return acquire_replace_never(md);
        case REPLACE_ALWAYS: return acquire_replace_always(md);
        case REPLACE_HIGHER_USN: return acquire_replace_higher_usn(md);
        default:
        {
            stringstream ss;
            ss << "cannot acquire into dataset " << name() << ": replace strategy " << (int)replace << " is not supported";
            throw runtime_error(ss.str());
        }
    }
}

void Writer::remove(Metadata& md)
{
    // Nothing to do
    throw std::runtime_error("cannot remove data from iseg dataset: dataset does not support removing items");
}

void Writer::flush()
{
    segmented::Writer::flush();
    //m_mft->flush();
    release_lock();
}

//Pending Writer::test_writelock()
//{
//    //return m_mft->test_writelock();
//}

Writer::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    // Acquire on iseg datasets always succeeds except in case of envrionment
    // issues like I/O errors and full disks
    return ACQ_OK;
}


Checker::Checker(std::shared_ptr<const iseg::Config> config)
    : m_config(config)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);
}

Checker::~Checker()
{
}

std::string Checker::type() const { return "iseg"; }

void Checker::list_segments(std::function<void(const std::string& relpath)> dest)
{
    vector<string> seg_relpaths;
    config().step().list_segments(config().path, config().format, Matcher(), [&](std::string&& s) { seg_relpaths.emplace_back(move(s)); });
    std::sort(seg_relpaths.begin(), seg_relpaths.end());
    for (const auto& relpath: seg_relpaths)
        dest(relpath);
}

void Checker::removeAll(dataset::Reporter& reporter, bool writable) { acquire_lock(); segmented::Checker::removeAll(reporter, writable); release_lock(); }
segmented::State Checker::scan(dataset::Reporter& reporter, bool quick)
{
    segmented::State segments_state;

    list_segments([&](const std::string& relpath) {
        string idx_abspath = str::joinpath(config().path, relpath) + ".index";
        if (!sys::exists(idx_abspath))
        {
            segments_state.insert(make_pair(relpath, segmented::SegmentState(SEGMENT_NEW)));
            return;
        }

        WIndex idx(m_config, relpath);
        metadata::Collection mds;
        idx.scan(mds.inserter_func(), "reftime, offset");
        segment::State state = SEGMENT_OK;

        // Compute the span of reftimes inside the segment
        unique_ptr<core::Time> md_begin;
        unique_ptr<core::Time> md_until;
        if (mds.empty())
        {
            reporter.segment_info(name(), relpath, "index knows of this segment but contains no data for it");
            md_begin.reset(new core::Time(0, 0, 0));
            md_until.reset(new core::Time(0, 0, 0));
            state = SEGMENT_UNALIGNED;
        } else {
            if (!mds.expand_date_range(md_begin, md_until))
            {
                reporter.segment_info(name(), relpath, "index data for this segment has no reference time information");
                state = SEGMENT_CORRUPTED;
                md_begin.reset(new core::Time(0, 0, 0));
                md_until.reset(new core::Time(0, 0, 0));
            } else {
                // Ensure that the reftime span fits inside the segment step
                core::Time seg_begin;
                core::Time seg_until;
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
    // Check if segments are old enough to be deleted or archived
    //

    core::Time archive_threshold(0, 0, 0);
    core::Time delete_threshold(0, 0, 0);
    const auto& st = SessionTime::get();

    if (config().archive_age != -1)
        archive_threshold = st.age_threshold(config().archive_age);
    if (config().delete_age != -1)
        delete_threshold = st.age_threshold(config().delete_age);

    for (auto& i: segments_state)
    {
        if (delete_threshold.ye != 0 && delete_threshold >= i.second.until)
        {
            reporter.segment_info(name(), i.first, "segment old enough to be deleted");
            i.second.state = i.second.state + SEGMENT_DELETE_AGE;
            continue;
        }

        if (archive_threshold.ye != 0 && archive_threshold >= i.second.until)
        {
            reporter.segment_info(name(), i.first, "segment old enough to be archived");
            i.second.state = i.second.state + SEGMENT_ARCHIVE_AGE;
            continue;
        }
    }

    return segments_state;
}
void Checker::repack(dataset::Reporter& reporter, bool writable) { acquire_lock(); segmented::Checker::repack(reporter, writable); release_lock(); }
void Checker::check(dataset::Reporter& reporter, bool fix, bool quick) { acquire_lock(); segmented::Checker::check(reporter, fix, quick); release_lock(); }

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
    //m_mft->acquire(relname, mtime, sum);
    //m_mft->flush();
}

void Checker::rescanSegment(const std::string& relpath)
{
    // Delete cached info to force a full rescan
    string pathname = str::joinpath(config().path, relpath);
    sys::unlink_ifexists(pathname + ".metadata");
    sys::unlink_ifexists(pathname + ".summary");

    //m_mft->rescanSegment(config().path, relpath);
}


size_t Checker::repackSegment(const std::string& relpath)
{
    WIndex idx(m_config, relpath);

    // Lock away writes and reads
    Pending p = idx.begin_exclusive_transaction();

    // Make a copy of the file with the right data in it, sorted by
    // reftime, and update the offsets in the index
    string pathname = str::joinpath(config().path, relpath);

    metadata::Collection mds;
    idx.scan(mds.inserter_func(), "reftime, offset");
    Pending p_repack = segment_manager().repack(relpath, mds);

    // Reindex mds
    idx.reset();
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        idx.index(**i, source.offset);
    }

    size_t size_pre = sys::size(pathname);

    // Remove the .metadata file if present, because we are shuffling the
    // data file and it will not be valid anymore
    string mdpathname = pathname + ".metadata";
    if (sys::exists(mdpathname))
        if (unlink(mdpathname.c_str()) < 0)
        {
            stringstream ss;
            ss << "cannot remove obsolete metadata file " << mdpathname;
            throw std::system_error(errno, std::system_category(), ss.str());
        }

    // Prevent reading the still open old file using the new offsets
    Metadata::flushDataReaders();

    // Commit the changes in the file system
    p_repack.commit();

    // Commit the changes in the database
    p.commit();

    size_t size_post = sys::size(pathname);

    return size_pre - size_post;
}

size_t Checker::removeSegment(const std::string& relpath, bool withData)
{
    sys::unlink_ifexists(str::joinpath(config().path, relpath + ".index"));
    return segmented::Checker::removeSegment(relpath, withData);
}

void Checker::releaseSegment(const std::string& relpath, const std::string& destpath)
{
    sys::unlink_ifexists(str::joinpath(config().path, relpath + ".index"));
    segmented::Checker::releaseSegment(relpath, destpath);
}

size_t Checker::vacuum()
{
    list_segments([&](const std::string& relpath) {
        WIndex idx(m_config, relpath);
        idx.vacuum();
    });
    return 0;
}


}
}
}
