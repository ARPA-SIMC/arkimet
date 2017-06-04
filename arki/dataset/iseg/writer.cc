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
    auto source = types::source::Blob::create_unlocked(md.source().format, config().path, writer->relname, ofs, md.data_size());

    try {
        idx->index(md, ofs);
        // Invalidate the summary cache for this month
        scache.invalidate(md);
        p_df.commit();
        p_idx.commit();
        source->lock();
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
    auto source = types::source::Blob::create_unlocked(md.source().format, config().path, writer->relname, ofs, md.data_size());

    try {
        idx->replace(md, ofs);
        // Invalidate the summary cache for this month
        scache.invalidate(md);
        p_df.commit();
        p_idx.commit();
        source->lock();
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
    auto source = types::source::Blob::create_unlocked(md.source().format, config().path, writer->relname, ofs, md.data_size());

    try {
        // Try to acquire without replacing
        idx->index(md, ofs);
        // Invalidate the summary cache for this month
        scache.invalidate(md);
        p_df.commit();
        p_idx.commit();
        source->lock();
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
        source->lock();
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
    if (md.source().format != config().format)
        throw std::runtime_error("cannot acquire into dataset " + name() + ": data is in format " + md.source().format + " but the dataset only accepts " + config().format);

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
    Segment* writer = file(md, md.source().format);
    WIndex* idx = static_cast<WIndex*>(writer->payload);

    const types::source::Blob* source = md.has_source_blob();
    if (!source)
        throw std::runtime_error("cannot remove metadata from dataset, because it has no Blob source");

    if (source->basedir != config().path)
        throw std::runtime_error("cannot remove metadata from dataset: its basedir is " + source->basedir + " but this dataset is at " + config().path);

    // TODO: refuse if md is in the archive

    // Delete from DB, and get file name
    Pending p_del = idx->begin_transaction();
    idx->remove(source->offset);

    // Commit delete from DB
    p_del.commit();

    // reset source and dataset in the metadata
    md.unset_source();
    md.unset(TYPE_ASSIGNEDDATASET);

    scache.invalidate(md);
}

void Writer::flush()
{
    segmented::Writer::flush();
    segment_manager().foreach_cached([](Segment& s) {
        WIndex* idx = static_cast<WIndex*>(s.payload);
        idx->flush();
    });
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
    config().step().list_segments(config().path, config().format + ".index", Matcher(), [&](std::string&& s) {
        s.resize(s.size() - 6);
        seg_relpaths.emplace_back(move(s));
    });
    std::sort(seg_relpaths.begin(), seg_relpaths.end());
    for (const auto& relpath: seg_relpaths)
        dest(relpath);
}

void Checker::removeAll(dataset::Reporter& reporter, bool writable)
{
    acquire_lock();
    list_segments([&](const std::string& relpath) {
        if (writable)
        {
            size_t freed = removeSegment(relpath, true);
            reporter.segment_delete(name(), relpath, "deleted (" + std::to_string(freed) + " freed)");
        } else
            reporter.segment_delete(name(), relpath, "should be deleted");
    });
    segmented::Checker::removeAll(reporter, writable);
    release_lock();
}

segmented::State Checker::scan(dataset::Reporter& reporter, bool quick)
{
    segmented::State segments_state;

    list_segments([&](const std::string& relpath) {
        if (!sys::exists(str::joinpath(config().path, relpath)) && ! sys::exists(str::joinpath(config().path, relpath + ".gz")))
        {
            segments_state.insert(make_pair(relpath, segmented::SegmentState(SEGMENT_DELETED)));
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

    // Look for data files without indices next to them
    config().step().list_segments(config().path, config().format, Matcher(), [&](std::string&& relpath) {
        if (segments_state.find(relpath) != segments_state.end()) return;
        reporter.segment_info(name(), relpath, "segment found on disk but not in index");
        segments_state.insert(make_pair(relpath, segmented::SegmentState(SEGMENT_NEW)));
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

void Checker::check_issue51(dataset::Reporter& reporter, bool fix)
{
    // Broken metadata for each segment
    std::map<string, metadata::Collection> broken_mds;

    // Iterate all segments
    list_segments([&](const std::string& relpath) {
        WIndex idx(m_config, relpath);
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


void Checker::repack(dataset::Reporter& reporter, bool writable, unsigned test_flags)
{
    acquire_lock();
    segmented::Checker::repack(reporter, writable, test_flags);
    release_lock();
}

void Checker::check(dataset::Reporter& reporter, bool fix, bool quick)
{
    acquire_lock();
    segmented::Checker::check(reporter, fix, quick);
    release_lock();
}

void Checker::indexSegment(const std::string& relpath, metadata::Collection&& mds)
{
    string pathname = str::joinpath(config().path, relpath);
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
    //m_mft->acquire(relpath, mtime, sum);
    //m_mft->flush();
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

void Checker::rescanSegment(const std::string& relpath)
{
    string pathname = str::joinpath(config().path, relpath);
    //fprintf(stderr, "Checker::rescanSegment %s\n", pathname.c_str());

    // Temporarily uncompress the file for scanning
    unique_ptr<utils::compress::TempUnzip> tu;
    if (scan::isCompressed(pathname))
        tu.reset(new utils::compress::TempUnzip(pathname));

    // Collect the scan results in a metadata::Collector
    metadata::Collection mds;
    if (!scan::scan(pathname, mds.inserter_func()))
        throw std::runtime_error("cannot rescan " + pathname + ": file format unknown");
    //fprintf(stderr, "SCANNED %s: %zd\n", pathname.c_str(), mds.size());

    WIndex idx(m_config, relpath);

    // Lock away writes and reads
    Pending p = idx.begin_exclusive_transaction();

    // Remove from the index all data about the file
    idx.reset();

    // Scan the list of metadata, looking for duplicates and marking all
    // the duplicates except the last one as deleted
    IDMaker id_maker(idx.unique_codes());

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
    std::string basename = str::basename(relpath);
    for (const auto& i: finddupes)
    {
        const Metadata& md = *i.second;
        const source::Blob& blob = md.sourceBlob();
        try {
            if (str::basename(blob.filename) != basename)
                throw std::runtime_error("cannot rescan " + relpath + ": metadata points to the wrong file: " + blob.filename);
            idx.index(md, blob.offset);
        } catch (utils::sqlite::DuplicateInsert& di) {
            stringstream ss;
            ss << "cannot reindex " << basename << ": data item at offset " << blob.offset << " has a duplicate elsewhere in the dataset: manual fix is required";
            throw runtime_error(ss.str());
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


size_t Checker::repackSegment(const std::string& relpath, unsigned test_flags)
{
    WIndex idx(m_config, relpath);

    // Lock away writes and reads
    Pending p = idx.begin_exclusive_transaction();

    // Make a copy of the file with the right data in it, sorted by
    // reftime, and update the offsets in the index
    string pathname = str::joinpath(config().path, relpath);

    metadata::Collection mds;
    idx.scan(mds.inserter_func(), "reftime, offset");

    return reorder_segment_backend(idx, p, relpath, mds, test_flags);
}

size_t Checker::reorder_segment(const std::string& relpath, metadata::Collection& mds, unsigned test_flags)
{
    WIndex idx(m_config, relpath);

    // Lock away writes and reads
    Pending p = idx.begin_exclusive_transaction();

    return reorder_segment_backend(idx, p, relpath, mds, test_flags);
}

size_t Checker::reorder_segment_backend(WIndex& idx, Pending& p, const std::string& relpath, metadata::Collection& mds, unsigned test_flags)
{
    // Make a copy of the file with the right data in it, sorted by
    // reftime, and update the offsets in the index
    string pathname = str::joinpath(config().path, relpath);

    Pending p_repack = segment_manager().repack(relpath, mds, test_flags);

    // Reindex mds
    idx.reset();
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        idx.index(**i, source.offset);
    }

    size_t size_pre = sys::isdir(pathname) ? 0 : sys::size(pathname);

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

    size_t size_post = sys::isdir(pathname) ? 0 : sys::size(pathname);

    return size_pre - size_post;
}

size_t Checker::removeSegment(const std::string& relpath, bool withData)
{
    string idx_abspath = str::joinpath(config().path, relpath + ".index");
    size_t res = sys::size(idx_abspath);
    sys::unlink_ifexists(idx_abspath);
    return res + segmented::Checker::removeSegment(relpath, withData);
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

void Checker::test_make_overlap(const std::string& relpath, unsigned data_idx)
{
    throw std::runtime_error("test_make_overlap not yet implemented for iseg");
}

void Checker::test_make_hole(const std::string& relpath, unsigned data_idx)
{
    throw std::runtime_error("test_make_hole not yet implemented for iseg");
}

void Checker::test_corrupt_data(const std::string& relpath, unsigned data_idx)
{
    throw std::runtime_error("test_corrupt_data not yet implemented for iseg");
}

void Checker::test_truncate_data(const std::string& relpath, unsigned data_idx)
{
    throw std::runtime_error("test_truncate_data not yet implemented for iseg");
}

void Checker::test_swap_data(const std::string& relpath, unsigned d1_idx, unsigned d2_idx)
{
    metadata::Collection mds;
    WIndex idx(m_config, relpath);
    idx.scan(mds.inserter_func(), "offset");
    std::swap(mds[d1_idx], mds[d2_idx]);
    reorder_segment(relpath, mds);
}

void Checker::test_rename(const std::string& relpath, const std::string& new_relpath)
{
    throw std::runtime_error("test_rename not yet implemented for iseg");
}

void Checker::test_deindex(const std::string& relpath)
{
    throw std::runtime_error("test_corrupt_data not yet implemented for iseg");
}

}
}
}
