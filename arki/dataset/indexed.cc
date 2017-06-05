#include "indexed.h"
#include "index.h"
#include "maintenance.h"
#include "step.h"
#include "reporter.h"
#include "arki/dataset/time.h"
#include "arki/scan/dir.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/core/time.h"
#include "arki/metadata/collection.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <algorithm>
#include <cstring>

using namespace std;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace dataset {

IndexedReader::~IndexedReader()
{
    delete m_idx;
}

bool IndexedReader::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    if (!LocalReader::query_data(q, dest))
        return false;
    if (!m_idx) return true;
    return m_idx->query_data(q, dest);
}

void IndexedReader::query_summary(const Matcher& matcher, Summary& summary)
{
    // Query the archives first
    LocalReader::query_summary(matcher, summary);
    if (!m_idx) return;
    // FIXME: this is cargo culted from the old ondisk2 reader: what is the use case for this?
    if (!m_idx->query_summary(matcher, summary))
        throw std::runtime_error("cannot query " + config().path + ": index could not be used");
}


IndexedWriter::~IndexedWriter()
{
    delete m_idx;
}


IndexedChecker::~IndexedChecker()
{
    delete m_idx;
}

segmented::State IndexedChecker::scan(dataset::Reporter& reporter, bool quick)
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
            state = SEGMENT_NEW;
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

    std::set<std::string> disk(scan::dir(config().path, true));

    // files: a, b, c,    e, f, g
    // index:       c, d, e, f, g

    for (auto& i: segments_state)
    {
        if (disk.erase(i.first) == 0)
        {
            // The file did not exist on disk
            reporter.segment_info(name(), i.first, "segment found in index but not on disk");
            i.second.state = i.second.state - SEGMENT_UNALIGNED + SEGMENT_DELETED;
        }
    }
    for (const auto& relpath : disk)
    {
        reporter.segment_info(name(), relpath, "segment found on disk but not in index");
        segments_state.insert(make_pair(relpath, segmented::SegmentState(SEGMENT_NEW)));
    }


    //
    // Check if segments are old enough to be deleted or archived
    //

    Time archive_threshold(0, 0, 0);
    Time delete_threshold(0, 0, 0);
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

// TODO: during checks, run file:///usr/share/doc/sqlite3-doc/pragma.html#debug
// and delete the index if it fails

void IndexedChecker::removeAll(Reporter& reporter, bool writable)
{
    m_idx->list_segments([&](const std::string& relpath) {
        if (writable)
        {
            size_t freed = removeSegment(relpath, true);
            reporter.segment_delete(name(), relpath, "deleted (" + std::to_string(freed) + " freed)");
        } else
            reporter.segment_delete(name(), relpath, "should be deleted");
    });
    segmented::Checker::removeAll(reporter, writable);
}

void IndexedChecker::check_issue51(dataset::Reporter& reporter, bool fix)
{
    // Broken metadata for each segment
    std::map<string, metadata::Collection> broken_mds;

    // Iterate all segments
    m_idx->scan_files([&](const std::string& relpath, segment::State state, const metadata::Collection& mds) {
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

void IndexedChecker::test_make_overlap(const std::string& relpath, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    segment_manager().get_segment(relpath)->test_make_overlap(mds, data_idx);
    m_idx->test_make_overlap(relpath, data_idx);
}

void IndexedChecker::test_make_hole(const std::string& relpath, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    segment_manager().get_segment(relpath)->test_make_hole(mds, data_idx);
    m_idx->test_make_hole(relpath, data_idx);
}

void IndexedChecker::test_corrupt_data(const std::string& relpath, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    segment_manager().get_segment(relpath)->test_corrupt(mds, data_idx);
}

void IndexedChecker::test_truncate_data(const std::string& relpath, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    segment_manager().get_segment(relpath)->test_truncate(mds, data_idx);
}

void IndexedChecker::test_swap_data(const std::string& relpath, unsigned d1_idx, unsigned d2_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    std::swap(mds[d1_idx], mds[d2_idx]);

    reorder_segment(relpath, mds);
}

void IndexedChecker::test_rename(const std::string& relpath, const std::string& new_relpath)
{
    m_idx->test_rename(relpath, new_relpath);
    sys::rename(str::joinpath(config().path, relpath), str::joinpath(config().path, new_relpath));
}

void IndexedChecker::test_deindex(const std::string& relpath)
{
    m_idx->test_deindex(relpath);
}

}
}

