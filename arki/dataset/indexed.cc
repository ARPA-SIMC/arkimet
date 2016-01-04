#include "indexed.h"
#include "index.h"
#include "maintenance.h"
#include "step.h"
#include "arki/scan/dir.h"
#include "arki/metadata/collection.h"
#include <algorithm>

using namespace std;

namespace arki {
namespace dataset {

static time_t override_now = 0;

TestOverrideCurrentDateForMaintenance::TestOverrideCurrentDateForMaintenance(time_t ts)
{
    old_ts = override_now;
    override_now = ts;
}
TestOverrideCurrentDateForMaintenance::~TestOverrideCurrentDateForMaintenance()
{
    override_now = old_ts;
}


namespace {

struct SegmentState
{
    // Segment relative path in the dataset
    std::string relpath;
    // Segment state
    segment::State state;
    // Minimum reference time of data that can fit in the segment
    types::Time md_begin;
    // Maximum reference time of data that can fit in the segment
    types::Time md_until;

    SegmentState(const std::string& relpath, segment::State state)
        : relpath(relpath), state(state), md_begin(0, 0, 0), md_until(0, 0, 0) {}
    SegmentState(const std::string& relpath, segment::State state, const types::Time& md_begin, const types::Time& md_until)
        : relpath(relpath), state(state), md_begin(md_begin), md_until(md_until) {}
    SegmentState(const SegmentState&) = default;
    SegmentState(SegmentState&&) = default;
};

/**
 * Check the age of segments in a dataset, to detect those that need to be
 * deleted or archived. It adds SEGMENT_DELETE_AGE or SEGMENT_ARCHIVE_AGE as needed.
 */
struct CheckAge
{
    const dataset::Checker& checker;
    types::Time archive_threshold;
    types::Time delete_threshold;

    CheckAge(const dataset::Checker& checker, int archive_age=-1, int delete_age=-1)
        : checker(checker), archive_threshold(0, 0, 0), delete_threshold(0, 0, 0)
    {
        time_t now = override_now ? override_now : time(NULL);
        struct tm t;

        // Go to the beginning of the day
        now -= (now % (3600*24));

        if (archive_age != -1)
        {
            time_t arc_thr = now - archive_age * 3600 * 24;
            gmtime_r(&arc_thr, &t);
            archive_threshold.set(t);
        }
        if (delete_age != -1)
        {
            time_t del_thr = now - delete_age * 3600 * 24;
            gmtime_r(&del_thr, &t);
            delete_threshold.set(t);
        }
    }

    segment::State check(const SegmentState& seginfo)
    {
        if (archive_threshold.vals[0] == 0 and delete_threshold.vals[0] == 0)
            return seginfo.state;

        if (delete_threshold.vals[0] != 0 && delete_threshold >= seginfo.md_until)
        {
            nag::verbose("%s:%s: old enough to be deleted", checker.name().c_str(), seginfo.relpath.c_str());
            return seginfo.state + SEGMENT_DELETE_AGE;
        }
        else if (archive_threshold.vals[0] != 0 && archive_threshold >= seginfo.md_until)
        {
            nag::verbose("%s:%s: old enough to be archived", checker.name().c_str(), seginfo.relpath.c_str());
            return seginfo.state + SEGMENT_ARCHIVE_AGE;
        }
        else
            return seginfo.state;
    }
};

/**
 * Scan the files actually present inside a directory and compare them with a
 * stream of files known by an indexed, detecting new files and files that have
 * been removed.
 *
 * Check needs to be called with relpaths in ascending alphabetical order.
 *
 * Results are sent to the \a next state_func. \a next can be called more times
 * than check has been called, in case of segments found on disk that the index
 * does not know about.
 *
 * end() needs to be called after all index information has been sent to
 * check(), in order to generate data for the remaining files found on disk and
 * not in the index.
 */
struct FindMissing
{
    const dataset::Checker& checker;
    std::vector<std::string> disk;
    std::vector<SegmentState> results;

    FindMissing(const dataset::Checker& checker, const std::string& root)
        : checker(checker), disk(scan::dir(root, true))
    {
        // Sort backwards because we read from the end
        auto sorter = [](const std::string& a, const std::string& b) { return b < a; };
        std::sort(disk.begin(), disk.end(), sorter);
    }
    FindMissing(const FindMissing&) = delete;
    FindMissing& operator=(const FindMissing&) = delete;

    // files: a, b, c,    e, f, g
    // index:       c, d, e, f, g

    void check(const std::string& relpath, segment::State state, types::Time& md_begin, types::Time& md_until)
    {
        while (not disk.empty() and disk.back() < relpath)
        {
            nag::verbose("%s:%s: not in index", checker.name().c_str(), disk.back().c_str());
            results.emplace_back(disk.back(), SEGMENT_NEW);
            disk.pop_back();
        }
        if (!disk.empty() && disk.back() == relpath)
        {
            disk.pop_back();
            results.emplace_back(relpath, state, md_begin, md_until);
        }
        else // if (disk.empty() || disk.back() > file)
        {
            nag::verbose("%s:%s: not on disk", checker.name().c_str(), relpath.c_str());
            results.emplace_back(relpath, state - SEGMENT_UNALIGNED + SEGMENT_DELETED, md_begin, md_until);
        }
    }

    void end()
    {
        while (not disk.empty())
        {
            nag::verbose("%s:%s is not in index", checker.name().c_str(), disk.back().c_str());
            results.emplace_back(disk.back(), SEGMENT_NEW);
            disk.pop_back();
        }
    }
};

}

IndexedReader::IndexedReader(const ConfigFile& cfg)
    : SegmentedReader(cfg)
{
}

IndexedReader::~IndexedReader()
{
    delete m_idx;
}

void IndexedReader::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    LocalReader::query_data(q, dest);
    if (!m_idx) return;
    // FIXME: this is cargo culted from the old ondisk2 reader: what is the use case for this?
    if (!m_idx->query_data(q, dest))
        throw std::runtime_error("cannot query " + m_path + ": index could not be used");
}

void IndexedReader::query_summary(const Matcher& matcher, Summary& summary)
{
    // Query the archives first
    LocalReader::query_summary(matcher, summary);
    if (!m_idx) return;
    // FIXME: this is cargo culted from the old ondisk2 reader: what is the use case for this?
    if (!m_idx->query_summary(matcher, summary))
        throw std::runtime_error("cannot query " + m_path + ": index could not be used");
}


IndexedWriter::IndexedWriter(const ConfigFile& cfg)
    : SegmentedWriter(cfg)
{
}

IndexedWriter::~IndexedWriter()
{
    delete m_idx;
}


IndexedChecker::IndexedChecker(const ConfigFile& cfg)
    : SegmentedChecker(cfg)
{
}

IndexedChecker::~IndexedChecker()
{
    delete m_idx;
}

void IndexedChecker::maintenance(segment::state_func v, bool quick)
{
    // TODO: run file:///usr/share/doc/sqlite3-doc/pragma.html#debug
    // and delete the index if it fails

    // First pass: detect files that have been added or removed compared to the
    // index

    // FindMissing accumulates states instead of acting on files right away;
    // this has the desirable side effect of avoiding modifying the index while
    // it is being iterated
    FindMissing fm(*this, m_path);
    m_idx->scan_files([&](const std::string& relpath, segment::State state, const metadata::Collection& mds) {
        // Compute the span of reftimes inside the segment
        unique_ptr<types::Time> md_begin;
        unique_ptr<types::Time> md_until;
        if (mds.empty())
        {
            nag::verbose("%s:%s: index knows of this segment but contains no data for it", name().c_str(), relpath.c_str());
            md_begin.reset(new types::Time(0, 0, 0));
            md_until.reset(new types::Time(0, 0, 0));
            state = SEGMENT_UNALIGNED;
        } else {
            if (!mds.expand_date_range(md_begin, md_until))
            {
                nag::verbose("%s:%s: index contains data without reference time information", name().c_str(), relpath.c_str());
                state = SEGMENT_CORRUPTED;
                md_begin.reset(new types::Time(0, 0, 0));
                md_until.reset(new types::Time(0, 0, 0));
            } else if (m_step) {
                // If we have a step, ensure that the reftime span fits inside the segment step
                types::Time seg_begin;
                types::Time seg_until;
                if (m_step->path_timespan(relpath, seg_begin, seg_until))
                {
                    if (*md_begin < seg_begin || *md_until > seg_until)
                    {
                        nag::verbose("%s:%s: segment contents do not fit inside the step of this dataset", name().c_str(), relpath.c_str());
                        state = SEGMENT_CORRUPTED;
                    }
                    // Expand segment timespan to the full possible segment timespan
                    *md_begin = seg_begin;
                    *md_until = seg_until;
                } else {
                    nag::verbose("%s:%s: segment name does not fit the step of this dataset", name().c_str(), relpath.c_str());
                    state = SEGMENT_CORRUPTED;
                }
            }
        }

        if (state.is_ok())
            state = m_segment_manager->check(relpath, mds, quick);
        fm.check(relpath, state, *md_begin, *md_until);
    });
    fm.end();

    // Second pass: checking if segments are old enough to be deleted or
    // archived

    CheckAge ca(*this, m_archive_age, m_delete_age);
    for (const auto& seginfo: fm.results)
        v(seginfo.relpath, ca.check(seginfo));

    SegmentedChecker::maintenance(v, quick);
}

void IndexedChecker::removeAll(Reporter& reporter, bool writable)
{
    m_idx->list_segments([&](const std::string& relpath) {
        if (writable)
        {
            size_t freed = removeFile(relpath, true);
            reporter.segment_delete(*this, relpath, "deleted (" + std::to_string(freed) + " freed)");
        } else
            reporter.segment_delete(*this, relpath, "should be deleted");
    });
    SegmentedChecker::removeAll(reporter, writable);
}

}
}

