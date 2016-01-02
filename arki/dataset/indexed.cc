#include "indexed.h"
#include "index.h"
#include "maintenance.h"
#include "step.h"
#include "arki/scan/dir.h"
#include <algorithm>

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
    std::string relpath;
    segment::State state;

    SegmentState(const std::string& relpath, segment::State state) : relpath(relpath), state(state) {}
    SegmentState(const SegmentState&) = default;
    SegmentState(SegmentState&&) = default;
};

/**
 * Check the age of segments in a dataset, to detect those that need to be
 * deleted or archived. It adds SEGMENT_DELETE_AGE or SEGMENT_ARCHIVE_AGE as needed.
 */
struct CheckAge
{
    typedef std::function<bool(const std::string&, types::Time&, types::Time&)> segment_timespan_func;
    segment_timespan_func get_segment_timespan;
    types::Time archive_threshold;
    types::Time delete_threshold;

    CheckAge(int archive_age=-1, int delete_age=-1)
        : archive_threshold(0, 0, 0), delete_threshold(0, 0, 0)
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

    segment::State check(const std::string& relpath, segment::State state)
    {
        if (archive_threshold.vals[0] == 0 and delete_threshold.vals[0] == 0)
            return state;

        types::Time start_time;
        types::Time end_time;
        if (!get_segment_timespan(relpath, start_time, end_time))
        {
            nag::verbose("CheckAge: cannot detect the timespan of segment %s", relpath.c_str());
            return state + SEGMENT_CORRUPTED;
        }
        else if (delete_threshold.vals[0] != 0 && delete_threshold >= end_time)
        {
            nag::verbose("CheckAge: %s is old enough to be deleted", relpath.c_str());
            return state + SEGMENT_DELETE_AGE;
        }
        else if (archive_threshold.vals[0] != 0 && archive_threshold >= end_time)
        {
            nag::verbose("CheckAge: %s is old enough to be archived", relpath.c_str());
            return state + SEGMENT_ARCHIVE_AGE;
        }
        else
            return state;
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
    std::vector<std::string> disk;
    std::vector<SegmentState> results;

    FindMissing(const std::string& root)
        : disk(scan::dir(root, true))
    {
        // Sort backwards because we read from the end
        auto sorter = [](const std::string& a, const std::string& b) { return b < a; };
        std::sort(disk.begin(), disk.end(), sorter);
    }
    FindMissing(const FindMissing&) = delete;
    FindMissing& operator=(const FindMissing&) = delete;

    // files: a, b, c,    e, f, g
    // index:       c, d, e, f, g

    void check(const std::string& relpath, segment::State state)
    {
        while (not disk.empty() and disk.back() < relpath)
        {
            nag::verbose("FindMissing: %s is not in index", disk.back().c_str());
            results.emplace_back(disk.back(), SEGMENT_NEW);
            disk.pop_back();
        }
        if (!disk.empty() && disk.back() == relpath)
        {
            disk.pop_back();
            results.emplace_back(relpath, state);
        }
        else // if (disk.empty() || disk.back() > file)
        {
            nag::verbose("FindMissing: %s has been deleted", relpath.c_str());
            results.emplace_back(relpath, state - SEGMENT_UNALIGNED + SEGMENT_DELETED);
        }
    }

    void end()
    {
        while (not disk.empty())
        {
            nag::verbose("FindMissing: %s is not in index", disk.back().c_str());
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
    FindMissing fm(m_path);
    m_idx->scan_files([&](const std::string& relname, segment::State state, const metadata::Collection& mds) {
        if (state.is_ok())
            state = m_segment_manager->check(relname, mds, quick);
        fm.check(relname, state);
    });
    fm.end();


    // Second pass: checking if segments are old enough to be deleted or
    // archived

    CheckAge ca(m_archive_age, m_delete_age);
    if (m_step)
    {
        // If we have a step, try and use it to get the timespan without hitting the index
        ca.get_segment_timespan = [&](const std::string& relpath, types::Time& begin, types::Time& until) {
            if (m_step->path_timespan(relpath, begin, until))
                return true;
            return m_idx->segment_timespan(relpath, begin, until);
        };
    } else {
        ca.get_segment_timespan = [&](const std::string& relpath, types::Time& begin, types::Time& until) {
            return m_idx->segment_timespan(relpath, begin, until);
        };
    }

    for (const auto& i: fm.results)
        v(i.relpath, ca.check(i.relpath, i.state));

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

