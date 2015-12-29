#ifndef ARKI_DATASET_MAINTENANCE_H
#define ARKI_DATASET_MAINTENANCE_H

/// dataset/maintenance - Dataset maintenance utilities

#include <arki/dataset/segment.h>
#include <arki/types/time.h>
#include <string>
#include <vector>
#include <sys/types.h>

namespace arki {

namespace metadata {
class Collection;
}

namespace scan {
class Validator;
}

namespace dataset {
class Reporter;
class SegmentedChecker;
class Step;

namespace maintenance {

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
    segment::state_func& next;
    std::vector<std::string> disk;

    FindMissing(const std::string& root, segment::state_func next);
    FindMissing(const FindMissing&) = delete;
    FindMissing& operator=(const FindMissing&) = delete;

    // files: a, b, c,    e, f, g
    // index:       c, d, e, f, g

    void check(const std::string& relpath, segment::State state);
    void end();
};

/**
 * Check the age of segments in a dataset, to detect those that need to be
 * deleted or archived. It adds SEGMENT_DELETE_AGE or SEGMENT_ARCHIVE_AGE as needed.
 */
struct CheckAge
{
    typedef std::function<bool(const std::string&, types::Time&, types::Time&)> segment_timespan_func;
    segment::state_func next;
    segment_timespan_func get_segment_timespan;
    types::Time archive_threshold;
    types::Time delete_threshold;

    CheckAge(segment::state_func next, segment_timespan_func get_segment_timespan, int archive_age=-1, int delete_age=-1);

    void operator()(const std::string& relpath, segment::State state);
};

/**
 * Temporarily override the current date used to check data age.
 *
 * This is used to be able to write unit tests that run the same independently
 * of when they are run.
 */
struct TestOverrideCurrentDateForMaintenance
{
    time_t old_ts;

    TestOverrideCurrentDateForMaintenance(time_t ts);
    ~TestOverrideCurrentDateForMaintenance();
};


/**
 * Print out the maintenance state for each segment
 */
struct Dumper
{
    void operator()(const std::string& relpath, segment::State state);
};

struct Tee
{
    segment::state_func& one;
    segment::state_func& two;

    Tee(segment::state_func& one, segment::state_func& two);
    virtual ~Tee();
    void operator()(const std::string& relpath, segment::State state);
};

/// Base class for all repackers and rebuilders
struct Agent
{
    dataset::Reporter& reporter;
    SegmentedChecker& w;
    bool lineStart;

    Agent(dataset::Reporter& reporter, SegmentedChecker& w);
    Agent(const Agent&) = delete;
    Agent& operator=(const Agent&) = delete;

    virtual void operator()(const std::string& relpath, segment::State state) = 0;

    virtual void end() {}
};

/**
 * Repacker used when some failsafe is triggered.
 * 
 * It only reports how many files would be deleted.
 */
struct FailsafeRepacker : public Agent
{
    using Agent::Agent;

    size_t m_count_deleted = 0;

    void operator()(const std::string& relpath, segment::State state);
    void end();
};

/**
 * Simulate a repack and print information on what would have been done
 */
struct MockRepacker : public Agent
{
    using Agent::Agent;

    size_t m_count_ok = 0;
    size_t m_count_packed = 0;
    size_t m_count_archived = 0;
    size_t m_count_deleted = 0;
    size_t m_count_deindexed = 0;
    size_t m_count_rescanned = 0;

    void operator()(const std::string& relpath, segment::State state);
    void end();
};

/**
 * Simulate a repack and print information on what would have been done
 */
struct MockFixer : public Agent
{
    using Agent::Agent;

    size_t m_count_ok = 0;
    size_t m_count_packed = 0;
    size_t m_count_rescanned = 0;
    size_t m_count_deindexed = 0;

    void operator()(const std::string& relpath, segment::State state);
    void end();
};

/**
 * Perform real repacking
 */
struct RealRepacker : public Agent
{
    using Agent::Agent;

    size_t m_count_ok = 0;
    size_t m_count_packed = 0;
    size_t m_count_archived = 0;
    size_t m_count_deleted = 0;
    size_t m_count_deindexed = 0;
    size_t m_count_rescanned = 0;
    size_t m_count_freed = 0;
    bool m_touched_archive = false;
    bool m_redo_summary = false;

    void operator()(const std::string& relpath, segment::State state);
    void end();
};

/**
 * Perform real repacking
 */
struct RealFixer : public Agent
{
    using Agent::Agent;

    size_t m_count_ok = 0;
    size_t m_count_packed = 0;
    size_t m_count_rescanned = 0;
    size_t m_count_deindexed = 0;
    bool m_touched_archive = 0;
    bool m_redo_summary = 0;

    void operator()(const std::string& relpath, segment::State state);
    void end();
};

}
}
}
#endif
