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
 * MaintFileVisitor that feeds a MaintFileVisitor with TO_INDEX status.
 *
 * The input feed is assumed to come from the index, and is checked against the
 * files found on disk in order to detect files that are on disk but not in the
 * index.
 */
struct FindMissing
{
    segment::state_func& next;
    std::vector<std::string> disk;

    FindMissing(segment::state_func next, const std::vector<std::string>& files);

    // files: a, b, c,    e, f, g
    // index:       c, d, e, f, g

    void operator()(const std::string& relpath, segment::State state);
    void end();
};

/**
 * Check the age of segments in a dataset, to detect those that need to be
 * deleted or archived. It adds FILE_TO_DELETE or FILE_TO_ARCHIVE as needed.
 */
struct CheckAge
{
    segment::state_func& next;
    const Step& step;
    types::Time archive_threshold;
    types::Time delete_threshold;

    CheckAge(segment::state_func& next, const Step& step, int archive_age=-1, int delete_age=-1);

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
