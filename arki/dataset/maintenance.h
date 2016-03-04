#ifndef ARKI_DATASET_MAINTENANCE_H
#define ARKI_DATASET_MAINTENANCE_H

/// dataset/maintenance - Dataset maintenance utilities

#include <arki/dataset/segment.h>
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
 * Print out the maintenance state for each segment
 */
struct Dumper
{
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
