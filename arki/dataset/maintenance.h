#ifndef ARKI_DATASET_MAINTENANCE_H
#define ARKI_DATASET_MAINTENANCE_H

/// dataset/maintenance - Dataset maintenance utilities

#include <arki/dataset/fwd.h>
#include <arki/segment/data.h>
#include <string>
#include <sys/types.h>
#include <vector>

namespace arki {

namespace metadata {
class Collection;
}

namespace scan {
class Validator;
}

namespace dataset {
class Step;

namespace segmented {
class Checker;
class CheckerSegment;
} // namespace segmented

namespace maintenance {

/**
 * Print out the maintenance state for each segment
 */
struct Dumper
{
    void operator()(segmented::CheckerSegment& relpath, segment::State state);
};

/// Base class for all repackers and rebuilders
struct Agent
{
    dataset::Reporter& reporter;
    segmented::Checker& w;
    bool lineStart;
    unsigned test_flags;

    Agent(dataset::Reporter& reporter, segmented::Checker& w,
          unsigned test_flags = 0);
    Agent(const Agent&)            = delete;
    Agent& operator=(const Agent&) = delete;
    virtual ~Agent() {}

    virtual void operator()(segmented::CheckerSegment& segment,
                            segment::State state) = 0;

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

    void operator()(segmented::CheckerSegment& relpath,
                    segment::State state) override;
    void end() override;
};

/**
 * Simulate a repack and print information on what would have been done
 */
struct MockRepacker : public Agent
{
    using Agent::Agent;

    size_t m_count_ok        = 0;
    size_t m_count_packed    = 0;
    size_t m_count_archived  = 0;
    size_t m_count_deleted   = 0;
    size_t m_count_deindexed = 0;
    size_t m_count_rescanned = 0;

    void operator()(segmented::CheckerSegment& relpath,
                    segment::State state) override;
    void end() override;
};

/**
 * Simulate a repack and print information on what would have been done
 */
struct MockFixer : public Agent
{
    using Agent::Agent;

    size_t m_count_ok                  = 0;
    size_t m_count_packed              = 0;
    size_t m_count_rescanned           = 0;
    size_t m_count_deindexed           = 0;
    size_t m_count_manual_intervention = 0;

    void operator()(segmented::CheckerSegment& relpath,
                    segment::State state) override;
    void end() override;
};

/**
 * Perform real repacking
 */
struct RealRepacker : public Agent
{
    using Agent::Agent;

    size_t m_count_ok        = 0;
    size_t m_count_packed    = 0;
    size_t m_count_archived  = 0;
    size_t m_count_deleted   = 0;
    size_t m_count_deindexed = 0;
    size_t m_count_rescanned = 0;
    size_t m_count_freed     = 0;
    bool m_touched_archive   = false;
    bool m_redo_summary      = false;

    void operator()(segmented::CheckerSegment& relpath,
                    segment::State state) override;
    void end() override;
};

/**
 * Perform real repacking
 */
struct RealFixer : public Agent
{
    using Agent::Agent;

    size_t m_count_ok        = 0;
    size_t m_count_packed    = 0;
    size_t m_count_rescanned = 0;
    size_t m_count_deindexed = 0;
    bool m_touched_archive   = 0;
    bool m_redo_summary      = 0;

    void operator()(segmented::CheckerSegment& relpath,
                    segment::State state) override;
    void end() override;
};

} // namespace maintenance
} // namespace dataset
} // namespace arki
#endif
