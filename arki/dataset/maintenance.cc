#include "config.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segmented.h"
#include "arki/dataset/archive.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/sort.h"
#include "arki/dataset/step.h"
#include "arki/dataset/reporter.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include <algorithm>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctime>
#include <cstdio>

// Be compatible with systems without O_NOATIME
#ifndef O_NOATIME
#  define O_NOATIME 0
#endif

using namespace std;
using namespace arki::utils;

template<typename T>
static std::string nfiles(const T& val)
{
    stringstream ss;
    if (val == 1)
        ss << "1 file";
    else
        ss << val << " files";
    return ss.str();
}

namespace arki {
namespace dataset {
namespace maintenance {

void Dumper::operator()(segmented::CheckerSegment& file, segment::State state)
{
    nag::warning("%s %s", file.path_relative().c_str(), state.to_string().c_str());
}

// Agent

Agent::Agent(dataset::Reporter& reporter, segmented::Checker& w, unsigned test_flags)
    : reporter(reporter), w(w), lineStart(true), test_flags(test_flags)
{
}

// FailsafeRepacker

void FailsafeRepacker::operator()(segmented::CheckerSegment& file, segment::State state)
{
    if (state.has(segment::SEGMENT_DELETED)) ++m_count_deleted;
}

void FailsafeRepacker::end()
{
    if (m_count_deleted == 0) return;
    reporter.operation_report(w.name(), "repack", "index is empty, skipping deletion of " + std::to_string(m_count_deleted) + " files.");
}

// MockRepacker

void MockRepacker::operator()(segmented::CheckerSegment& segment, segment::State state)
{
    if (state.has(segment::SEGMENT_DIRTY) && !state.has(segment::SEGMENT_DELETE_AGE))
    {
        reporter.segment_repack(w.name(), segment.path_relative(), "should be packed");
        ++m_count_packed;
    }
    if (state.has(segment::SEGMENT_ARCHIVE_AGE))
    {
        reporter.segment_archive(w.name(), segment.path_relative(), "should be archived");
        ++m_count_archived;
    }
    if (state.has(segment::SEGMENT_DELETE_AGE))
    {
        reporter.segment_delete(w.name(), segment.path_relative(), "should be deleted and removed from the index");
        ++m_count_deleted;
        ++m_count_deindexed;
    }
    if (state.has(segment::SEGMENT_DELETED))
    {
        reporter.segment_delete(w.name(), segment.path_relative(), "should be deleted");
        ++m_count_deleted;
    }
    if (state.has(segment::SEGMENT_MISSING))
    {
        reporter.segment_deindex(w.name(), segment.path_relative(), "should be removed from the index");
        ++m_count_deindexed;
    }
    if (state.has(segment::SEGMENT_UNALIGNED))
    {
        reporter.segment_rescan(w.name(), segment.path_relative(), "should be rescanned");
        ++m_count_rescanned;
    }
    if (state.is_ok()) ++m_count_ok;
}

void MockRepacker::end()
{
    vector<string> reports;
    reports.emplace_back(nfiles(m_count_ok) + " ok");
    if (m_count_packed) reports.emplace_back(nfiles(m_count_packed) + " should be packed");
    if (m_count_archived) reports.emplace_back(nfiles(m_count_archived) + " should be archived");
    if (m_count_deleted) reports.emplace_back(nfiles(m_count_deleted) + " should be deleted");
    if (m_count_deindexed) reports.emplace_back(nfiles(m_count_deindexed) + " should be removed from the index");
    if (m_count_rescanned) reports.emplace_back(nfiles(m_count_rescanned) + " should be rescanned");
    reporter.operation_report(w.name(), "repack", str::join(", ", reports));
}

// MockFixer

void MockFixer::operator()(segmented::CheckerSegment& segment, segment::State state)
{
    if (state.has(segment::SEGMENT_DIRTY))
    {
        reporter.segment_repack(w.name(), segment.path_relative(), "should be packed");
        ++m_count_packed;
    }
    if (state.has(segment::SEGMENT_UNALIGNED))
    {
        reporter.segment_rescan(w.name(), segment.path_relative(), "should be rescanned");
        ++m_count_rescanned;
    }
    if (state.has(segment::SEGMENT_MISSING))
    {
        reporter.segment_deindex(w.name(), segment.path_relative(), "should be removed from the index");
        ++m_count_deindexed;
    }
    if (state.is_ok()) ++m_count_ok;
}

void MockFixer::end()
{
    vector<string> reports;
    reports.emplace_back(nfiles(m_count_ok) + " ok");
    if (m_count_packed) reports.emplace_back(nfiles(m_count_packed) + " should be packed");
    if (m_count_rescanned) reports.emplace_back(nfiles(m_count_rescanned) + " should be rescanned");
    if (m_count_deindexed) reports.emplace_back(nfiles(m_count_deindexed) + " should be removed from the index");
    reporter.operation_report(w.name(), "check", str::join(", ", reports));
}

// RealRepacker

void RealRepacker::operator()(segmented::CheckerSegment& segment, segment::State state)
{
    if (test_flags & segment::RepackConfig::TEST_MISCHIEF_MOVE_DATA) state = state + segment::SEGMENT_DIRTY;

    if (state.has(segment::SEGMENT_DIRTY) && !state.has(segment::SEGMENT_DELETE_AGE))
    {
        // Repack the file
        size_t saved = segment.repack(test_flags);
        reporter.segment_repack(w.name(), segment.path_relative(), "repacked (" + std::to_string(saved) + " freed)");
        ++m_count_packed;
        m_count_freed += saved;
    }
    if (state.has(segment::SEGMENT_ARCHIVE_AGE))
    {
        // Create the target directory in the archive
        segment.archive();
        reporter.segment_archive(w.name(), segment.path_relative(), "archived");
        ++m_count_archived;
        m_touched_archive = true;
        m_redo_summary = true;
    }
    if (state.has(segment::SEGMENT_DELETE_AGE))
    {
        // Delete obsolete files
        size_t size = segment.remove(true);
        reporter.segment_delete(w.name(), segment.path_relative(), "deleted (" + std::to_string(size) + " freed)");
        ++m_count_deleted;
        ++m_count_deindexed;
        m_count_freed += size;
        m_redo_summary = true;
    }
    if (state.has(segment::SEGMENT_DELETED))
    {
        // Delete all files not indexed
        size_t size = segment.remove(true);
        reporter.segment_delete(w.name(), segment.path_relative(), "deleted (" + std::to_string(size) + " freed)");
        ++m_count_deleted;
        m_count_freed += size;
    }
    if (state.has(segment::SEGMENT_MISSING))
    {
        // Remove from index those files that have been deleted
        segment.remove(false);
        reporter.segment_deindex(w.name(), segment.path_relative(), "removed from index");
        ++m_count_deindexed;
        m_redo_summary = true;
    }
    if (state.is_ok()) ++m_count_ok;
}

void RealRepacker::end()
{
    // Finally, tidy up the database
    size_t size_vacuum = w.vacuum(reporter);

    vector<string> reports;
    reports.emplace_back(nfiles(m_count_ok) + " ok");
    if (m_count_packed) reports.emplace_back(nfiles(m_count_packed) + " packed");
    if (m_count_archived) reports.emplace_back(nfiles(m_count_archived) + " archived");
    if (m_count_deleted) reports.emplace_back(nfiles(m_count_deleted) + " deleted");
    if (m_count_deindexed) reports.emplace_back(nfiles(m_count_deindexed) + " removed from index");
    if (m_count_rescanned) reports.emplace_back(nfiles(m_count_rescanned) + " rescanned");
    if (size_vacuum)
    {
        //logAdd() + size_vacuum << " bytes reclaimed on the index";
        m_count_freed += size_vacuum;
    }
    if (m_count_freed > 0) reports.emplace_back(std::to_string(m_count_freed) + " total bytes freed");
    reporter.operation_report(w.name(), "repack", str::join(", ", reports));
}

// RealFixer

void RealFixer::operator()(segmented::CheckerSegment& segment, segment::State state)
{
    /* Packing is left to the repacker, during check we do not
     * mangle the data files
     case segment::SEGMENT_DIRTY: {
            // Repack the file
            size_t saved = repack(w.path(), file, w.m_idx);
            log() << "packed " << file << " (" << saved << " saved)" << endl;
            ++m_count_packed;
            break;
    }
    */
    if (state.has(segment::SEGMENT_UNALIGNED))
    {
        segment.rescan(reporter);
        reporter.segment_rescan(w.name(), segment.path_relative(), "rescanned");
        ++m_count_rescanned;
        m_redo_summary = true;
    }
    if (state.has(segment::SEGMENT_MISSING))
    {
        // Remove from index those files that have been deleted
        segment.remove(false);
        reporter.segment_deindex(w.name(), segment.path_relative(), "removed from the index");
        ++m_count_deindexed;
        m_redo_summary = true;
    }
    if (state.is_ok()) ++m_count_ok;
}

void RealFixer::end()
{
    vector<string> reports;
    reports.emplace_back(nfiles(m_count_ok) + " ok");
    if (m_count_packed) reports.emplace_back(nfiles(m_count_packed) + " packed");
    if (m_count_rescanned) reports.emplace_back(nfiles(m_count_rescanned) + " rescanned");
    if (m_count_deindexed) reports.emplace_back(nfiles(m_count_deindexed) + " removed from index");
    reporter.operation_report(w.name(), "check", str::join(", ", reports));
}

}
}
}
