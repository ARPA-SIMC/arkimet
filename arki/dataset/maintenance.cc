#include "config.h"

#include <arki/dataset/maintenance.h>
#include <arki/dataset/segmented.h>
#include <arki/dataset/archive.h>
#include <arki/metadata/collection.h>
#include <arki/dataset/step.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/compress.h>
#include <arki/utils/string.h>
#include <arki/utils/sys.h>
#include <arki/scan/any.h>
#include <arki/sort.h>
#include <arki/nag.h>
#include <algorithm>
#include <iostream>
#include <ostream>
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

static bool sorter(const std::string& a, const std::string& b)
{
	return b < a;
}

FindMissing::FindMissing(segment::state_func next, const std::vector<std::string>& files)
	: next(next), disk(files)
{
	// Sort backwards because we read from the end
	std::sort(disk.begin(), disk.end(), sorter);
}

void FindMissing::operator()(const std::string& file, segment::State state)
{
    while (not disk.empty() and disk.back() < file)
    {
        nag::verbose("FindMissing: %s is not in index", disk.back().c_str());
        next(disk.back(), state + FILE_TO_INDEX);
        disk.pop_back();
    }
    if (!disk.empty() && disk.back() == file)
    {
        // TODO: if requested, check for internal consistency
        // TODO: it requires to have an infrastructure for quick
        // TODO:   consistency checkers (like, "GRIB starts with GRIB
        // TODO:   and ends with 7777")

        disk.pop_back();
        next(file, state);
    }
    else // if (disk.empty() || disk.back() > file)
    {
        nag::verbose("FindMissing: %s has been deleted", file.c_str());
        next(file, state - FILE_TO_RESCAN + FILE_TO_DEINDEX);
    }
}

void FindMissing::end()
{
    while (not disk.empty())
    {
        nag::verbose("FindMissing: %s is not in index", disk.back().c_str());
        next(disk.back(), FILE_TO_INDEX);
        disk.pop_back();
    }
}

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


CheckAge::CheckAge(segment::state_func& next, const Step& step, int archive_age, int delete_age)
    : next(next), step(step), archive_threshold(0, 0, 0), delete_threshold(0, 0, 0)
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

void CheckAge::operator()(const std::string& file, segment::State state)
{
    if (archive_threshold.vals[0] == 0 and delete_threshold.vals[0] == 0)
        next(file, state);
    else
    {
        types::Time start_time;
        types::Time end_time;
#warning if the path is invalid, path_timespan throws, and the exception is not currently handled. Handle it setting the file in a "needs manual recovery" status
        step.path_timespan(file, start_time, end_time);
        if (delete_threshold.vals[0] != 0 && delete_threshold >= end_time)
        {
            nag::verbose("CheckAge: %s is old enough to be deleted", file.c_str());
            next(file, state + FILE_TO_DELETE);
        }
        else if (archive_threshold.vals[0] != 0 && archive_threshold >= end_time)
        {
            nag::verbose("CheckAge: %s is old enough to be archived", file.c_str());
            next(file, state + FILE_TO_ARCHIVE);
        }
        else
            next(file, state);
    }
}

void Dumper::operator()(const std::string& file, segment::State state)
{
    cerr << file << " " << state.to_string() << endl;
}

Tee::Tee(segment::state_func& one, segment::state_func& two) : one(one), two(two) {}
Tee::~Tee() {}
void Tee::operator()(const std::string& file, segment::State state)
{
	one(file, state);
	two(file, state);
}

// Agent

Agent::Agent(dataset::Reporter& reporter, SegmentedChecker& w)
    : reporter(reporter), w(w), lineStart(true)
{
}

// FailsafeRepacker

void FailsafeRepacker::operator()(const std::string& file, segment::State state)
{
    if (state.has(FILE_TO_INDEX)) ++m_count_deleted;
}

void FailsafeRepacker::end()
{
    if (m_count_deleted == 0) return;
    reporter.operation_report(w, "repack", "index is empty, skipping deletion of " + std::to_string(m_count_deleted) + " files.");
}

// MockRepacker

void MockRepacker::operator()(const std::string& relpath, segment::State state)
{
    if (state.has(FILE_TO_PACK) && !state.has(FILE_TO_DELETE))
    {
        reporter.segment_repack(w, relpath, "should be packed");
        ++m_count_packed;
    }
    if (state.has(FILE_TO_ARCHIVE))
    {
        reporter.segment_archive(w, relpath, "should be archived");
        ++m_count_archived;
    }
    if (state.has(FILE_TO_DELETE))
    {
        reporter.segment_delete(w, relpath, "should be deleted and removed from the index");
        ++m_count_deleted;
        ++m_count_deindexed;
    }
    if (state.has(FILE_TO_INDEX))
    {
        reporter.segment_delete(w, relpath, "should be deleted");
        ++m_count_deleted;
    }
    if (state.has(FILE_TO_DEINDEX))
    {
        reporter.segment_deindex(w, relpath, "should be removed from the index");
        ++m_count_deindexed;
    }
    if (state.has(FILE_TO_RESCAN))
    {
        reporter.segment_rescan(w, relpath, "should be rescanned");
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
    reporter.operation_report(w, "repack", str::join(", ", reports));
}

// MockFixer

void MockFixer::operator()(const std::string& relpath, segment::State state)
{
    if (state.has(FILE_TO_PACK))
    {
        reporter.segment_repack(w, relpath, "should be packed");
        ++m_count_packed;
    }
    if (state.has(FILE_TO_INDEX) || state.has(FILE_TO_RESCAN))
    {
        reporter.segment_rescan(w, relpath, "should be rescanned");
        ++m_count_rescanned;
    }
    if (state.has(FILE_TO_DEINDEX))
    {
        reporter.segment_deindex(w, relpath, "should be removed from the index");
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
    reporter.operation_report(w, "check", str::join(", ", reports));
}

// RealRepacker

void RealRepacker::operator()(const std::string& relpath, segment::State state)
{
    if (state.has(FILE_TO_PACK) && !state.has(FILE_TO_DELETE))
    {
        // Repack the file
        size_t saved = w.repackFile(relpath);
        reporter.segment_repack(w, relpath, "repacked (" + std::to_string(saved) + " freed)");
        ++m_count_packed;
        m_count_freed += saved;
    }
    if (state.has(FILE_TO_ARCHIVE))
    {
        // Create the target directory in the archive
        w.archiveFile(relpath);
        reporter.segment_archive(w, relpath, "archived");
        ++m_count_archived;
        m_touched_archive = true;
        m_redo_summary = true;
    }
    if (state.has(FILE_TO_DELETE))
    {
        // Delete obsolete files
        size_t size = w.removeFile(relpath, true);
        reporter.segment_delete(w, relpath, "deleted (" + std::to_string(size) + " freed)");
        ++m_count_deleted;
        ++m_count_deindexed;
        m_count_freed += size;
        m_redo_summary = true;
    }
    if (state.has(FILE_TO_INDEX))
    {
        // Delete all files not indexed
        size_t size = w.removeFile(relpath, true);
        reporter.segment_delete(w, relpath, "deleted (" + std::to_string(size) + " freed)");
        ++m_count_deleted;
        m_count_freed += size;
    }
    if (state.has(FILE_TO_DEINDEX))
    {
        // Remove from index those files that have been deleted
        w.removeFile(relpath, false);
        reporter.segment_deindex(w, relpath, "removed from index");
        ++m_count_deindexed;
        m_redo_summary = true;
    }
    if (state.is_ok()) ++m_count_ok;
}

void RealRepacker::end()
{
    // Finally, tidy up the database
    size_t size_vacuum = w.vacuum();

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
    reporter.operation_report(w, "repack", str::join(", ", reports));
}

// RealFixer

void RealFixer::operator()(const std::string& relpath, segment::State state)
{
    /* Packing is left to the repacker, during check we do not
     * mangle the data files
    case FILE_TO_PACK: {
            // Repack the file
            size_t saved = repack(w.path(), file, w.m_idx);
            log() << "packed " << file << " (" << saved << " saved)" << endl;
            ++m_count_packed;
            break;
    }
    */
    if (state.has(FILE_TO_INDEX) || state.has(FILE_TO_RESCAN))
    {
        w.rescanFile(relpath);
        reporter.segment_rescan(w, relpath, "rescanned");
        ++m_count_rescanned;
        m_redo_summary = true;
    }
    if (state.has(FILE_TO_DEINDEX))
    {
        // Remove from index those files that have been deleted
        w.removeFile(relpath, false);
        reporter.segment_deindex(w, relpath, "removed from the index");
        ++m_count_deindexed;
        m_redo_summary = true;
    }
    if (state.is_ok()) ++m_count_ok;
}

void RealFixer::end()
{
    // Finally, tidy up the database
    /*size_t size_vacuum =*/ w.vacuum();

    vector<string> reports;
    reports.emplace_back(nfiles(m_count_ok) + " ok");
    if (m_count_packed) reports.emplace_back(nfiles(m_count_packed) + " packed");
    if (m_count_rescanned) reports.emplace_back(nfiles(m_count_rescanned) + " rescanned");
    if (m_count_deindexed) reports.emplace_back(nfiles(m_count_deindexed) + " removed from index");
    reporter.operation_report(w, "check", str::join(", ", reports));
    //if (size_vacuum)
    //  logAdd() << size_vacuum << " bytes reclaimed cleaning the index";
}

}
}
}
