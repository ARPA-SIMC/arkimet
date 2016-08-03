#include "indexed.h"
#include "index.h"
#include "maintenance.h"
#include "step.h"
#include "reporter.h"
#include "arki/scan/dir.h"
#include "arki/core/time.h"
#include "arki/metadata/collection.h"
#include <algorithm>

using namespace std;
using arki::core::Time;

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
        throw std::runtime_error("cannot query " + config().path + ": index could not be used");
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

SegmentsState IndexedChecker::scan(dataset::Reporter& reporter, bool quick)
{
    SegmentsState segments_state;

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
            state = SEGMENT_UNALIGNED;
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

        segments_state.insert(make_pair(relpath, dataset::SegmentState(state, *md_begin, *md_until)));
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
        segments_state.insert(make_pair(relpath, dataset::SegmentState(SEGMENT_NEW)));
    }


    //
    // Check if segments are old enough to be deleted or archived
    //

    Time archive_threshold(0, 0, 0);
    Time delete_threshold(0, 0, 0);
    time_t now = override_now ? override_now : time(nullptr);
    struct tm t;

    // Go to the beginning of the day
    now -= (now % (3600*24));

    if (config().archive_age != -1)
    {
        time_t arc_thr = now - config().archive_age * 3600 * 24;
        gmtime_r(&arc_thr, &t);
        archive_threshold.set(t);
    }
    if (config().delete_age != -1)
    {
        time_t del_thr = now - config().delete_age * 3600 * 24;
        gmtime_r(&del_thr, &t);
        delete_threshold.set(t);
    }

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
    SegmentedChecker::removeAll(reporter, writable);
}

}
}

