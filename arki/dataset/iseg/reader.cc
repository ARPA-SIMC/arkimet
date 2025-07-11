#include "reader.h"
#include "arki/dataset/step.h"
#include "arki/query.h"
#include "arki/query/progress.h"
#include "arki/segment/iseg/index.h"
#include "arki/summary.h"
#include "arki/utils/sys.h"
#include <algorithm>

using namespace std;
using namespace arki::utils;
using arki::segment::iseg::RIndex;

namespace arki {
namespace dataset {
namespace iseg {

Reader::Reader(std::shared_ptr<iseg::Dataset> dataset)
    : DatasetAccess(dataset), scache(dataset->summary_cache_pathname)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);
    scache.openRW();
}

Reader::~Reader() {}

std::string Reader::type() const { return "iseg"; }

bool Reader::is_dataset(const std::filesystem::path& dir) { return true; }

bool Reader::list_segments(
    const Matcher& matcher,
    std::function<bool(std::shared_ptr<arki::Segment>)> dest)
{
    std::vector<filesystem::path> seg_relpaths;
    step::SegmentQuery squery(dataset().path,
                              dataset().iseg_segment_session->format,
                              "\\.index$", matcher);
    dataset().step().list_segments(squery, [&](std::filesystem::path&& s) {
        seg_relpaths.emplace_back(std::move(s));
    });
    std::sort(seg_relpaths.begin(), seg_relpaths.end());
    for (const auto& relpath : seg_relpaths)
        if (!dest(dataset().segment_session->segment_from_relpath_and_format(
                relpath, dataset().iseg_segment_session->format)))
            return false;
    return true;
}

bool Reader::impl_query_data(const query::Data& q, metadata_dest_func dest)
{
    query::TrackProgress track(q.progress);
    dest = track.wrap(dest);

    if (!segmented::Reader::impl_query_data(q, dest))
        return false;

    bool res = list_segments(q.matcher, [&](std::shared_ptr<Segment> segment) {
        auto reader =
            segment->reader(dataset().read_lock_segment(segment->relpath()));
        return reader->query_data(q, dest);
    });
    return track.done(res);
}

void Reader::summary_from_indices(const Matcher& matcher, Summary& summary)
{
    list_segments(matcher, [&](std::shared_ptr<Segment> segment) {
        auto reader =
            segment->reader(dataset().read_lock_segment(segment->relpath()));
        reader->query_summary(matcher, summary);
        return true;
    });
}

void Reader::summary_for_month(int year, int month, Summary& out)
{
    if (scache.read(out, year, month))
        return;

    Matcher matcher = Matcher::for_month(year, month);

    Summary summary;
    summary_from_indices(matcher, summary);

    scache.write(summary, year, month);
    out.add(summary);
}

void Reader::summary_for_all(Summary& out)
{
    if (scache.read(out))
        return;

    // Find the datetime extremes in the database
    core::Interval interval;
    dataset().step().time_extremes(
        step::SegmentQuery(dataset().path,
                           dataset().iseg_segment_session->format),
        interval);

    // If there is data in the database, get all the involved
    // monthly summaries
    if (!interval.is_unbounded())
    {
        int year  = interval.begin.ye;
        int month = interval.begin.mo;
        while (year < interval.end.ye ||
               (year == interval.end.ye && month <= interval.end.mo))
        {
            summary_for_month(year, month, out);

            // Increment the month
            month = (month % 12) + 1;
            if (month == 1)
                ++year;
        }
    }

    scache.write(out);
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    // Query the archives first
    segmented::Reader::impl_query_summary(matcher, summary);

    // Check if the matcher discriminates on reference times
    core::Interval interval;
    if (!matcher.intersect_interval(interval))
        return; // If the matcher contains an impossible reftime, return right
                // away

    if (!interval.begin.is_set() && !interval.end.is_set())
    {
        // The matcher does not contain reftime, or does not restrict on a
        // datetime range, we can work with a global summary
        Summary s;
        summary_for_all(s);
        s.filter(matcher, summary);
        return;
    }

    // Amend open ends with the bounds from the database
    core::Interval db_interval;
    dataset().step().time_extremes(
        step::SegmentQuery(dataset().path,
                           dataset().iseg_segment_session->format),
        db_interval);
    // If the database is empty then the result is empty:
    // we are done
    if (!db_interval.begin.is_set())
        return;
    bool begin_from_db = false;
    if (!interval.begin.is_set() || interval.begin < db_interval.begin)
    {
        interval.begin = db_interval.begin;
        begin_from_db  = true;
    }
    bool end_from_db = false;
    if (!interval.end.is_set() || interval.end > db_interval.end)
    {
        interval.end = db_interval.end;
        end_from_db  = true;
    }

    // If the interval is under a week, query the DB directly
    long long int range = core::Time::duration(interval);
    if (range <= 7 * 24 * 3600)
    {
        summary_from_indices(matcher, summary);
        return;
    }

    if (begin_from_db)
    {
        // Round down to month begin, so we reuse the cached summary if
        // available
        interval.begin = interval.begin.start_of_month();
    }
    if (end_from_db && !interval.end.is_start_of_month())
    {
        // Round up to month end, so we reuse the cached summary if
        // available
        interval.end = interval.end.start_of_next_month();
    }

    // If the selected interval does not envelope any whole month, query
    // the DB directly
    if (!interval.spans_one_whole_month())
    {
        summary_from_indices(matcher, summary);
        return;
    }

    // Query partial month at beginning, middle whole months, partial
    // month at end. Query whole months at extremes if they are indeed whole
    interval.iter_months([&](const core::Interval& month) {
        if (month.begin.is_start_of_month() && month.end.is_start_of_month())
        {
            Summary s;
            summary_for_month(month.begin.ye, month.begin.mo, s);
            s.filter(matcher, summary);
        }
        else
        {
            Summary s;
            summary_from_indices(Matcher::for_interval(month), s);
            s.filter(matcher, summary);
        }
        return true;
    });
}

core::Interval Reader::get_stored_time_interval()
{
    throw std::runtime_error(
        "iseg::Reader::get_stored_time_interval not yet implemented");
}

} // namespace iseg
} // namespace dataset
} // namespace arki
