#include "reader.h"
#include "index.h"
#include "arki/dataset/step.h"
#include "arki/dataset/progress.h"
#include "arki/dataset/query.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/summary.h"
#include <algorithm>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace iseg {

Reader::Reader(std::shared_ptr<iseg::Dataset> dataset)
    : DatasetAccess(dataset), scache(dataset->summary_cache_pathname)
{
    // Create the directory if it does not exist
    sys::makedirs(dataset->path);
    scache.openRW();
}

Reader::~Reader()
{
}

std::string Reader::type() const { return "iseg"; }

bool Reader::is_dataset(const std::string& dir)
{
    return true;
}

bool Reader::list_segments(const Matcher& matcher, std::function<bool(const std::string& relpath)> dest)
{
    vector<string> seg_relpaths;
    step::SegmentQuery squery(dataset().path, dataset().format, "\\.index$", matcher);
    dataset().step().list_segments(squery, [&](std::string&& s) {
        seg_relpaths.emplace_back(move(s));
    });
    std::sort(seg_relpaths.begin(), seg_relpaths.end());
    for (const auto& relpath: seg_relpaths)
        if (!dest(relpath))
            return false;
    return true;
}

bool Reader::impl_query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    dataset::TrackProgress track(q.progress);
    dest = track.wrap(dest);

    if (!segmented::Reader::impl_query_data(q, dest))
        return false;

    bool res = list_segments(q.matcher, [&](const std::string& relpath) {
        RIndex idx(m_dataset, relpath, dataset().read_lock_segment(relpath));
        return idx.query_data(q, *dataset().session, dest);
    });
    return track.done(res);
}

void Reader::summary_from_indices(const Matcher& matcher, Summary& summary)
{
    list_segments(matcher, [&](const std::string& relpath) {
        RIndex idx(m_dataset, relpath, dataset().read_lock_segment(relpath));
        idx.query_summary_from_db(matcher, summary);
        return true;
    });
}

void Reader::summary_for_month(int year, int month, Summary& out)
{
    if (scache.read(out, year, month)) return;

    Matcher matcher = Matcher::match_month(year, month);

    Summary summary;
    summary_from_indices(matcher, summary);

    scache.write(summary, year, month);
    out.add(summary);
}

void Reader::summary_for_all(Summary& out)
{
    if (scache.read(out)) return;

    // Find the datetime extremes in the database
    unique_ptr<core::Time> begin;
    unique_ptr<core::Time> end;
    dataset().step().time_extremes(step::SegmentQuery(dataset().path, dataset().format), begin, end);

    // If there is data in the database, get all the involved
    // monthly summaries
    if (begin.get() && end.get())
    {
        int year = begin->ye;
        int month = begin->mo;
        while (year < end->ye || (year == end->ye && month <= end->mo))
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

namespace {
inline bool range_envelopes_full_month(const core::Time& begin, const core::Time& end)
{
    bool begins_at_beginning = begin.da == 1 && begin.ho == 0 && begin.mi == 0 && begin.se == 0;
    if (begins_at_beginning)
        return end >= begin.end_of_month();

    bool ends_at_end = end.da == core::Time::days_in_month(end.ye, end.mo) && end.ho == 23 && end.mi == 59 && end.se == 59;
    if (ends_at_end)
        return begin <= end.start_of_month();

    return end.ye == begin.ye + begin.mo / 12 && end.mo == (begin.mo % 12) + 1;
}
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    // Query the archives first
    segmented::Reader::impl_query_summary(matcher, summary);

    // Check if the matcher discriminates on reference times
    unique_ptr<core::Time> begin;
    unique_ptr<core::Time> end;
    if (!matcher.restrict_date_range(begin, end))
        return; // If the matcher contains an impossible reftime, return right away

    if (!begin.get() && !end.get())
    {
        // The matcher does not contain reftime, or does not restrict on a
        // datetime range, we can work with a global summary
        Summary s;
        summary_for_all(s);
        s.filter(matcher, summary);
        return;
    }

    // Amend open ends with the bounds from the database
    unique_ptr<core::Time> db_begin;
    unique_ptr<core::Time> db_end;
    dataset().step().time_extremes(step::SegmentQuery(dataset().path, dataset().format), db_begin, db_end);
    // If the database is empty then the result is empty:
    // we are done
    if (!db_begin.get())
        return;
    bool begin_from_db = false;
    if (!begin.get())
    {
        begin.reset(new core::Time(*db_begin));
        begin_from_db = true;
    } else if (*begin < *db_begin) {
        *begin = *db_begin;
        begin_from_db = true;
    }
    bool end_from_db = false;
    if (!end.get())
    {
        end.reset(new core::Time(*db_end));
        end_from_db = true;
    } else if (*end > *db_end) {
        *end = *db_end;
        end_from_db = true;
    }

    // If the interval is under a week, query the DB directly
    long long int range = core::Time::duration(*begin, *end);
    if (range <= 7 * 24 * 3600)
    {
        summary_from_indices(matcher, summary);
        return;
    }

    if (begin_from_db)
    {
        // Round down to month begin, so we reuse the cached summary if
        // available
        *begin = begin->start_of_month();
    }
    if (end_from_db)
    {
        // Round up to month end, so we reuse the cached summary if
        // available
        *end = end->end_of_month();
    }

    // If the selected interval does not envelope any whole month, query
    // the DB directly
    if (!range_envelopes_full_month(*begin, *end))
    {
        summary_from_indices(matcher, summary);
        return;
    }

    // Query partial month at beginning, middle whole months, partial
    // month at end. Query whole months at extremes if they are indeed whole
    while (*begin <= *end)
    {
        core::Time endmonth = begin->end_of_month();

        bool starts_at_beginning = (begin->da == 1 && begin->ho == 0 && begin->mi == 0 && begin->se == 0);
        if (starts_at_beginning && endmonth <= *end)
        {
            Summary s;
            summary_for_month(begin->ye, begin->mo, s);
            s.filter(matcher, summary);
        } else if (endmonth <= *end) {
            Summary s;
            summary_from_indices(Matcher::parse("reftime:>=" + begin->to_sql() + ",<=" + endmonth.to_sql()), s);
            s.filter(matcher, summary);
        } else {
            Summary s;
            summary_from_indices(Matcher::parse("reftime:>=" + begin->to_sql() + ",<" + end->to_sql()), s);
            s.filter(matcher, summary);
        }

        // Advance to the beginning of the next month
        *begin = begin->start_of_next_month();
    }
}

void Reader::expand_date_range(unique_ptr<core::Time>& begin, unique_ptr<core::Time>& end)
{
    //m_mft->expand_date_range(begin, end);
}

}
}
}
