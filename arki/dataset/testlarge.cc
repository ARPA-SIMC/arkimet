#include "testlarge.h"
#include "empty.h"
#include "arki/query.h"
#include "arki/query/progress.h"
#include "arki/core/time.h"
#include "arki/types/reftime.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/summary.h"

using namespace std;

namespace arki {
namespace dataset {
namespace testlarge {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::Dataset(session, cfg)
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader() { return std::make_shared<Reader>(shared_from_this()); }
std::shared_ptr<dataset::Writer> Dataset::create_writer() { return std::make_shared<empty::Writer>(shared_from_this()); }
std::shared_ptr<dataset::Checker> Dataset::create_checker() { return std::make_shared<empty::Checker>(shared_from_this()); }


bool Reader::generate(const core::Interval& interval, std::function<bool(std::unique_ptr<Metadata>)> out) const
{
    core::Time cur = interval.begin;
    cur.ho = 0;
    cur.mi = 0;
    cur.se = 0;
    while (cur <= interval.end)
    {
        unique_ptr<Metadata> md(new Metadata);
        md->set(types::Reftime::createPosition(cur));
        // TODO: set other metadata

        std::vector<uint8_t> data(1024 * 1024, 0);
        md->set_source_inline(DataFormat::GRIB, metadata::DataManager::get().to_data(DataFormat::GRIB, move(data)));

        if (!out(move(md)))
            return false;

        cur.ho += 6;
        cur.normalise();
    }

    return true;
}

bool Reader::impl_query_data(const query::Data& q, metadata_dest_func dest)
{
    query::TrackProgress track(q.progress);
    dest = track.wrap(dest);

    core::Interval interval;
    if (!q.matcher.intersect_interval(interval))
        return true;
    interval.intersect(core::Interval(core::Time(2000, 1, 1), core::Time(2017, 1, 1)));
    // TODO: implement support for q.sort
    return track.done(generate(interval, [&](std::unique_ptr<Metadata> md) {
        if (!q.matcher(*md)) return true;
        return dest(move(md));
    }));
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    core::Interval interval;
    if (!matcher.intersect_interval(interval))
        return;
    interval.intersect(core::Interval(core::Time(2000, 1, 1), core::Time(2017, 1, 1)));
    generate(interval, [&](std::unique_ptr<Metadata> md) {
        if (!matcher(*md)) return true;
        summary.add(*md);
        return true;
    });
}

core::Interval Reader::get_stored_time_interval()
{
    throw std::runtime_error("testlarge::Reader::get_stored_time_interval not yet implemented");
}

}
}
}
