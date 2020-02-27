#include "testlarge.h"
#include "empty.h"
#include "query.h"
#include "arki/core/time.h"
#include "arki/types/reftime.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/summary.h"
#include "config.h"

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


bool Reader::generate(const core::Time& begin, const core::Time& until, std::function<bool(std::unique_ptr<Metadata>)> out) const
{
    core::Time cur = begin;
    cur.ho = 0;
    cur.mi = 0;
    cur.se = 0;
    while (cur <= until)
    {
        unique_ptr<Metadata> md(new Metadata);
        md->set(types::Reftime::createPosition(cur));
        // TODO: set other metadata

        std::vector<uint8_t> data(1024 * 1024, 0);
        md->set_source_inline("grib", metadata::DataManager::get().to_data("grib", move(data)));

        if (!out(move(md)))
            return false;

        cur.ho += 6;
        cur.normalise();
    }

    return true;
}

bool Reader::query_data(const dataset::DataQuery& q, metadata_dest_func out)
{
    std::unique_ptr<core::Time> begin;
    std::unique_ptr<core::Time> until;
    if (!q.matcher.restrict_date_range(begin, until))
        return true;
    if (!begin || *begin < core::Time(2000, 1, 1))
        begin.reset(new core::Time(2000, 1, 1));
    if (!until || *until > core::Time(2017, 1, 1))
        until.reset(new core::Time(2017, 1, 1));
    // TODO: implement support for q.sort
    return generate(*begin, *until, [&](std::unique_ptr<Metadata> md) {
        if (!q.matcher(*md)) return true;
        return out(move(md));
    });
}

void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    std::unique_ptr<core::Time> begin;
    std::unique_ptr<core::Time> until;
    if (!matcher.restrict_date_range(begin, until))
        return;
    if (!begin || *begin < core::Time(2000, 1, 1))
        begin.reset(new core::Time(2000, 1, 1));
    if (!until || *until > core::Time(2017, 1, 1))
        until.reset(new core::Time(2017, 1, 1));
    generate(*begin, *until, [&](std::unique_ptr<Metadata> md) {
        if (!matcher(*md)) return true;
        summary.add(*md);
        return true;
    });
}

}
}
}
