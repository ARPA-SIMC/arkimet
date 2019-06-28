#include "testlarge.h"
#include "empty.h"
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

Config::Config(const core::cfg::Section& cfg)
    : dataset::Config(cfg)
{
}

std::shared_ptr<const Config> Config::create(const core::cfg::Section& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}

std::unique_ptr<dataset::Reader> Config::create_reader() const { return std::unique_ptr<dataset::Reader>(new Reader(shared_from_this())); }
std::unique_ptr<dataset::Writer> Config::create_writer() const { return std::unique_ptr<dataset::Writer>(new empty::Writer(shared_from_this())); }
std::unique_ptr<dataset::Checker> Config::create_checker() const { return std::unique_ptr<dataset::Checker>(new empty::Checker(shared_from_this())); }


Reader::Reader(std::shared_ptr<const dataset::Config> config) : m_config(config) {}
Reader::~Reader() {}

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

bool Reader::query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> out)
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
