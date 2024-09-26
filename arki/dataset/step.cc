#include "step.h"
#include "arki/core/time.h"
#include "arki/matcher.h"
#include "arki/matcher/utils.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/regexp.h"
#include <algorithm>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace dataset {

namespace step {

SegmentQuery::SegmentQuery()
{
}

SegmentQuery::SegmentQuery(const std::filesystem::path& root, const std::string& format)
    : root(root), format(format)
{
}

SegmentQuery::SegmentQuery(const std::filesystem::path& root, const std::string& format, const Matcher& matcher)
    : root(root), format(format), matcher(matcher)
{
}

SegmentQuery::SegmentQuery(const std::filesystem::path& root, const std::string& format, const std::string& extension_re, const Matcher& matcher)
    : root(root), format(format), extension_re(extension_re), matcher(matcher)
{
}


Files::Files(const Dirs& dirs, const std::filesystem::path& relpath, int value)
    : dirs(dirs), relpath(relpath), value(value) {}


struct BaseFiles : public Files
{
    using Files::Files;

    virtual std::unique_ptr<utils::Regexp> make_regexp() const = 0;
    virtual core::Interval to_period(const utils::Regexp& re) const = 0;
    virtual std::filesystem::path to_relpath(const Regexp& re) const = 0;
    virtual std::string to_format(const utils::Regexp& re) const = 0;

    void list(std::function<void(std::filesystem::path&& relpath)> dest) const override
    {
        auto re = make_regexp();
        sys::Path dir(dirs.query.root / this->relpath);
        for (sys::Path::iterator f = dir.begin(); f != dir.end(); ++f)
        {
            if (f->d_name[0] == '.') continue;
            if (!re->match(f->d_name)) continue;
            if (dirs.query.format != to_format(*re)) continue;

            // Its period must match the matcher in q
            if (!dirs.query.matcher.empty())
            {
                auto rt = to_period(*re);
                if (!dirs.query.matcher(rt)) continue;
            }

            dest(relpath / to_relpath(*re));
        }
    }

    core::Interval first() const override
    {
        std::string res_name;
        core::Interval res;

        auto re = make_regexp();
        sys::Path dir(dirs.query.root / this->relpath);
        for (sys::Path::iterator f = dir.begin(); f != dir.end(); ++f)
        {
            if (f->d_name[0] == '.') continue;
            if (!re->match(f->d_name)) continue;
            if (dirs.query.format != to_format(*re)) continue;

            if (res_name.empty() || f->d_name < res_name)
            {
                auto rt = to_period(*re);
                if (!dirs.query.matcher(rt)) continue;

                res_name = f->d_name;
                res = rt;
            }
        }
        return res;
    }

    core::Interval last() const override
    {
        std::string res_name;
        core::Interval res;

        auto re = make_regexp();
        sys::Path dir(dirs.query.root / this->relpath);
        for (sys::Path::iterator f = dir.begin(); f != dir.end(); ++f)
        {
            if (f->d_name[0] == '.') continue;
            if (!re->match(f->d_name)) continue;
            if (dirs.query.format != to_format(*re)) continue;
            if (res_name.empty() || res_name < f->d_name)
            {
                auto rt = to_period(*re);
                if (!dirs.query.matcher(rt)) continue;

                res_name = f->d_name;
                res = rt;
            }
        }
        return res;
    }
};


struct SingleFiles : public Files
{
    using Files::Files;

    void list(std::function<void(std::filesystem::path&& relpath)> dest) const override
    {
        string relpath = "all";
        relpath += ".";
        relpath += dirs.query.format;
        if (!std::filesystem::exists(dirs.query.root / relpath)) return;
        dest(move(relpath));
    }

    core::Interval first() const override
    {
        return core::Interval(
            core::Time::create_lowerbound(1000),
            core::Time::create_lowerbound(100000));
    }

    core::Interval last() const override
    {
        return core::Interval(
            core::Time::create_lowerbound(1000),
            core::Time::create_lowerbound(100000));
    }
};

struct YearFiles : public BaseFiles
{
    using BaseFiles::BaseFiles;

    std::unique_ptr<Regexp> make_regexp() const override
    {
        string re = "^(([[:digit:]]{4})\\.([^.]+))" + dirs.query.extension_re;
        return unique_ptr<Regexp>(new ERegexp(re, 4));
    }

    core::Interval to_period(const Regexp& re) const override
    {
        unsigned year = std::stoul(re[2]);
        return core::Interval(
                core::Time::create_lowerbound(year),
                core::Time::create_lowerbound(year + 1));
    }

    std::string to_format(const Regexp& re) const override { return re[3]; }
    std::filesystem::path to_relpath(const Regexp& re) const override { return re[1]; }
};

struct MonthFiles : public BaseFiles
{
    using BaseFiles::BaseFiles;

    std::unique_ptr<Regexp> make_regexp() const override
    {
        string re = "^(([[:digit:]]{2})\\.([^.]+))" + dirs.query.extension_re;
        return unique_ptr<Regexp>(new ERegexp(re, 4));
    }

    core::Interval to_period(const Regexp& re) const override
    {
        unsigned month = std::stoul(re[2]);
        core::Time begin = core::Time::create_lowerbound(value, month);
        return core::Interval(begin, begin.start_of_next_month());
    }

    std::string to_format(const Regexp& re) const override { return re[3]; }
    std::filesystem::path to_relpath(const Regexp& re) const override { return re[1]; }
};

struct MonthDayFiles : public BaseFiles
{
    using BaseFiles::BaseFiles;

    std::unique_ptr<Regexp> make_regexp() const override
    {
        string re = "^(([[:digit:]]{2})-([[:digit:]]{2})\\.([^.]+))" + dirs.query.extension_re;
        return unique_ptr<Regexp>(new ERegexp(re, 5));
    }

    core::Interval to_period(const Regexp& re) const override
    {
        unsigned month = std::stoul(re[2]);
        unsigned day = std::stoul(re[3]);
        core::Time begin = core::Time::create_lowerbound(value, month, day);
        core::Time end = begin;
        end.da += 1;
        end.normalise();
        return core::Interval(begin, end);
    }

    std::string to_format(const Regexp& re) const override { return re[4]; }
    std::filesystem::path to_relpath(const Regexp& re) const override { return re[1]; }
};




Dirs::Dirs(const SegmentQuery& query)
    : query(query)
{
}

struct BaseDirs : public Dirs
{
    virtual bool parse(const char* name, int& value) const = 0;
    virtual core::Interval to_period(int value) const = 0;
    virtual std::unique_ptr<Files> make_files(const std::filesystem::path& name, int value) const = 0;

    using Dirs::Dirs;

    void list(std::function<void(std::unique_ptr<Files>)> dest) const override
    {
        sys::Path dir(query.root);
        for (sys::Path::iterator f = dir.begin(); f != dir.end(); ++f)
        {
            if (f->d_name[0] == '.') continue;
            if (!f.isdir()) continue;
            int value;
            if (!parse(f->d_name, value)) continue;

            // Its period must match the matcher in q
            if (!query.matcher.empty())
            {
                auto rt = to_period(value);
                if (!query.matcher(rt)) continue;
            }

            dest(make_files(f->d_name, value));
        }
    }

    void extremes(core::Interval& first, core::Interval& last) const override
    {
        sys::Path dir(query.root);

        std::vector<std::pair<int, std::string>> subdirs;
        for (sys::Path::iterator f = dir.begin(); f != dir.end(); ++f)
        {
            if (f->d_name[0] == '.') continue;
            if (!f.isdir()) continue;
            int value;
            if (!parse(f->d_name, value)) continue;
            subdirs.push_back(make_pair(value, f->d_name));
        }

        if (subdirs.empty())
        {
            first = core::Interval();
            last = core::Interval();
            return;
        }

        std::sort(subdirs.begin(), subdirs.end());

        for (const auto& sd: subdirs)
        {
            auto files = make_files(sd.second, sd.first);
            first = files->first();
            if (!first.is_unbounded()) break;
        }

        for (auto i = subdirs.crbegin(); i != subdirs.crend(); ++i)
        {
            auto files = make_files(i->second, i->first);
            last = files->last();
            if (!last.is_unbounded()) break;
        }
    }
};

struct SingleDirs : public Dirs
{
    using Dirs::Dirs;

    void list(std::function<void(std::unique_ptr<Files>)> dest) const override
    {
        if (!std::filesystem::exists(query.root / ("all."s + query.format))) return;
        dest(std::unique_ptr<Files>(new SingleFiles(*this, "", 0)));
    }

    void extremes(core::Interval& first, core::Interval& last) const override
    {
        if (!std::filesystem::exists(query.root / ("all." + query.format)))
        {
            first = core::Interval();
            last = core::Interval();
            return;
        }

        first = core::Interval(
            core::Time::create_lowerbound(1000),
            core::Time::create_lowerbound(100000));
        last = core::Interval(
            core::Time::create_lowerbound(1000),
            core::Time::create_lowerbound(100000));
    }
};

template<typename FILES>
struct CenturyDirs : public BaseDirs
{
    using BaseDirs::BaseDirs;

    bool parse(const char* name, int& value) const override
    {
        char* endptr;
        value = strtoul(name, &endptr, 10);
        if (endptr - name != 2) return false;
        if (*endptr) return false;
        return true;
    }

    core::Interval to_period(int value) const override
    {
        return core::Interval(
            core::Time::create_lowerbound(value * 100),
            core::Time::create_lowerbound((value + 1) * 100));
    }

    std::unique_ptr<Files> make_files(const std::filesystem::path& name, int value) const override
    {
        return std::unique_ptr<Files>(new FILES(*this, name, value));
    }
};

template<typename FILES>
struct YearDirs : public BaseDirs
{
    using BaseDirs::BaseDirs;

    bool parse(const char* name, int& value) const override
    {
        char* endptr;
        value = strtoul(name, &endptr, 10);
        if (endptr - name != 4) return false;
        if (*endptr) return false;
        return true;
    }

    core::Interval to_period(int value) const override
    {
        return core::Interval(
            core::Time::create_lowerbound(value),
            core::Time::create_lowerbound(value + 1));
    }

    std::unique_ptr<Files> make_files(const std::filesystem::path& name, int value) const override
    {
        return std::unique_ptr<Files>(new FILES(*this, name, value));
    }
};

}


struct StepQuery
{
    const std::filesystem::path& root;
    const std::string& format;

    StepQuery(const std::filesystem::path& root, const std::string& format)
        : root(root), format(format) {}
    virtual ~StepQuery() {}

    virtual void list_segments(const Matcher& m, std::function<void(std::filesystem::path&&)> dest) const = 0;
    virtual void time_extremes(core::Interval& interval) const = 0;
};


struct PathQuery
{
    const std::filesystem::path& root;
    const std::string& format;
    const matcher::OR* m = nullptr;

    PathQuery(const std::filesystem::path& root, const std::string& format, const Matcher& m)
        : root(root), format(format)
    {
        if (!m.empty())
        {
            auto rt_matcher = m->get(TYPE_REFTIME);
            if (rt_matcher)
                this->m = rt_matcher.get();
        }
    }
};

struct StepParser
{
    // Depth at which segments are found
    const unsigned max_depth;
    unsigned depth = 0;

    StepParser(unsigned max_depth) : max_depth(max_depth) {}
    virtual ~StepParser() {}

    // Timespan of what has been parsed so far
    virtual void timespan(Time& start_time, Time& end_time) = 0;

    virtual bool parse_component(const PathQuery& q, unsigned depth, const char* name) = 0;
};


struct BaseStep : public Step
{
    std::unique_ptr<step::Dirs> explore(const step::SegmentQuery& query) const override
    {
        throw std::runtime_error("Step::explore not available for " + query.root.native());
    }

    void time_extremes(const step::SegmentQuery& query, core::Interval& interval) const override
    {
        auto dirs(explore(query));
        core::Interval first;
        core::Interval last;
        dirs->extremes(first, last);

        if (first.is_unbounded())
        {
            interval.begin = Time();
            interval.end = Time();
        } else {
            interval.begin = first.begin;
            interval.end = last.end;
        }
    }

    bool pathMatches(const std::filesystem::path& path, const matcher::OR& m) const override
    {
        core::Interval interval;
        if (!path_timespan(path, interval))
            return false;
        return m.match_interval(interval);
    }

    void list_segments(const step::SegmentQuery& query, std::function<void(std::filesystem::path&&)> dest) const override
    {
        auto dirs(explore(query));
        dirs->list([&](std::unique_ptr<step::Files> files) {
            files->list(dest);
        });
    }
};

struct SubStep : public BaseStep
{
    int year;
    SubStep(int year) : year(year) {}
};

struct Single : public BaseStep
{
    static const char* name() { return "single"; }

    std::unique_ptr<step::Dirs> explore(const step::SegmentQuery& query) const override
    {
        return std::unique_ptr<step::Dirs>(new step::SingleDirs(query));
    }

    bool path_timespan(const std::filesystem::path& path, core::Interval& interval) const override
    {
        interval.begin.set_lowerbound(1000);
        interval.end.set_upperbound(100000);
        return true;
    }

    std::filesystem::path operator()(const core::Time& time) const override
    {
        return "all";
    }
};


struct Yearly : public BaseStep
{
    static const char* name() { return "yearly"; }

    std::unique_ptr<step::Dirs> explore(const step::SegmentQuery& query) const override
    {
        return std::unique_ptr<step::Dirs>(new step::CenturyDirs<step::YearFiles>(query));
    }

    bool path_timespan(const std::filesystem::path& path, core::Interval& interval) const override
    {
        int dummy;
        int ye;
        if (sscanf(path.c_str(), "%02d/%04d", &dummy, &ye) != 2)
            return false;

        interval.begin.set_lowerbound(ye);
        interval.end.set_lowerbound(ye + 1);
        return true;
    }

    std::filesystem::path operator()(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[22];
        snprintf(buf, 22, "%02d/%04d", tt.ye/100, tt.ye);
        return buf;
    }
};

struct Monthly : public BaseStep
{
    static const char* name() { return "monthly"; }

    std::unique_ptr<step::Dirs> explore(const step::SegmentQuery& query) const override
    {
        return std::unique_ptr<step::Dirs>(new step::YearDirs<step::MonthFiles>(query));
    }

    bool path_timespan(const std::filesystem::path& path, core::Interval& interval) const override
    {
        int ye, mo;
        if (sscanf(path.c_str(), "%04d/%02d", &ye, &mo) != 2)
            return false;

        interval.begin.set_lowerbound(ye, mo);
        interval.end = interval.begin;
        interval.end.mo += 1;
        interval.end.normalise();
        return true;
    }

    std::filesystem::path operator()(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[10];
        snprintf(buf, 10, "%04d/%02d", tt.ye, tt.mo);
        return buf;
    }
};

struct SubMonthly : public SubStep
{
    using SubStep::SubStep;

    static const char* name() { return "monthly"; }

    bool path_timespan(const std::filesystem::path& path, core::Interval& interval) const override
    {
        int mo;
        if (sscanf(path.c_str(), "%02d", &mo) != 1)
            return false;

        interval.begin.set_lowerbound(year, mo);
        interval.end = interval.begin;
        interval.end.mo += 1;
        interval.end.normalise();
        return true;
    }

    std::filesystem::path operator()(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[10];
        snprintf(buf, 10, "%02d", tt.mo);
        return buf;
    }
};

struct Biweekly : public BaseStep
{
    static const char* name() { return "biweekly"; }

    bool path_timespan(const std::filesystem::path& path, core::Interval& interval) const override
    {
        int ye, mo = -1, biweek = -1;
        if (sscanf(path.c_str(), "%04d/%02d-%d", &ye, &mo, &biweek) != 2)
            return false;

        switch (biweek)
        {
            case 1:
                interval.begin.set_lowerbound(ye, mo, 1);
                interval.end.set_lowerbound(ye, mo, 15);
                break;
            case 2:
                interval.begin.set_lowerbound(ye, mo, 15);
                interval.end.set_lowerbound(ye, mo + 1, 1);
                interval.end.normalise();
                break;
            default:
                break;
        }
        return true;
    }

    std::filesystem::path operator()(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[10];
        snprintf(buf, 10, "%04d/%02d-", tt.ye, tt.mo);
        stringstream res;
        res << buf;
        res << (tt.da > 15 ? 2 : 1);
        return res.str();
    }
};

struct Weekly : public BaseStep
{
    static const char* name() { return "weekly"; }

    bool path_timespan(const std::filesystem::path& path, core::Interval& interval) const override
    {
        int ye, mo = -1, week = -1;
        if (sscanf(path.c_str(), "%04d/%02d-%d", &ye, &mo, &week) != 2)
            return false;
        int min_da = -1;
        int max_da = -1;
        if (week != -1)
        {
            min_da = (week - 1) * 7 + 1;
            max_da = min_da + 7;
        }
        // TODO: are these normalised?
        interval.begin.set_lowerbound(ye, mo, min_da);
        interval.end.set_lowerbound(ye, mo, max_da);
        return true;
    }

    std::filesystem::path operator()(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[10];
        snprintf(buf, 10, "%04d/%02d-", tt.ye, tt.mo);
        stringstream res;
        res << buf;
        res << (((tt.da - 1) / 7) + 1);
        return res.str();
    }
};

struct SubWeekly : public SubStep
{
    using SubStep::SubStep;

    static const char* name() { return "weekly"; }

    bool path_timespan(const std::filesystem::path& path, core::Interval& interval) const override
    {
        int mo = -1, week = -1;
        if (sscanf(path.c_str(), "%02d-%d", &mo, &week) != 2)
            return false;
        int min_da = -1;
        int max_da = -1;
        if (week != -1)
        {
            min_da = (week - 1) * 7 + 1;
            max_da = min_da + 7;
        }
        interval.begin.set_lowerbound(year, mo, min_da);
        interval.end.set_lowerbound(year, mo, max_da);
        return true;
    }

    std::filesystem::path operator()(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[10];
        snprintf(buf, 10, "%02d-", tt.mo);
        stringstream res;
        res << buf;
        res << (((tt.da - 1) / 7) + 1);
        return res.str();
    }
};

struct Daily : public BaseStep
{
    static const char* name() { return "daily"; }

    std::unique_ptr<step::Dirs> explore(const step::SegmentQuery& query) const override
    {
        return std::unique_ptr<step::Dirs>(new step::YearDirs<step::MonthDayFiles>(query));
    }

    bool path_timespan(const std::filesystem::path& path, core::Interval& interval) const override
    {
        int ye, mo, da;
        if (sscanf(path.c_str(), "%04d/%02d-%02d", &ye, &mo, &da) != 3)
            return false;
        interval.begin.set_lowerbound(ye, mo, da);
        interval.end = interval.begin;
        interval.end.da += 1;
        interval.end.normalise();
        return true;
    }

    std::filesystem::path operator()(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[15];
        snprintf(buf, 15, "%04d/%02d-%02d", tt.ye, tt.mo, tt.da);
        return buf;
    }
};

struct SubDaily : public SubStep
{
    using SubStep::SubStep;

    static const char* name() { return "daily"; }

    bool path_timespan(const std::filesystem::path& path, core::Interval& interval) const override
    {
        int mo, da;
        if (sscanf(path.c_str(), "%02d/%02d", &mo, &da) != 2)
            return false;
        interval.begin.set_lowerbound(year, mo, da);
        interval.end = interval.begin;
        interval.end.da += 1;
        interval.end.normalise();
        return true;
    }

    std::filesystem::path operator()(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[15];
        snprintf(buf, 15, "%02d/%02d", tt.mo, tt.da);
        return buf;
    }
};

std::shared_ptr<Step> Step::create(const std::string& type)
{
    if (type == Daily::name())
        return shared_ptr<Step>(new Daily);
    else if (type == Weekly::name())
        return shared_ptr<Step>(new Weekly);
    else if (type == Biweekly::name())
        return shared_ptr<Step>(new Biweekly);
    else if (type == Monthly::name())
        return shared_ptr<Step>(new Monthly);
    else if (type == Yearly::name())
        return shared_ptr<Step>(new Yearly);
    else if (type == Single::name())
        return shared_ptr<Step>(new Single);
    else
        throw std::runtime_error("step '" + type + "' is not supported.  Valid values are daily, weekly, biweekly, monthly, and yearly.");
}

std::vector<std::filesystem::path> Step::list()
{
    std::vector<std::filesystem::path> res {
        Daily::name(),
        Weekly::name(),
        Biweekly::name(),
        Monthly::name(),
        Yearly::name(),
    };
    return res;
}

}
}
