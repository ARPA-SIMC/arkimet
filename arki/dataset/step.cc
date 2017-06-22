#include "step.h"
#include "arki/matcher.h"
#include "arki/types/reftime.h"
#include "arki/utils/pcounter.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/regexp.h"
#include <algorithm>
#include <cstdint>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace dataset {

namespace step {

Dirs::Dirs(const std::string& root, const std::string& format)
    : root(root), format(format) {}

struct BaseDirs : public Dirs
{
    virtual bool parse(const char* name, int& value) const = 0;
    virtual std::unique_ptr<types::reftime::Period> to_period(int value) const = 0;
    virtual std::unique_ptr<Files> make_files(const std::string& name, int value) const = 0;

    using Dirs::Dirs;

    void list(const Matcher& m, std::function<void(std::unique_ptr<Files>)> dest) const override
    {
        sys::Path dir(root);
        for (sys::Path::iterator f = dir.begin(); f != dir.end(); ++f)
        {
            if (f->d_name[0] == '.') continue;
            if (!f.isdir()) continue;
            int value;
            if (!parse(f->d_name, value)) continue;

            // Its period must match the matcher in q
            if (!m.empty())
            {
                auto rt = to_period(value);
                if (!m(*rt)) continue;
            }

            dest(move(make_files(f->d_name, value)));
        }
    }

    void extremes(std::unique_ptr<types::reftime::Period>& first, std::unique_ptr<types::reftime::Period>& last) const override
    {
        sys::Path dir(root);

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
            first.reset();
            last.reset();
            return;
        }

        std::sort(subdirs.begin(), subdirs.end());

        for (const auto& sd: subdirs)
        {
            auto files = make_files(sd.second, sd.first);
            first = files->first();
            if (first) break;
        }

        for (auto i = subdirs.crbegin(); i != subdirs.crend(); ++i)
        {
            auto files = make_files(i->second, i->first);
            last = files->last();
            if (first) break;
        }
    }
};


Files::Files(const Dirs& dirs, const std::string& relpath, int value)
    : dirs(dirs), relpath(relpath), value(value) {}


struct BaseFiles : public Files
{
    using Files::Files;

    virtual std::unique_ptr<utils::Regexp> make_regexp() const = 0;
    virtual std::unique_ptr<types::reftime::Period> to_period(const utils::Regexp& re) const = 0;

    void list(const Matcher& m, std::function<void(std::string&& relpath)> dest) const override
    {
        auto re = make_regexp();
        sys::Path dir(str::joinpath(dirs.root, this->relpath));
        for (sys::Path::iterator f = dir.begin(); f != dir.end(); ++f)
        {
            if (f->d_name[0] == '.') continue;
            if (!re->match(f->d_name)) continue;
            if (dirs.format != f->d_name + re->match_end(0)) continue;

            // Its period must match the matcher in q
            if (!m.empty())
            {
                auto rt = to_period(*re);
                if (!m(*rt)) continue;
            }

            dest(str::joinpath(relpath, f->d_name));
        }
    }

    std::unique_ptr<types::reftime::Period> first() const override
    {
        string res_name;
        std::unique_ptr<types::reftime::Period> res;

        auto re = make_regexp();
        sys::Path dir(str::joinpath(dirs.root, this->relpath));
        for (sys::Path::iterator f = dir.begin(); f != dir.end(); ++f)
        {
            if (f->d_name[0] == '.') continue;
            if (!re->match(f->d_name)) continue;
            if (dirs.format != f->d_name + re->match_end(0)) continue;
            if (res_name.empty() || res_name > f->d_name)
            {
                res_name = f->d_name;
                res = to_period(*re);
            }
        }
        return res;
    }

    std::unique_ptr<types::reftime::Period> last() const override
    {
        string res_name;
        std::unique_ptr<types::reftime::Period> res;

        auto re = make_regexp();
        sys::Path dir(str::joinpath(dirs.root, this->relpath));
        for (sys::Path::iterator f = dir.begin(); f != dir.end(); ++f)
        {
            if (f->d_name[0] == '.') continue;
            if (!re->match(f->d_name)) continue;
            if (dirs.format != f->d_name + re->match_end(0)) continue;
            if (res_name.empty() || res_name < f->d_name)
            {
                res_name = f->d_name;
                res = to_period(*re);
            }
        }
        return res;
    }
};


struct SingleFiles : public Files
{
    using Files::Files;

    void list(const Matcher& m, std::function<void(std::string&& relpath)> dest) const override
    {
        string relpath = "all";
        relpath += ".";
        relpath += dirs.format;
        if (!sys::exists(str::joinpath(dirs.root, relpath))) return;
        dest(move(relpath));
    }

    std::unique_ptr<types::reftime::Period> first() const override
    {
        return unique_ptr<types::reftime::Period>(new reftime::Period(
            core::Time::create_lowerbound(1000),
            core::Time::create_upperbound(99999)));
    }

    std::unique_ptr<types::reftime::Period> last() const override
    {
        return unique_ptr<types::reftime::Period>(new reftime::Period(
            core::Time::create_lowerbound(1000),
            core::Time::create_upperbound(99999)));
    }
};


struct SingleDirs : public Dirs
{
    using Dirs::Dirs;

    void list(const Matcher& m, std::function<void(std::unique_ptr<Files>)> dest) const override
    {
        if (!sys::exists(str::joinpath(root, "all") + "." + format)) return;
        dest(std::unique_ptr<Files>(new SingleFiles(*this, "", 0)));
    }

    void extremes(std::unique_ptr<types::reftime::Period>& first, std::unique_ptr<types::reftime::Period>& last) const override
    {
        if (!sys::exists(str::joinpath(root, "all") + "." + format))
        {
            first.reset();
            last.reset();
            return;
        }

        first.reset(new reftime::Period(
            core::Time::create_lowerbound(1000),
            core::Time::create_upperbound(99999)));
        last.reset(new reftime::Period(
            core::Time::create_lowerbound(1000),
            core::Time::create_upperbound(99999)));
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

    virtual std::unique_ptr<types::reftime::Period> to_period(int value) const
    {
        return unique_ptr<types::reftime::Period>(new reftime::Period(
            core::Time::create_lowerbound(value * 100),
            core::Time::create_upperbound(value * 100 + 99)));
    }

    virtual std::unique_ptr<Files> make_files(const std::string& name, int value) const
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

    virtual std::unique_ptr<types::reftime::Period> to_period(int value) const
    {
        return unique_ptr<types::reftime::Period>(new reftime::Period(
            core::Time::create_lowerbound(value),
            core::Time::create_upperbound(value)));
    }

    virtual std::unique_ptr<Files> make_files(const std::string& name, int value) const
    {
        return std::unique_ptr<Files>(new FILES(*this, name, value));
    }
};

struct YearFiles : public BaseFiles
{
    using BaseFiles::BaseFiles;

    std::unique_ptr<Regexp> make_regexp() const override
    {
        return unique_ptr<Regexp>(new ERegexp("^([[:digit:]]{4})\\.", 2));
    }

    std::unique_ptr<types::reftime::Period> to_period(const Regexp& re) const
    {
        unsigned year = std::stoul(re[1]);
        return unique_ptr<types::reftime::Period>(new reftime::Period(
                core::Time::create_lowerbound(year),
                core::Time::create_upperbound(year)));
    }
};

struct MonthFiles : public BaseFiles
{
    using BaseFiles::BaseFiles;

    std::unique_ptr<Regexp> make_regexp() const override
    {
        return unique_ptr<Regexp>(new ERegexp("^([[:digit:]]{2})\\.", 2));
    }

    std::unique_ptr<types::reftime::Period> to_period(const Regexp& re) const
    {
        unsigned month = std::stoul(re[1]);
        return unique_ptr<types::reftime::Period>(new reftime::Period(
                core::Time::create_lowerbound(value, month),
                core::Time::create_upperbound(value, month)));
    }
};

struct MonthDayFiles : public BaseFiles
{
    using BaseFiles::BaseFiles;

    std::unique_ptr<Regexp> make_regexp() const override
    {
        return unique_ptr<Regexp>(new ERegexp("^([[:digit:]]{2})-([[:digit:]]{2})\\.", 3));
    }

    std::unique_ptr<types::reftime::Period> to_period(const Regexp& re) const
    {
        unsigned month = std::stoul(re[1]);
        unsigned day = std::stoul(re[2]);
        return unique_ptr<types::reftime::Period>(new reftime::Period(
                core::Time::create_lowerbound(value, month, day),
                core::Time::create_upperbound(value, month, day)));
    }
};


}


struct StepQuery
{
    const std::string& root;
    const std::string& format;

    StepQuery(const std::string& root, const std::string& format)
        : root(root), format(format) {}

    virtual void list_segments(const Matcher& m, std::function<void(std::string&&)> dest) const = 0;
    virtual void time_extremes(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& until) const = 0;
};


struct PathQuery
{
    const std::string& root;
    const std::string& format;
    const matcher::OR* m = nullptr;

    PathQuery(const std::string& root, const std::string& format, const Matcher& m)
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

    // Timespan of what has been parsed so far
    virtual void timespan(Time& start_time, Time& end_time) = 0;

    virtual bool parse_component(const PathQuery& q, unsigned depth, const char* name) = 0;
};


struct BaseStep : public Step
{
    std::unique_ptr<step::Dirs> explore(const std::string& root, const std::string& format) const override
    {
        throw std::runtime_error("Step::explore not available for " + root);
    }

    void time_extremes(const std::string& root, const std::string& format, std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& until) const override
    {
        auto dirs(explore(root, format));
        std::unique_ptr<types::reftime::Period> first;
        std::unique_ptr<types::reftime::Period> last;
        dirs->extremes(first, last);

        if (!first)
        {
            begin.reset();
            until.reset();
        } else {
            begin.reset(new Time(first->begin));
            until.reset(new Time(last->end));
        }
    }

    bool pathMatches(const std::string& path, const matcher::OR& m) const override
    {
        Time min;
        Time max;
        if (!path_timespan(path, min, max))
            return false;
        auto rt = Reftime::createPeriod(min, max);
        return m.matchItem(*rt);
    }

    void list_segments(const std::string& root, const std::string& format, const Matcher& m, std::function<void(std::string&&)> dest) const override
    {
        auto dirs(explore(root, format));
        dirs->list(m, [&](std::unique_ptr<step::Files> files) {
            files->list(m, dest);
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

    std::unique_ptr<step::Dirs> explore(const std::string& root, const std::string& format) const override
    {
        return std::unique_ptr<step::Dirs>(new step::SingleDirs(root, format));
    }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        start_time.set_lowerbound(1000);
        end_time.set_upperbound(99999);
        return true;
    }

    std::string operator()(const core::Time& time) const override
    {
        return "all";
    }
};


struct Yearly : public BaseStep
{
    static const char* name() { return "yearly"; }

    std::unique_ptr<step::Dirs> explore(const std::string& root, const std::string& format) const override
    {
        return std::unique_ptr<step::Dirs>(new step::CenturyDirs<step::YearFiles>(root, format));
    }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int dummy;
        int ye;
        if (sscanf(path.c_str(), "%02d/%04d", &dummy, &ye) != 2)
            return false;

        start_time.set_lowerbound(ye);
        end_time.set_upperbound(ye);
        return true;
    }

    std::string operator()(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[9];
        snprintf(buf, 9, "%02d/%04d", tt.ye/100, tt.ye);
        return buf;
    }
};

struct Monthly : public BaseStep
{
    static const char* name() { return "monthly"; }

    std::unique_ptr<step::Dirs> explore(const std::string& root, const std::string& format) const override
    {
        return std::unique_ptr<step::Dirs>(new step::YearDirs<step::MonthFiles>(root, format));
    }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo;
        if (sscanf(path.c_str(), "%04d/%02d", &ye, &mo) != 2)
            return false;

        start_time.set_lowerbound(ye, mo);
        end_time.set_upperbound(ye, mo);
        return true;
    }

    std::string operator()(const core::Time& time) const override
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

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int mo;
        if (sscanf(path.c_str(), "%02d", &mo) != 1)
            return false;

        start_time.set_lowerbound(year, mo);
        end_time.set_upperbound(year, mo);
        return true;
    }

    std::string operator()(const core::Time& time) const override
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

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo = -1, biweek = -1;
        if (sscanf(path.c_str(), "%04d/%02d-%d", &ye, &mo, &biweek) != 2)
            return false;

        int min_da = -1;
        int max_da = -1;
        switch (biweek)
        {
            case 1: min_da = 1; max_da = 14; break;
            case 2: min_da = 15; max_da = -1; break;
            default: break;
        }
        start_time.set_lowerbound(ye, mo, min_da);
        end_time.set_upperbound(ye, mo, max_da);
        return true;
    }

    std::string operator()(const core::Time& time) const override
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

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo = -1, week = -1;
        if (sscanf(path.c_str(), "%04d/%02d-%d", &ye, &mo, &week) != 2)
            return false;
        int min_da = -1;
        int max_da = -1;
        if (week != -1)
        {
            min_da = (week - 1) * 7 + 1;
            max_da = min_da + 6;
        }
        start_time.set_lowerbound(ye, mo, min_da);
        end_time.set_upperbound(ye, mo, max_da);
        return true;
    }

    std::string operator()(const core::Time& time) const override
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

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int mo = -1, week = -1;
        if (sscanf(path.c_str(), "%02d-%d", &mo, &week) != 2)
            return false;
        int min_da = -1;
        int max_da = -1;
        if (week != -1)
        {
            min_da = (week - 1) * 7 + 1;
            max_da = min_da + 6;
        }
        start_time.set_lowerbound(year, mo, min_da);
        end_time.set_upperbound(year, mo, max_da);
        return true;
    }

    std::string operator()(const core::Time& time) const override
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

    std::unique_ptr<step::Dirs> explore(const std::string& root, const std::string& format) const override
    {
        return std::unique_ptr<step::Dirs>(new step::YearDirs<step::MonthDayFiles>(root, format));
    }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo, da;
        if (sscanf(path.c_str(), "%04d/%02d-%02d", &ye, &mo, &da) != 3)
            return false;
        start_time.set_lowerbound(ye, mo, da);
        end_time.set_upperbound(ye, mo, da);
        return true;
    }

    std::string operator()(const core::Time& time) const override
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

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int mo, da;
        if (sscanf(path.c_str(), "%02d/%02d", &mo, &da) != 2)
            return false;
        start_time.set_lowerbound(year, mo, da);
        end_time.set_upperbound(year, mo, da);
        return true;
    }

    std::string operator()(const core::Time& time) const override
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

std::vector<std::string> Step::list()
{
    vector<string> res {
        Daily::name(),
        Weekly::name(),
        Biweekly::name(),
        Monthly::name(),
        Yearly::name(),
    };
    return res;
}


struct BaseShardStep : public ShardStep
{
    std::string type;

    BaseShardStep(const std::string& type) : type(type) {}

    std::shared_ptr<Step> substep(const core::Time& time) const override
    {
        const Time& tt = time;
        auto year = tt.ye;

        if (type == SubDaily::name())
            return shared_ptr<Step>(new SubDaily(year));
        else if (type == SubWeekly::name())
            return shared_ptr<Step>(new SubWeekly(year));
        else if (type == SubMonthly::name())
            return shared_ptr<Step>(new SubMonthly(year));
        else if (type == Yearly::name())
            return shared_ptr<Step>(new Yearly());
        else if (type == Single::name())
            return shared_ptr<Step>(new Single());
        else
            throw std::runtime_error("step '" + type + "' is not supported.  Valid values are daily, weekly, and monthly.");
    }

    std::vector<std::pair<core::Time, core::Time>> list_shards(const std::string& pathname) const override
    {
        std::vector<std::pair<core::Time, core::Time>> res;
        sys::Path dir(pathname);
        for (sys::Path::iterator f = dir.begin(); f != dir.end(); ++f)
        {
            if (!f.isdir()) continue;
            auto span = shard_span(f->d_name);
            if (span.first.ye == 0) continue;
            res.emplace_back(span);
        }
        std::sort(res.begin(), res.end());
        return res;
    }
};

struct ShardSingle : public BaseShardStep
{
    using BaseShardStep::BaseShardStep;

    static const char* name() { return "single"; }

    std::string shard_path(const core::Time& time) const override
    {
        return "all";
    }

    std::pair<core::Time, core::Time> shard_span(const std::string& shard_path) const override
    {
        if (shard_path != "all")
            return make_pair(core::Time(0, 0, 0), core::Time(0, 0, 0));
        return make_pair(core::Time::create_lowerbound(1000), core::Time::create_upperbound(99999));
    }
};

struct ShardYearly : public BaseShardStep
{
    using BaseShardStep::BaseShardStep;

    static const char* name() { return "yearly"; }

    std::string shard_path(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[9];
        snprintf(buf, 9, "%04d", tt.ye);
        return buf;
    }

    std::pair<core::Time, core::Time> shard_span(const std::string& shard_path) const override
    {
        int year;
        if (sscanf(shard_path.c_str(), "%04d", &year) != 1)
            return make_pair(core::Time(0, 0, 0), core::Time(0, 0, 0));
        return make_pair(core::Time(year, 1, 1, 0, 0, 0), core::Time(year, 12, 31, 23, 59, 59));
    }
};

struct ShardMonthly : public BaseShardStep
{
    using BaseShardStep::BaseShardStep;

    static const char* name() { return "monthly"; }

    std::string shard_path(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[10];
        snprintf(buf, 10, "%04d-%02d", tt.ye, tt.mo);
        return buf;
    }

    std::pair<core::Time, core::Time> shard_span(const std::string& shard_path) const override
    {
        int ye, mo;
        if (sscanf(shard_path.c_str(), "%04d-%02d", &ye, &mo) != 2)
            return make_pair(core::Time(0, 0, 0), core::Time(0, 0, 0));
        core::Time begin(ye, mo, 1, 0, 0, 0);
        return make_pair(begin, begin.end_of_month());
    }
};

struct ShardWeekly : public BaseShardStep
{
    using BaseShardStep::BaseShardStep;

    static const char* name() { return "weekly"; }

    std::string shard_path(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[10];
        snprintf(buf, 10, "%04d-%02d-%1d", tt.ye, tt.mo, (((tt.da - 1) / 7) + 1));
        return buf;
    }

    std::pair<core::Time, core::Time> shard_span(const std::string& shard_path) const override
    {
        int ye, mo, we;
        if (sscanf(shard_path.c_str(), "%04d-%02d-%1d", &ye, &mo, &we) != 3)
            return make_pair(core::Time(0, 0, 0), core::Time(0, 0, 0));
        core::Time begin(ye, mo, (we - 1) * 7 + 1, 0, 0, 0);
        core::Time end(ye, mo, (we - 1) * 7 + 8, 0, 0, 0);
        end.normalise();
        return make_pair(begin, end);
    }
};


std::shared_ptr<ShardStep> ShardStep::create(const std::string& shard_type, const std::string& type)
{
    if (shard_type == ShardWeekly::name())
        return shared_ptr<ShardStep>(new ShardWeekly(type));
    else if (shard_type == ShardMonthly::name())
        return shared_ptr<ShardStep>(new ShardMonthly(type));
    else if (shard_type == ShardYearly::name())
        return shared_ptr<ShardStep>(new ShardYearly(type));
    else
        throw std::runtime_error("shard step '" + shard_type + "' is not supported.  Valid values are weekly, monthly, and yearly.");
}

}
}
