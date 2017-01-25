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
        throw std::runtime_error("list_segments not implemented for this step");
    }

    void impl_list_segments(const PathQuery& q, StepParser& sp, std::function<void(std::string&& s)> dest) const
    {
        impl_list_segments(q, sp, string(), 0, dest);
    }

    void impl_list_segments(const PathQuery& q, StepParser& sp, const std::string& relpath, unsigned depth, std::function<void(std::string&& s)> dest) const
    {
        bool is_leaf = depth == sp.max_depth - 1;
        string root;
        if (depth == 0)
            root = q.root;
        else
            root = str::joinpath(q.root, relpath);

        sys::Path dir(root);
        for (sys::Path::iterator f = dir.begin(); f != dir.end(); ++f)
        {
            if (f->d_name[0] == '.') continue;
            if (!is_leaf && !f.isdir()) continue;
            if (!sp.parse_component(q, depth, f->d_name)) continue;

            // Its period must match the matcher in q
            if (q.m)
            {
                core::Time begin;
                core::Time until;
                sp.timespan(begin, until);
                auto rt = Reftime::createPeriod(begin, until);
                if (!q.m->matchItem(*rt)) continue;
            }

            if (is_leaf)
                dest(str::joinpath(relpath, f->d_name));
            else
                impl_list_segments(q, sp, str::joinpath(relpath, f->d_name), depth + 1, dest);
        }
    }
};

struct SubStep : public BaseStep
{
    int year;
    SubStep(int year) : year(year) {}
};

struct YearlyStepParser : public StepParser
{
    ERegexp re_century;
    ERegexp re_year;
    unsigned century;
    unsigned year;

    YearlyStepParser() :
        StepParser(2),
        re_century("^([[:digit:]]{2})$", 2),
        re_year("^([[:digit:]]{4})\\.([[:alnum:]]+)$", 3) {}

    bool parse_component(const PathQuery& q, unsigned depth, const char* name) override
    {
        this->depth = depth;
        switch (depth)
        {
            case 0:
                if (!re_century.match(name)) return false;
                century = std::stoul(re_century[1]);
                return true;
            case 1:
                if (!re_year.match(name)) return false;
                if (re_year[2] != q.format) return false;
                year = std::stoul(re_year[1]);
                return true;
            default:
                throw std::runtime_error("invalid depth " + std::to_string(depth));
        }
    }

    void timespan(Time& start_time, Time& end_time) override
    {
        switch (depth)
        {
            case 0:
                start_time.set_lowerbound(century * 100);
                end_time.set_upperbound(century * 100 + 99);
                break;
            case 1:
                start_time.set_lowerbound(year);
                end_time.set_upperbound(year);
                break;
        }
    }
};

struct Yearly : public BaseStep
{
    static const char* name() { return "yearly"; }

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

    void list_segments(const std::string& root, const std::string& format, const Matcher& m, std::function<void(std::string&&)> dest) const override
    {
        YearlyStepParser sp;
        impl_list_segments(PathQuery(root, format, m), sp, dest);
    }

    std::string operator()(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[9];
        snprintf(buf, 9, "%02d/%04d", tt.ye/100, tt.ye);
        return buf;
    }
};

struct MonthlyStepParser : public StepParser
{
    ERegexp re_year;
    ERegexp re_month;
    unsigned year;
    unsigned month;

    MonthlyStepParser() :
        StepParser(2),
        re_year("^([[:digit:]]{4})$", 2),
        re_month("^([[:digit:]]{2})\\.([[:alnum:]]+)$", 3) {}

    bool parse_component(const PathQuery& q, unsigned depth, const char* name) override
    {
        this->depth = depth;
        switch (depth)
        {
            case 0:
                if (!re_year.match(name)) return false;
                year = std::stoul(re_year[1]);
                return true;
            case 1:
                if (!re_month.match(name)) return false;
                if (re_month[2] != q.format) return false;
                month = std::stoul(re_month[1]);
                return true;
            default:
                throw std::runtime_error("invalid depth " + std::to_string(depth));
        }
    }

    void timespan(Time& start_time, Time& end_time) override
    {
        switch (depth)
        {
            case 0:
                start_time.set_lowerbound(year);
                end_time.set_upperbound(year);
                break;
            case 1:
                start_time.set_lowerbound(year, month);
                end_time.set_upperbound(year, month);
                break;
        }
    }
};

struct Monthly : public BaseStep
{
    static const char* name() { return "monthly"; }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo;
        if (sscanf(path.c_str(), "%04d/%02d", &ye, &mo) == 0)
            return false;

        start_time.set_lowerbound(ye, mo);
        end_time.set_upperbound(ye, mo);
        return true;
    }

    void list_segments(const std::string& root, const std::string& format, const Matcher& m, std::function<void(std::string&&)> dest) const override
    {
        MonthlyStepParser sp;
        impl_list_segments(PathQuery(root, format, m), sp, dest);
    }

    std::string operator()(const core::Time& time) const override
    {
        const Time& tt = time;
        char buf[10];
        snprintf(buf, 10, "%04d/%02d", tt.ye, tt.mo);
        return buf;
    }

#if 0
    void subpath_timespan(unsigned sp_value, Time& start_time, Time& end_time) const override
    {
        start_time.set_lowerbound(sp_value);
        end_time.set_upperbound(sp_value);
    }

    void segment_timespan(unsigned sp_value, const Regexp& re, Time& start_time, Time& end_time) const override
    {
        unsigned month = strtoul(re[1].c_str(), 0, 10);
        start_time.set_lowerbound(sp_value, month);
        end_time.set_upperbound(sp_value, month);
    }

    std::vector<std::string> list_paths(const PathQuery& q) const override
    {
        ERegexp re("^([[:digit:]]{2})$", 1);
        return impl_list_paths(q, 4, re);
    }
#endif
};

struct SubMonthly : public SubStep
{
    using SubStep::SubStep;

    static const char* name() { return "monthly"; }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int mo;
        if (sscanf(path.c_str(), "%02d", &mo) == 0)
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
        if (sscanf(path.c_str(), "%04d/%02d-%d", &ye, &mo, &biweek) == 0)
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

#if 0
    void subpath_timespan(unsigned sp_value, Time& start_time, Time& end_time) const override
    {
        start_time.set_lowerbound(sp_value);
        end_time.set_upperbound(sp_value);
    }

    void segment_timespan(unsigned sp_value, const Regexp& re, Time& start_time, Time& end_time) const override
    {
        unsigned month = strtoul(re[1].c_str(), 0, 10);
        unsigned biweek = strtoul(re[2].c_str(), 0, 10);
        if (biweek == 1)
        {
            start_time.set_lowerbound(sp_value, month);
            end_time.set_upperbound(sp_value, month, 15);
        } else {
            start_time.set_lowerbound(sp_value, month, 16);
            end_time.set_upperbound(sp_value, month);
        }
    }

    std::vector<std::string> list_paths(const PathQuery& q) const override
    {
        ERegexp re("^([[:digit:]]{2})-([[:digit:]])$", 2);
        return impl_list_paths(q, 4, re);
    }
#endif
};

struct Weekly : public BaseStep
{
    static const char* name() { return "weekly"; }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo = -1, week = -1;
        if (sscanf(path.c_str(), "%04d/%02d-%d", &ye, &mo, &week) == 0)
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
        if (sscanf(path.c_str(), "%02d-%d", &mo, &week) == 0)
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

struct DailyStepParser : public StepParser
{
    ERegexp re_year;
    ERegexp re_month;
    unsigned year;
    unsigned month;
    unsigned day;

    DailyStepParser() :
        StepParser(2),
        re_year("^([[:digit:]]{4})$", 2),
        re_month("^([[:digit:]]{2})-([[:digit:]]{2})\\.([[:alnum:]]+)$", 4) {}

    bool parse_component(const PathQuery& q, unsigned depth, const char* name) override
    {
        this->depth = depth;
        switch (depth)
        {
            case 0:
                if (!re_year.match(name)) return false;
                year = std::stoul(re_year[0]);
                return true;
            case 1:
                if (!re_month.match(name)) return false;
                if (re_month[3] != q.format) return false;
                month = std::stoul(re_month[1]);
                day = std::stoul(re_month[2]);
                return true;
            default:
                throw std::runtime_error("invalid depth " + std::to_string(depth));
        }
    }

    void timespan(Time& start_time, Time& end_time) override
    {
        switch (depth)
        {
            case 0:
                start_time.set_lowerbound(year);
                end_time.set_upperbound(year);
                break;
            case 1:
                start_time.set_lowerbound(year, month, day);
                end_time.set_upperbound(year, month, day);
                break;
        }
    }
};

struct Daily : public BaseStep
{
    static const char* name() { return "daily"; }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo, da;
        if (sscanf(path.c_str(), "%04d/%02d-%02d", &ye, &mo, &da) == 0)
            return false;
        start_time.set_lowerbound(ye, mo, da);
        end_time.set_upperbound(ye, mo, da);
        return true;
    }

    void list_segments(const std::string& root, const std::string& format, const Matcher& m, std::function<void(std::string&&)> dest) const override
    {
        DailyStepParser sp;
        impl_list_segments(PathQuery(root, format, m), sp, dest);
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
        if (sscanf(path.c_str(), "%02d/%02d", &mo, &da) == 0)
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
