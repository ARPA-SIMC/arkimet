#include "parser.h"
#include "arki/core/time.h"
#include "arki/core/fuzzytime.h"
#include <sstream>
#include <set>
#include <memory>

using namespace std;
using arki::core::Time;
using arki::core::Interval;
using arki::core::FuzzyTime;

namespace arki {
namespace matcher {
namespace reftime {

static int lowerbound_sec(const int* src)
{
    int res = 0;
    if (src[0] != -1) res += src[0] * 3600;
    if (src[1] != -1) res += src[1] * 60;
    if (src[2] != -1) res += src[2];
    return res;
}

static int upperbound_sec(const int* src)
{
    int res = 0;
    res += (src[0] != -1 ? src[0] : 23) * 3600;
    res += (src[1] != -1 ? src[1] : 59) * 60;
    res += (src[2] != -1 ? src[2] : 59);
    return res;
}

static std::string formatTime(const int& tt)
{
    char buf[20];
    snprintf(buf, 20, "%02d:%02d:%02d", tt / 3600, (tt % 3600) / 60, tt % 60);
    return buf;
}

static std::string tosqlTime(const int& tt)
{
    char buf[20];
    snprintf(buf, 20, "'%02d:%02d:%02d'", tt / 3600, (tt % 3600) / 60, tt % 60);
    return buf;
}

static std::string tostringInterval(const int& tt)
{
    std::stringstream res;
    if (tt / 3600) res << (tt / 3600) << "h";
    if ((tt % 3600) / 60) res << ((tt % 3600) / 60) << "m";
    if (tt % 60) res << (tt % 60) << "s";
    return res.str();
}

static int time_to_seconds(const Time& t)
{
    return t.ho * 3600 + t.mi * 60 + t.se;
}

struct DateInterval : public DTMatch
{
    Interval interval;

    DateInterval(const core::Interval& interval)
        : interval(interval)
    {
    }

    bool match(const core::Time& tt) const override
    {
        return interval.contains(tt);
    }

    bool match(const core::Interval& interval) const override
    {
        return this->interval.intersects(interval);
    }

    bool intersect_interval(core::Interval& interval) const override
    {
        return interval.intersect(this->interval);
    }

    std::string sql(const std::string& column) const override
    {
        std::stringstream res;
        if (interval.begin.is_set())
        {
            if (interval.end.is_set())
                res << "(" << column << ">='" << interval.begin.to_sql() << "' AND " << column << "<'" << interval.end.to_sql() << "')";
            else
                res << column << ">='" << interval.begin.to_sql() << "'";
        } else {
            if (interval.end.is_set())
                res << column << "<'" << interval.end.to_sql() << "'";
            else
                res << "1=1";
        }
        return res.str();
    }

    std::string toString() const override
    {
        std::stringstream res;
        if (interval.begin.is_set())
        {
            if (interval.end.is_set())
                res << ">=" << interval.begin.to_sql() << ",<" << interval.end.to_sql();
            else
                res << ">=" << interval.begin.to_sql();
        } else {
            if (interval.end.is_set())
                res << "<" << interval.end.to_sql();
        }
        return res.str();
    }
};


struct TimeMatch : public DTMatch
{
    int ref;

    TimeMatch(int ref) : ref(ref) {}

    bool intersect_interval(core::Interval& interval) const override { return true; }
};

struct TimeLE : public TimeMatch
{
    using TimeMatch::TimeMatch;
    bool match(const core::Time& tt) const override { return time_to_seconds(tt) <= ref; }
    bool match(const core::Interval& interval) const override
    {
        if (Time::duration(interval) >= 3600*24) return true;
        if (time_to_seconds(interval.begin) <= ref) return true;
        if (time_to_seconds(interval.end) <= ref) return true;
        return false;
    }
    string sql(const std::string& column) const override { return "TIME(" + column + ")<=" + tosqlTime(ref); }
    string toString() const override { return "<="+formatTime(ref); }
};

struct TimeLT : public TimeMatch
{
    using TimeMatch::TimeMatch;
    bool match(const core::Time& tt) const override { return time_to_seconds(tt) < ref; }
    bool match(const core::Interval& interval) const override
    {
        if (Time::duration(interval) >= 3600*24) return true;
        if (time_to_seconds(interval.begin) < ref) return true;
        if (time_to_seconds(interval.end) < ref) return true;
        return false;
    }
    string sql(const std::string& column) const override { return "TIME(" + column + ")<" + tosqlTime(ref); }
    string toString() const override { return "<"+formatTime(ref); }
};

struct TimeGT : public TimeMatch
{
    using TimeMatch::TimeMatch;
    bool match(const core::Time& tt) const override { return time_to_seconds(tt) > ref; }
    bool match(const core::Interval& interval) const override
    {
        if (Time::duration(interval) >= 3600*24) return true;
        if (time_to_seconds(interval.begin) > ref) return true;
        if (time_to_seconds(interval.end) > ref) return true;
        return false;
    }
    string sql(const std::string& column) const override { return "TIME(" + column + ")>" + tosqlTime(ref); }
    string toString() const override { return ">"+formatTime(ref); }
};

struct TimeGE : public TimeMatch
{
    using TimeMatch::TimeMatch;
    bool match(const core::Time& tt) const override { return time_to_seconds(tt) >= ref; }
    bool match(const core::Interval& interval) const override
    {
        if (Time::duration(interval) >= 3600*24) return true;
        if (time_to_seconds(interval.begin) >= ref) return true;
        if (time_to_seconds(interval.end) >= ref) return true;
        return false;
    }
    string sql(const std::string& column) const override { return "TIME(" + column + ")>=" + tosqlTime(ref); }
    string toString() const override { return ">="+formatTime(ref); }
    bool intersect_interval(core::Interval& interval) const override { return true; }
};

struct TimeEQ : public DTMatch
{
    int geref;
    int leref;
    TimeEQ(int geref, int leref) : geref(geref), leref(leref) {}
    bool match(const core::Time& tt) const override
    {
        int t = time_to_seconds(tt);
        return t >= geref and t <= leref;
    }
    bool match(const core::Interval& interval) const override
    {
        // If the times are more than 24hours apart, then we necessarily cover a whole day
        if (Time::duration(interval) >= 3600*24) return true;

        int rb = time_to_seconds(interval.begin);
        int re = time_to_seconds(interval.end);
        if (rb <= re)
        {
            if (rb <= geref && leref <= re)
                return true;
        }
        else
        {
            if (leref <= re || rb <= geref)
                return true;
        }

        return false;
    }
    string sql(const std::string& column) const override
    {
        if (geref == leref)
            return "(TIME(" + column + ")==" + tosqlTime(geref) + ")";
        else
            return "(TIME(" + column + ")>=" + tosqlTime(geref) + " AND TIME(" + column + ")<=" + tosqlTime(leref) + ")";
    }
    string toString() const override
    {
        if (geref == leref)
            return "=="+formatTime(geref);
        else
            return ">="+formatTime(geref)+",<="+formatTime(leref);
    }
    bool intersect_interval(core::Interval& interval) const override { return true; }
};

struct TimeExact : public DTMatch
{
    std::set<int> times;
    TimeExact(const set<int>& times) : times(times)
    {
        //fprintf(stderr, "CREATED %zd times lead %d\n", times.size(), lead);
    }
    bool match(const core::Time& tt) const override
    {
        int t = time_to_seconds(tt);
        return times.find(t) != times.end();
    }
    bool intersect_interval(core::Interval& interval) const override { return true; }
    bool match(const core::Interval& interval) const override
    {
        if (Time::duration(interval) >= 3600*24) return true;
        int rb = time_to_seconds(interval.begin);
        int re = time_to_seconds(interval.end);
        for (set<int>::const_iterator i = times.begin(); i != times.end(); ++i)
        {
            if (rb <= re)
            {
                if (*i >= rb && *i <= re)
                    return true;
            }
            else
            {
                if (*i >= rb || *i <= re)
                    return true;
            }
        }
        return false;
    }
    string sql(const std::string& column) const override
    {
        string res = "(";
        bool first = true;
        for (set<int>::const_iterator i = times.begin(); i != times.end(); ++i)
        {
            if (first)
                first = false;
            else
                res += " OR ";
            res += "TIME(" + column + ")==" + tosqlTime(*i);
        }
        return res + ")";
    }
    string toString() const override
    {
        std::string res;
        std::set<int>::const_iterator i = times.begin();

        if (*i)
        {
            res += '@';
            res += formatTime(*i);
        }

        //fprintf(stderr, "STOS %zd %d\n", times.size(), lead);
        if (times.size() == 1)
        {
            res += "%24h";
        }
        else
        {
            int a = *i;
            ++i;
            int b = *i;
            res += '%';
            res += tostringInterval(b-a);
        }

        return res;
    }
};


static inline int timesecs(int val, int idx)
{
    switch (idx)
    {
        case 3: return val * 3600;
        case 4: return val * 60;
        case 5: return val;
        default: return 0;
    }
}

DTMatch* Parser::createLE(FuzzyTime* tt)
{
    if (timebase == -1) timebase = time_to_seconds(tt->lowerbound());
    Time ref = tt->upperbound();
    ++ref.se;
    ref.normalise();
    delete tt;
    return new DateInterval(Interval(Time(), ref));
}

DTMatch* Parser::createLT(FuzzyTime* tt)
{
    Time ref = tt->lowerbound();
    if (timebase == -1) timebase = time_to_seconds(ref);
    delete tt;
    return new DateInterval(Interval(Time(), ref));
}

DTMatch* Parser::createGE(FuzzyTime* tt)
{
    Time ref(tt->lowerbound());
    if (timebase == -1) timebase = time_to_seconds(ref);
    delete tt;
    return new DateInterval(Interval(ref, Time()));
}

DTMatch* Parser::createGT(FuzzyTime* tt)
{
    if (timebase == -1) timebase = time_to_seconds(tt->lowerbound());
    Time ref(tt->upperbound());
    ++ref.se;
    ref.normalise();
    delete tt;
    return new DateInterval(Interval(ref, Time()));
}

DTMatch* Parser::createEQ(FuzzyTime* tt)
{
    Time geref(tt->lowerbound());
    if (timebase == -1) timebase = time_to_seconds(geref);
    Time ltref(tt->upperbound());
    ++ltref.se;
    ltref.normalise();
    delete tt;
    return new DateInterval(Interval(geref, ltref));
}

DTMatch* DTMatch::createInterval(const core::Interval& interval)
{
    return new DateInterval(interval);
}

DTMatch* Parser::createTimeLE(const int* tt)
{
    if (timebase == -1) timebase = lowerbound_sec(tt);
    return new TimeLE(upperbound_sec(tt));
}

DTMatch* Parser::createTimeLT(const int* tt)
{
    int val = lowerbound_sec(tt);
    if (timebase == -1) timebase = val;
    return new TimeLT(val);
}

DTMatch* Parser::createTimeGE(const int* tt)
{
    int val = lowerbound_sec(tt);
    if (timebase == -1) timebase = val;
    return new TimeGE(val);
}

DTMatch* Parser::createTimeGT(const int* tt)
{
    timebase = lowerbound_sec(tt);
    return new TimeGT(upperbound_sec(tt));
}

DTMatch* Parser::createTimeEQ(const int* tt)
{
    int val = lowerbound_sec(tt);
    if (timebase == -1) timebase = val;
    return new TimeEQ(val, upperbound_sec(tt));
}

DTMatch* Parser::createStep(int val, int idx, const int* tt)
{
    if (tt)
        timebase = lowerbound_sec(tt);
    else if (timebase == -1)
        timebase = 0;

    std::set<int> times;
    int repetition = timesecs(val, idx);

    for (int step = timebase % repetition; step < 3600 * 24; step += repetition)
        times.emplace(step);

    return new TimeExact(times);
}

void Parser::add(DTMatch* val)
{
    //fprintf(stderr, "ADD %s\n", t->toString().c_str());
    res.push_back(val);
}

arki::core::FuzzyTime* Parser::mknow()
{
    struct tm v;
    gmtime_r(&tnow, &v);
    unique_ptr<arki::core::FuzzyTime> res(new arki::core::FuzzyTime(v));
    return res.release();
}

arki::core::FuzzyTime* Parser::mktoday()
{
    struct tm v;
    gmtime_r(&tnow, &v);
    unique_ptr<arki::core::FuzzyTime> res(new arki::core::FuzzyTime(v));
    res->ho = res->mi = res->se = -1;
    return res.release();
}

arki::core::FuzzyTime* Parser::mkyesterday()
{
    time_t tv = tnow - 3600*24;
    struct tm v;
    gmtime_r(&tv, &v);
    unique_ptr<arki::core::FuzzyTime> res(new arki::core::FuzzyTime(v));
    res->ho = res->mi = res->se = -1;
    return res.release();
}

arki::core::FuzzyTime* Parser::mktomorrow()
{
    time_t tv = tnow + 3600*24;
    struct tm v;
    gmtime_r(&tv, &v);
    unique_ptr<arki::core::FuzzyTime> res(new arki::core::FuzzyTime(v));
    res->ho = res->mi = res->se = -1;
    return res.release();
}


}
}
}
