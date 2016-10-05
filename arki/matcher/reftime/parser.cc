#include "parser.h"
#include "arki/wibble/grcal/grcal.h"

using namespace std;
using arki::core::Time;
using arki::core::FuzzyTime;

namespace arki {
namespace matcher {
namespace reftime {

static int lowerbound_sec(const FuzzyTime& t)
{
    int res = 0;
    if (t.ho != -1) res += t.ho * 3600;
    if (t.mi != -1) res += t.mi * 60;
    if (t.se != -1) res += t.se;
    return res;
}

static std::string tosqlTime(const int& tt)
{
    char buf[15];
    snprintf(buf, 15, "'%02d:%02d:%02d'", tt / 3600, (tt % 3600) / 60, tt % 60);
    return buf;
}

static std::string tostringInterval(const int& tt)
{
    stringstream res;
    if (tt / 3600) res << (tt / 3600) << "h";
    if ((tt % 3600) / 60) res << ((tt % 3600) / 60) << "m";
    if (tt % 60) res << (tt % 60) << "s";
    return res.str();
}

static int time_to_seconds(const Time& t)
{
    return t.ho * 3600 + t.mi * 60 + t.se;
}


struct DateLE : public DTMatch
{
    Time ref;
    int tbase;

    DateLE(FuzzyTime* tt)
        : ref(tt->upperbound()), tbase(lowerbound_sec(*tt))
    {
        delete tt;
    }
    bool match(const core::Time& tt) const { return tt <= ref; }
    bool match(const core::Time& begin, const core::Time& end) const { return begin <= ref; }
    string sql(const std::string& column) const { return column + "<='" + ref.to_sql() + "'"; }
    string toString() const { return "<=" + ref.to_sql(); }
    int timebase() const { return tbase; }
    bool restrict_date_range(std::unique_ptr<Time>& begin, std::unique_ptr<Time>& end) const override
    {
        if (begin.get() && *begin > ref)
            return false;

        if (!end.get() || *end > ref)
            end.reset(new Time(ref));

        return true;
    }
};

DTMatch* DTMatch::createLE(FuzzyTime* tt)
{
    return new DateLE(tt);
}

struct DateLT : public DTMatch
{
    Time ref;
    int tbase;

    DateLT(FuzzyTime* tt)
        : ref(tt->lowerbound()), tbase(lowerbound_sec(*tt))
    {
        delete tt;
    }
    bool match(const core::Time& tt) const { return tt < ref; }
    bool match(const core::Time& begin, const core::Time& end) const { return begin < ref; }
    string sql(const std::string& column) const { return column + "<'" + ref.to_sql() + "'"; }
    string toString() const { return "<" + ref.to_sql(); }
    int timebase() const { return tbase; }
    bool restrict_date_range(std::unique_ptr<Time>& begin, std::unique_ptr<Time>& end) const override
    {
        if (begin.get() && *begin >= ref)
            return false;

        if (!end.get() || *end >= ref)
        {
            end.reset(new Time(ref));
            --(end->se);
            end->normalise();
        }

        return true;
    }
};

DTMatch* DTMatch::createLT(FuzzyTime* tt)
{
    return new DateLT(tt);
}

struct DateGE : public DTMatch
{
    Time ref;
    int tbase;
    DateGE(FuzzyTime* tt)
        : ref(tt->lowerbound()), tbase(lowerbound_sec(*tt))
    {
        delete tt;
    }
    bool match(const core::Time& tt) const { return ref <= tt; }
    bool match(const core::Time& begin, const core::Time& end) const { return ref <= end; }
    string sql(const std::string& column) const { return column + ">='" + ref.to_sql() + "'"; }
    string toString() const { return ">=" + ref.to_sql(); }
    int timebase() const { return tbase; }
    bool restrict_date_range(std::unique_ptr<Time>& begin, std::unique_ptr<Time>& end) const override
    {
        if (end.get() && *end < ref)
            return false;

        if (!begin.get() || *begin < ref)
            begin.reset(new Time(ref));

        return true;
    }
};

DTMatch* DTMatch::createGE(FuzzyTime* tt)
{
    return new DateGE(tt);
}

struct DateGT : public DTMatch
{
    Time ref;
    int tbase;
    DateGT(FuzzyTime* tt)
        : ref(tt->upperbound()), tbase(lowerbound_sec(*tt))
    {
        delete tt;
    }
    bool match(const core::Time& tt) const { return ref < tt; }
    bool match(const core::Time& begin, const core::Time& end) const { return ref < end; }
    string sql(const std::string& column) const { return column + ">'" + ref.to_sql() + "'"; }
    string toString() const { return ">" + ref.to_sql(); }
    int timebase() const { return tbase; }
    bool restrict_date_range(std::unique_ptr<Time>& begin, std::unique_ptr<Time>& end) const override
    {
        if (end.get() && *end <= ref)
            return false;

        if (!begin.get() || *begin <= ref)
        {
            begin.reset(new Time(ref));
            ++(begin->se);
            begin->normalise();
        }

        return true;
    }
};

DTMatch* DTMatch::createGT(FuzzyTime* tt)
{
    return new DateGT(tt);
}

struct DateEQ : public DTMatch
{
    Time geref;
    Time leref;
    int tbase;
    DateEQ(FuzzyTime* tt)
        : geref(tt->lowerbound()), leref(tt->upperbound()), tbase(lowerbound_sec(*tt))
    {
        delete tt;
    }
    bool match(const core::Time& tt) const
    {
        return geref <= tt && tt <= leref;
    }
    bool match(const core::Time& begin, const core::Time& end) const
    {
        // If it's an interval, return true if we intersect it
        return (geref <= end && begin <= leref);
    }
    string sql(const std::string& column) const
    {
        return '(' + column + ">='" + geref.to_sql() + "' AND " + column + "<='" + leref.to_sql() + "')";
    }
    string toString() const
    {
        return ">=" + geref.to_sql() + ",<=" + leref.to_sql();
    }
    int timebase() const { return tbase; }
    bool restrict_date_range(std::unique_ptr<Time>& begin, std::unique_ptr<Time>& end) const override
    {
        if (begin.get() && *begin > leref)
            return false;

        if (end.get() && *end < geref)
            return false;

        if (!end.get() || *end > leref)
            end.reset(new Time(leref));

        if (!begin.get() || *begin < geref)
            begin.reset(new Time(geref));

        return true;
    }
};

DTMatch* DTMatch::createEQ(FuzzyTime* tt)
{
    return new DateEQ(tt);
}


struct TimeLE : public DTMatch
{
    int ref;
    TimeLE(const int* tt) : ref(wibble::grcal::dtime::upperbound_sec(tt)) {}
    bool match(const core::Time& tt) const { return time_to_seconds(tt) <= ref; }
    bool match(const core::Time& begin, const core::Time& end) const
    {
        if (Time::duration(begin, end) >= 3600*24) return true;
        if (time_to_seconds(begin) <= ref) return true;
        if (time_to_seconds(end) <= ref) return true;
        return false;
    }
    string sql(const std::string& column) const { return "TIME(" + column + ")<=" + tosqlTime(ref); }
    string toString() const { return "<="+wibble::grcal::dtime::tostring(ref); }
    int timebase() const { return ref; }
    bool restrict_date_range(std::unique_ptr<Time>& begin, std::unique_ptr<Time>& end) const override { return true; }
};
struct TimeLT : public DTMatch
{
    int ref;
    TimeLT(const int* tt) : ref(wibble::grcal::dtime::lowerbound_sec(tt)) {}
    bool match(const core::Time& tt) const { return time_to_seconds(tt) < ref; }
    bool match(const core::Time& begin, const core::Time& end) const
    {
        if (Time::duration(begin, end) >= 3600*24) return true;
        if (time_to_seconds(begin) < ref) return true;
        if (time_to_seconds(end) < ref) return true;
        return false;
    }
    string sql(const std::string& column) const { return "TIME(" + column + ")<" + tosqlTime(ref); }
    string toString() const { return "<"+wibble::grcal::dtime::tostring(ref); }
    int timebase() const { return ref; }
    bool restrict_date_range(std::unique_ptr<Time>& begin, std::unique_ptr<Time>& end) const override { return true; }
};
struct TimeGT : public DTMatch
{
    int ref;
    TimeGT(const int* tt) : ref(wibble::grcal::dtime::upperbound_sec(tt)) {}
    bool match(const core::Time& tt) const { return time_to_seconds(tt) > ref; }
    bool match(const core::Time& begin, const core::Time& end) const
    {
        if (Time::duration(begin, end) >= 3600*24) return true;
        if (time_to_seconds(begin) > ref) return true;
        if (time_to_seconds(end) > ref) return true;
        return false;
    }
    string sql(const std::string& column) const { return "TIME(" + column + ")>" + tosqlTime(ref); }
    string toString() const { return ">"+wibble::grcal::dtime::tostring(ref); }
    int timebase() const { return ref; }
    bool restrict_date_range(std::unique_ptr<Time>& begin, std::unique_ptr<Time>& end) const override { return true; }
};
struct TimeGE : public DTMatch
{
    int ref;
    TimeGE(const int* tt) : ref(wibble::grcal::dtime::lowerbound_sec(tt)) {}
    bool match(const core::Time& tt) const { return time_to_seconds(tt) >= ref; }
    bool match(const core::Time& begin, const core::Time& end) const
    {
        if (Time::duration(begin, end) >= 3600*24) return true;
        if (time_to_seconds(begin) >= ref) return true;
        if (time_to_seconds(end) >= ref) return true;
        return false;
    }
    string sql(const std::string& column) const { return "TIME(" + column + ")>=" + tosqlTime(ref); }
    string toString() const { return ">="+wibble::grcal::dtime::tostring(ref); }
    int timebase() const { return ref; }
    bool restrict_date_range(std::unique_ptr<Time>& begin, std::unique_ptr<Time>& end) const override { return true; }
};
struct TimeEQ : public DTMatch
{
    int geref;
    int leref;
    TimeEQ(const int* tt) : geref(wibble::grcal::dtime::lowerbound_sec(tt)), leref(wibble::grcal::dtime::upperbound_sec(tt)) {}
    bool match(const core::Time& tt) const
    {
        int t = time_to_seconds(tt);
        return t >= geref and t <= leref;
    }
    bool match(const core::Time& begin, const core::Time& end) const
    {
        // If the times are more than 24hours apart, then we necessarily cover a whole day
        if (Time::duration(begin, end) >= 3600*24) return true;

        int rb = time_to_seconds(begin);
        int re = time_to_seconds(end);
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
    string sql(const std::string& column) const
    {
        if (geref == leref)
            return "(TIME(" + column + ")==" + tosqlTime(geref) + ")";
        else
            return "(TIME(" + column + ")>=" + tosqlTime(geref) + " AND TIME(" + column + ")<=" + tosqlTime(leref) + ")";
    }
    string toString() const
    {
        if (geref == leref)
            return "=="+wibble::grcal::dtime::tostring(geref);
        else
            return ">="+wibble::grcal::dtime::tostring(geref)+",<="+wibble::grcal::dtime::tostring(leref);
    }
    int timebase() const { return geref; }
    bool restrict_date_range(std::unique_ptr<Time>& begin, std::unique_ptr<Time>& end) const override { return true; }
};
struct TimeExact : public DTMatch
{
    set<int> times;
    // Set to true when we're not followed by a time to which we provide an interval
    bool lead;
    TimeExact(const set<int>& times, bool lead=false) : times(times), lead(lead)
    {
        //fprintf(stderr, "CREATED %zd times lead %d\n", times.size(), lead);
    }
    bool isLead() const { return lead; }
    bool match(const core::Time& tt) const
    {
        int t = time_to_seconds(tt);
        return times.find(t) != times.end();
    }
    bool restrict_date_range(std::unique_ptr<Time>& begin, std::unique_ptr<Time>& end) const override { return true; }
    bool match(const core::Time& begin, const core::Time& end) const
    {
        if (Time::duration(begin, end) >= 3600*24) return true;
        int rb = time_to_seconds(begin);
        int re = time_to_seconds(end);
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
    string sql(const std::string& column) const
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
    string toString() const
    {
        //fprintf(stderr, "STOS %zd %d\n", times.size(), lead);
        if (times.size() == 1)
        {
            if (lead)
                return "==" + wibble::grcal::dtime::tostring(*times.begin());
            else
                return "% 24";
        }
        else
        {
            set<int>::const_iterator i = times.begin();
            int a = *i;
            ++i;
            int b = *i;
            if (lead)
            {
                string res = "==" + wibble::grcal::dtime::tostring(a) + "%";
                return res + tostringInterval(b-a);
            }
            else
            {
                return "%" + tostringInterval(b-a);
            }
        }
    }
    int timebase() const { return times.empty() ? 0 : *times.begin(); }
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

DTMatch* DTMatch::createTimeLE(const int* tt) { return new TimeLE(tt); }
DTMatch* DTMatch::createTimeLT(const int* tt) { return new TimeLT(tt); }
DTMatch* DTMatch::createTimeGE(const int* tt) { return new TimeGE(tt); }
DTMatch* DTMatch::createTimeGT(const int* tt) { return new TimeGT(tt); }
DTMatch* DTMatch::createTimeEQ(const int* tt) { return new TimeEQ(tt); }

void Parser::add_step(int val, int idx, DTMatch* base)
{
    //fprintf(stderr, "ADD STEP %d %d %s\n", val, idx, base ? base->toString().c_str() : "NONE");

    // Compute all the hh:mm:ss points we want in every day that gets matched,
    // expressed as seconds from midnight.
    set<int> times;
    int timebase = base ? base->timebase() : 0;
    times.insert(timebase);
    int repetition = timesecs(val, idx);
    for (int tstep = timebase + repetition; tstep < 3600*24; tstep += repetition)
        times.insert(tstep);
    for (int tstep = timebase - repetition; tstep >= 0; tstep -= repetition)
        times.insert(tstep);

    res.push_back(new TimeExact(times, base==0));
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
