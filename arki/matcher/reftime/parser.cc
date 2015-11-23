/*
 * matcher/reftime/parser.h - Parser for reftime expressions
 *
 * Copyright (C) 2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */
#include "parser.h"

namespace arki {
namespace matcher {
namespace reftime {

static std::string tosql(const int* tt)
{
    char buf[25];
    snprintf(buf, 25, "'%04d-%02d-%02d %02d:%02d:%02d'",
        tt[0], tt[1], tt[2], tt[3], tt[4], tt[5]);
    return buf;
}

static std::string tosqlTime(const int& tt)
{
    char buf[15];
    snprintf(buf, 15, "'%02d:%02d:%02d'", tt / 3600, (tt % 3600) / 60, tt % 60);
    return buf;
}

static std::string tostringInterval(const int& tt)
{
    string res;
    if (tt / 3600) res += str::fmt(tt / 3600) + "h";
    if ((tt % 3600) / 60) res += str::fmt((tt % 3600) / 60) + "m";
    if (tt % 60) res += str::fmt(tt % 60) + "s";
    return res;
}

static bool isnow(const int* t)
{
    for (int i = 0; i < 6; ++i)
        if (t[i]) return false;
    return true;
}

struct DateLE : public DTMatch
{
    int ref[6];
    int tbase;
    bool match(const int* tt) const { return date::duration(tt, ref) >= 0; }
    bool match(const int* begin, const int* end) const { return date::duration(begin, ref) >= 0; }
    string sql(const std::string& column) const { return column+"<="+tosql(ref); }
    string toString() const { return "<="+date::tostring(ref); }
    int timebase() const { return tbase; }
    bool restrict_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const override
    {
        if (begin.get() && begin->compare_raw(ref) > 0)
            return false;

        if (!end.get() || end->compare_raw(ref) > 0)
            end.reset(new types::Time(ref));

        return true;
    }
};

DTMatch* DTMatch::createLE(const int* tt)
{
    unique_ptr<DateLE> res(new DateLE);
    date::upperbound(tt, res->ref);
    res->tbase = dtime::lowerbound_sec(tt + 3);
    return res.release();
}

struct DateLT : public DTMatch
{
    int ref[6];
    bool match(const int* tt) const { return date::duration(tt, ref) > 0; }
    bool match(const int* begin, const int* end) const { return date::duration(begin, ref) > 0; }
    string sql(const std::string& column) const { return column+"<"+tosql(ref); }
    string toString() const { return "<"+date::tostring(ref); }
    int timebase() const { return dtime::lowerbound_sec(ref+3); }
    bool restrict_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const override
    {
        if (begin.get() && begin->compare_raw(ref) >= 0)
            return false;

        if (!end.get() || end->compare_raw(ref) >= 0)
        {
            end.reset(new types::Time(ref));
            --(end->vals[5]);
            date::normalise(end->vals);
        }

        return true;
    }
};

DTMatch* DTMatch::createLT(const int* tt)
{
    unique_ptr<DateLT> res(new DateLT);
    date::lowerbound(tt, res->ref);
    return res.release();
}

struct DateGE : public DTMatch
{
    int ref[6];
    int tbase;
    bool match(const int* tt) const { return date::duration(ref, tt) >= 0; }
    bool match(const int* begin, const int* end) const { return isnow(end) || date::duration(ref, end) >= 0; }
    string sql(const std::string& column) const { return column+">="+tosql(ref); }
    string toString() const { return ">="+date::tostring(ref); }
    int timebase() const { return tbase; }
    bool restrict_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const override
    {
        if (end.get() && end->compare_raw(ref) < 0)
            return false;

        if (!begin.get() || begin->compare_raw(ref) < 0)
            begin.reset(new types::Time(ref));

        return true;
    }
};

DTMatch* DTMatch::createGE(const int* tt)
{
    unique_ptr<DateGE> res(new DateGE);
    date::lowerbound(tt, res->ref);
    res->tbase = dtime::lowerbound_sec(tt + 3);
    return res.release();
}

struct DateGT : public DTMatch
{
    int ref[6];
    int tbase;
    bool match(const int* tt) const { return date::duration(ref, tt) > 0; }
    bool match(const int* begin, const int* end) const { return isnow(end) || date::duration(ref, end) > 0; }
    string sql(const std::string& column) const { return column+">"+tosql(ref); }
    string toString() const { return ">"+date::tostring(ref); }
    int timebase() const { return tbase; }
    bool restrict_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const override
    {
        if (end.get() && end->compare_raw(ref) <= 0)
            return false;

        if (!begin.get() || begin->compare_raw(ref) <= 0)
        {
            begin.reset(new types::Time(ref));
            ++(begin->vals[5]);
            date::normalise(begin->vals);
        }

        return true;
    }
};

DTMatch* DTMatch::createGT(const int* tt)
{
    unique_ptr<DateGT> res(new DateGT);
    date::upperbound(tt, res->ref);
    res->tbase = dtime::lowerbound_sec(tt + 3);
    return res.release();
}

struct DateEQ : public DTMatch
{
    int geref[6];
    int leref[6];
    bool match(const int* tt) const
    {
        return date::duration(geref, tt) >= 0 && date::duration(tt, leref) >= 0;
    }
    bool match(const int* begin, const int* end) const
    {
        // If it's an interval, return true if we intersect it
        return (isnow(end) || date::duration(geref, end) >= 0) && date::duration(begin, leref) >= 0;
    }
    string sql(const std::string& column) const
    {
        return '(' + column + ">=" + tosql(geref) + " AND " + column + "<=" + tosql(leref) + ')';
    }
    string toString() const
    {
        return ">="+date::tostring(geref)+",<="+date::tostring(leref);
    }
    int timebase() const { return dtime::lowerbound_sec(geref+3); }
    bool restrict_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const override
    {
        if (begin.get() && begin->compare_raw(leref) > 0)
            return false;

        if (end.get() && end->compare_raw(geref) < 0)
            return false;

        if (!end.get() || end->compare_raw(leref) > 0)
            end.reset(new types::Time(leref));

        if (!begin.get() || begin->compare_raw(geref) < 0)
            begin.reset(new types::Time(geref));

        return true;
    }
};

DTMatch* DTMatch::createEQ(const int* tt)
{
    unique_ptr<DateEQ> res(new DateEQ);
    date::lowerbound(tt, res->geref);
    date::upperbound(tt, res->leref);
    return res.release();
}


struct TimeLE : public DTMatch
{
    int ref;
    TimeLE(const int* tt) : ref(dtime::upperbound_sec(tt)) {}
    bool match(const int* tt) const { return dtime::upperbound_sec(tt+3) <= ref; }
    bool match(const int* begin, const int* end) const
    {
        int d = date::duration(begin, end);
        if (d >= 3600*24) return true;
        if (dtime::upperbound_sec(begin+3) <= ref) return true;
        if (dtime::upperbound_sec(end+3) <= ref) return true;
        return false;
    }
    string sql(const std::string& column) const { return "TIME("+column+")<="+tosqlTime(ref); }
    string toString() const { return "<="+dtime::tostring(ref); }
    int timebase() const { return ref; }
};
struct TimeLT : public DTMatch
{
    int ref;
    TimeLT(const int* tt) : ref(dtime::lowerbound_sec(tt)) {}
    bool match(const int* tt) const { return dtime::upperbound_sec(tt+3) < ref; }
    bool match(const int* begin, const int* end) const
    {
        int d = date::duration(begin, end);
        if (d >= 3600*24) return true;
        if (dtime::upperbound_sec(begin+3) < ref) return true;
        if (dtime::upperbound_sec(end+3) < ref) return true;
        return false;
    }
    string sql(const std::string& column) const { return "TIME("+column+")<"+tosqlTime(ref); }
    string toString() const { return "<"+dtime::tostring(ref); }
    int timebase() const { return ref; }
};
struct TimeGT : public DTMatch
{
    int ref;
    TimeGT(const int* tt) : ref(dtime::upperbound_sec(tt)) {}
    bool match(const int* tt) const { return dtime::lowerbound_sec(tt+3) > ref; }
    bool match(const int* begin, const int* end) const
    {
        int d = date::duration(begin, end);
        if (d >= 3600*24) return true;
        if (dtime::lowerbound_sec(begin+3) > ref) return true;
        if (dtime::lowerbound_sec(end+3) > ref) return true;
        return false;
    }
    string sql(const std::string& column) const { return "TIME("+column+")>"+tosqlTime(ref); }
    string toString() const { return ">"+dtime::tostring(ref); }
    int timebase() const { return ref; }
};
struct TimeGE : public DTMatch
{
    int ref;
    TimeGE(const int* tt) : ref(dtime::lowerbound_sec(tt)) {}
    bool match(const int* tt) const { return dtime::lowerbound_sec(tt+3) >= ref; }
    bool match(const int* begin, const int* end) const
    {
        int d = date::duration(begin, end);
        if (d >= 3600*24) return true;
        if (dtime::lowerbound_sec(begin+3) >= ref) return true;
        if (dtime::lowerbound_sec(end+3) >= ref) return true;
        return false;
    }
    string sql(const std::string& column) const { return "TIME("+column+")>="+tosqlTime(ref); }
    string toString() const { return ">="+dtime::tostring(ref); }
    int timebase() const { return ref; }
};
struct TimeEQ : public DTMatch
{
    int geref;
    int leref;
    TimeEQ(const int* tt) : geref(dtime::lowerbound_sec(tt)), leref(dtime::upperbound_sec(tt)) {}
    bool match(const int* tt) const { return dtime::lowerbound_sec(tt+3) >= geref and dtime::upperbound_sec(tt+3) <= leref; }
    bool match(const int* begin, const int* end) const
    {
        // If the times are more than 24hours apart, then we necessarily cover a whole day
        int d = date::duration(begin, end);
        if (d >= 3600*24) return true;
        //if (timeOnly(begin) < leref) return false;
        //if (timeOnly(end) > geref) return false;

        int rb = dtime::lowerbound_sec(begin+3);
        int re = dtime::upperbound_sec(end+3);
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
            return "(TIME(" + column + ")==" + tosqlTime(geref) + ')';
        else
            return "(TIME(" + column + ")>=" + tosqlTime(geref) + " AND TIME(" + column + ")<=" + tosqlTime(leref) + ')';
    }
    string toString() const
    {
        if (geref == leref)
            return "=="+dtime::tostring(geref);
        else
            return ">="+dtime::tostring(geref)+",<="+dtime::tostring(leref);
    }
    int timebase() const { return geref; }
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
    bool match(const int* tt) const
    {
        int t = dtime::lowerbound_sec(tt+3);
        return times.find(t) != times.end();
    }
    bool match(const int* begin, const int* end) const
    {
        int d = date::duration(begin, end);
        if (d >= 3600*24) return true;
        int rb = dtime::lowerbound_sec(begin+3);
        int re = dtime::upperbound_sec(end+3);
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
                return "==" + dtime::tostring(*times.begin());
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
                string res = "==" + dtime::tostring(a) + "%";
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

}
}
}
