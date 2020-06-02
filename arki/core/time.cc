#include "time.h"
#include "binary.h"
#include "arki/exceptions.h"
#include <ostream>
#include <cmath>
#include <cstring>
#include <ctime>
#include <cstdio>

using namespace std;

namespace arki {
namespace core {

TimeBase::TimeBase(struct tm& t) { set_tm(t); }

const bool TimeBase::is_set() const
{
    // Don't bother checking all elements, since we don't work with dates in
    // year zero.
    return ye != 0;
}

void TimeBase::set_tm(struct tm& t)
{
    ye = t.tm_year + 1900;
    mo = t.tm_mon + 1;
    da = t.tm_mday;
    ho = t.tm_hour;
    mi = t.tm_min;
    se = t.tm_sec;
}

void TimeBase::set_iso8601(const std::string& str)
{
    int count = sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &ye, &mo, &da, &ho, &mi, &se);
    if (count < 6)
        count = sscanf(str.c_str(), "%d-%d-%dT%d:%d:%d", &ye, &mo, &da, &ho, &mi, &se);
    if (count < 6)
        throw std::runtime_error("Cannot parse ISO-8601 string '" + str + "'");
}

void TimeBase::set_sql(const std::string& str)
{
    int count = sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &ye, &mo, &da, &ho, &mi, &se);
    if (count == 0)
        throw std::runtime_error("Cannot parse SQL string '" + str + "'");
}

void TimeBase::set_now()
{
    time_t timet_now = time(0);
    struct tm now;
    gmtime_r(&timet_now, &now);
    set_tm(now);
}

void TimeBase::set_easter(int year)
{
    // Meeus/Jones/Butcher Gregorian algorithm
    // from http://en.wikipedia.org/wiki/Computus
    int a = year % 19;
    int b = year / 100;
    int c = year % 100;
    int d = b / 4;
    int e = b % 4;
    int f = (b + 8) / 25;
    int g = (b - f + 1) / 3;
    int h = (19 * a + b - d - g + 15) % 30;
    int i = c / 4;
    int k = c % 4;
    int L = (32 + 2 * e + 2 * i - h - k) % 7;
    int m = (a + 11 * h + 22 * L) / 451;
    ye = year;
    mo = (h + L - 7 * m + 114) / 31;
    da = ((h + L - 7 * m + 114) % 31) + 1;
    ho = 0;
    mi = 0;
    se = 0;
}

int TimeBase::compare(const TimeBase& o) const
{
    if (int res = ye - o.ye) return res;
    if (int res = mo - o.mo) return res;
    if (int res = da - o.da) return res;
    if (int res = ho - o.ho) return res;
    if (int res = mi - o.mi) return res;
    return se - o.se;
}

bool TimeBase::operator==(const TimeBase& o) const
{
    return ye == o.ye && mo == o.mo && da == o.da
        && ho == o.ho && mi == o.mi && se == o.se;
}

bool TimeBase::operator!=(const TimeBase& o) const
{
    return ye != o.ye || mo != o.mo || da != o.da
        || ho != o.ho || mi != o.mi || se != o.se;
}

bool TimeBase::operator!=(const TimeBase& o) const;



Time Time::create_iso8601(const std::string& str)
{
    Time res;
    res.set_iso8601(str);
    return res;
}

Time Time::create_sql(const std::string& str)
{
    Time res;
    res.set_sql(str);
    return res;
}

Time Time::create_now()
{
    time_t timet_now = time(nullptr);
    struct tm now;
    gmtime_r(&timet_now, &now);
    return Time(now);
}

Time Time::create_lowerbound(int ye, int mo, int da, int ho, int mi, int se)
{
    Time res;
    res.set_lowerbound(ye, mo, da, ho, mi, se);
    return res;
}

Time Time::create_upperbound(int ye, int mo, int da, int ho, int mi, int se)
{
    Time res;
    res.set_upperbound(ye, mo, da, ho, mi, se);
    return res;
}

void Time::unset()
{
    this->ye = 0;
    this->mo = 0;
    this->da = 0;
    this->ho = 0;
    this->mi = 0;
    this->se = 0;
}

void Time::set(int ye, int mo, int da, int ho, int mi, int se)
{
    this->ye = ye;
    this->mo = mo;
    this->da = da;
    this->ho = ho;
    this->mi = mi;
    this->se = se;
}

void Time::set_lowerbound(int ye, int mo, int da, int ho, int mi, int se)
{
    this->ye = ye;
    this->mo = mo != -1 ? mo : 1;
    this->da = da != -1 ? da : 1;
    this->ho = ho != -1 ? ho : 0;
    this->mi = mi != -1 ? mi : 0;
    this->se = se != -1 ? se : 0;
}

void Time::set_upperbound(int ye, int mo, int da, int ho, int mi, int se)
{
    // Lowerbound, but increment the last valid value by 1
    if (mo != -1)
        this->ye = ye;
    else
        this->ye = ye + 1;

    if (mo == -1)
        this->mo = 1;
    else if (da != -1)
        this->mo = mo;
    else
        this->mo = mo + 1;

    if (da == -1)
        this->da = 1;
    else if (ho != -1)
        this->da = da;
    else
        this->da = da + 1;

    if (ho == -1)
        this->ho = 0;
    else if (mi != -1)
        this->ho = ho;
    else
        this->ho = ho + 1;

    if (mi == -1)
        this->mi = 0;
    else if (se != -1)
        this->mi = mi;
    else
        this->mi = mi + 1;

    if (se == -1)
        this->se = 0;
    else
        this->se = se + 1;

    // Decrement the seconds, to get an inclusive range
    --(this->se);

    // Normalise the result
    normalise();
}

bool Time::operator==(const std::string& o) const
{
    return TimeBase::operator==(Time::create_iso8601(o));
}

Time Time::start_of_month() const
{
    return Time(ye, mo, 1);
}

Time Time::start_of_next_month() const
{
    if (mo == 12)
        return Time(ye + 1, 1, 1);
    else
        return Time(ye, mo + 1, 1);
}

Time Time::start_of_previous_month() const
{
    if (mo == 1)
        return Time(ye - 1, 12, 1);
    else
        return Time(ye, mo - 1, 1);
}

Time Time::end_of_month() const
{
    return Time(ye, mo, days_in_month(ye, mo), 23, 59, 59);
}

Time Time::prev_instant() const
{
    Time res(*this);
    res.se -= 1;
    res.normalise();
    return res;
}

Time Time::next_instant() const
{
    Time res(*this);
    res.se += 1;
    res.normalise();
    return res;
}

bool Time::is_start_of_month() const
{
    return da == 1 && ho == 0 && mi == 0 && se == 0;
}

/*
 * Make sure `lo` fits between >= 0 and < N.
 *
 * Adjust `hi` so that hi * N + lo remains the same value.
 */
static inline void normalN(int& lo, int& hi, int N)
{
    if (lo < 0)
    {
        int m = (-lo)/N;
        if (lo % N) ++m;
        hi -= m;
        lo = (lo + (m*N)) % N;
    } else {
        hi += lo / N;
        lo = lo % N;
    }
}

void Time::normalise()
{
    // Rebase day and month numbers on 0
    --mo;
    --da;

    // Normalise seconds
    normalN(se, mi, 60);

    // Normalise minutes
    normalN(mi, ho, 60);

    // Normalise hours
    normalN(ho, da, 24);

    // Normalise days
    while (da < 0)
    {
        --mo;
        normalN(mo, ye, 12);
        da += days_in_month(ye, mo + 1);
    }
    while (true)
    {
        normalN(mo, ye, 12);
        int dim = days_in_month(ye, mo + 1);
        if (da < dim) break;
        da -= dim;
        ++mo;
    }

    // Normalise months
    normalN(mo, ye, 12);

    ++mo;
    ++da;
}

std::string Time::to_iso8601(char sep) const
{
    char buf[25];
    snprintf(buf, 25, "%04d-%02d-%02d%c%02d:%02d:%02dZ", ye, mo, da, sep, ho, mi, se);
    return buf;
}

std::string Time::to_sql() const
{
    char buf[25];
    snprintf(buf, 25, "%04d-%02d-%02d %02d:%02d:%02d", ye, mo, da, ho, mi, se);
    return buf;
}

time_t Time::to_unix() const
{
    if (ye < 1970) return 0;
    struct tm t;
    t.tm_sec = se;
    t.tm_min = mi;
    t.tm_hour = ho;
    t.tm_mday = da;
    t.tm_mon = mo - 1;
    t.tm_year = ye - 1900;
    return timegm(&t);
}

Time Time::decode(core::BinaryDecoder& dec)
{
    uint32_t a = dec.pop_uint(4, "first 32 bits of encoded time");
    uint32_t b = dec.pop_uint(1, "last 8 bits of encoded time");
    return Time(
        a >> 18,
        (a >> 14) & 0xf,
        (a >> 9) & 0x1f,
        (a >> 4) & 0x1f,
        ((a & 0xf) << 2) | ((b >> 6) & 0x3),
        b & 0x3f);
}

Time Time::decodeString(const std::string& val)
{
    return Time::create_iso8601(val);
}

void Time::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    uint32_t a = ((ye & 0x3fff) << 18)
               | ((mo & 0xf)    << 14)
               | ((da & 0x1f)   << 9)
               | ((ho & 0x1f)   << 4)
               | ((mi >> 2) & 0xf);
    uint32_t b = ((mi & 0x3) << 6)
               | (se & 0x3f);
    enc.add_unsigned(a, 4);
    enc.add_unsigned(b, 1);
}

int Time::days_in_month(int year, int month)
{
    switch (month)
    {
        case  1: return 31;
        case  2:
            if (year % 400 == 0 || (year % 4 == 0 && ! (year % 100 == 0)))
                return 29;
            return 28;
        case  3: return 31;
        case  4: return 30;
        case  5: return 31;
        case  6: return 30;
        case  7: return 31;
        case  8: return 31;
        case  9: return 30;
        case 10: return 31;
        case 11: return 30;
        case 12: return 31;
        default: throw runtime_error("cannot compute number of days in month " + std::to_string(month) + " (needs to be between 1 and 12)");
    }
}

int Time::days_in_year(int year)
{
    if (year % 400 == 0 || (year % 4 == 0 && ! (year % 100 == 0)))
        return 366;
    return 365;
}

bool Time::range_overlaps(
        const Time* ts1, const Time* te1,
        const Time* ts2, const Time* te2)
{
    // If any of the intervals are open at both ends, they obviously overlap
    if (!ts1 && !te1) return true;
    if (!ts2 && !te2) return true;

    if (!ts1) return !ts2 || ts2->compare(*te1) < 0;
    if (!te1) return !te2 || te2->compare(*ts1) > 0;

    if (!ts2) return te2->compare(*ts1) > 0;
    if (!te2) return ts2->compare(*te1) < 0;

    return te1->compare(*ts2) > 0 && ts1->compare(*te2) < 0;
}

static long long int seconds_from(int year, const Time& t)
{
    // Duration since the beginning of the month
    long long int res =
          t.se
        + t.mi * 60
        + t.ho * 3600
        + (t.da - 1) * 3600 * 24;

    // Add duration of months since the beginning of the year
    for (int i = 1; i < t.mo; ++i)
        res += Time::days_in_month(t.ye, i) * 3600 * 24;

    // Add duration of past years
    for (int i = year; i < t.ye; ++i)
        res += Time::days_in_year(i) * 3600 * 24;

    return res;
}

long long int Time::duration(const Time& begin, const Time& until)
{
    // Find the smaller year, to use as a reference for secondsfrom
    int y = min(begin.ye, until.ye);
    return seconds_from(y, until) - seconds_from(y, begin);
}

long long int Time::duration(const core::Interval& interval)
{
    return duration(interval.begin, interval.end);
}

std::vector<Time> Time::generate(const Time& begin, const Time& end, int step)
{
    vector<Time> res;
    for (Time cur = begin; cur < end; )
    {
        res.push_back(cur);
        cur.se += step;
        cur.normalise();
    }
    return res;
}


/*
 * Interval
 */

Interval::Interval(const Time& begin, const Time& end)
    : begin(begin), end(end)
{
}

bool Interval::is_unbounded() const
{
    return !begin.is_set() && !end.is_set();
}

bool Interval::contains(const Time& time) const
{
    if (begin.is_set() && time < begin) return false;
    if (end.is_set() && time >= end) return false;
    return true;
}

bool Interval::contains(const Interval& interval) const
{
    if (!begin.is_set())
    {
        if (!end.is_set())
            return true;
        else
        {
            if (interval.begin.is_set() && interval.begin >= end) return false;
            return interval.end.is_set() && interval.end <= end;
        }
    }

    // begin is set

    if (!end.is_set())
    {
        if (interval.end.is_set() && interval.end <= begin) return false;
        return interval.begin.is_set() && interval.begin >= begin;
    }

    // begin and end are set

    if (!interval.begin.is_set()) return false;
    if (!interval.end.is_set()) return false;

    return interval.begin >= begin && interval.begin < end &&
           interval.end > begin && interval.end <= end;
}

bool Interval::intersects(const Interval& interval) const
{
    if (!begin.is_set())
    {
        if (!end.is_set())
            return true;
        else
            return !interval.begin.is_set() || interval.begin < end;
    }

    // begin is set

    if (!end.is_set())
        return !interval.end.is_set() || interval.end > begin;

    // begin and end are set

    if (!interval.begin.is_set())
        return !interval.end.is_set() || interval.end > begin;

    if (!interval.end.is_set())
        return interval.begin < end;

    return !(interval.end <= begin) && !(interval.begin >= end);
}

bool Interval::spans_one_whole_month() const
{
    if (!begin.is_set() || !end.is_set())
        return true;

    if (begin.is_start_of_month())
        return end >= begin.start_of_next_month();
    else
        return end >= begin.start_of_next_month().start_of_next_month();
}

void Interval::iter_months(std::function<bool(const Interval&)> f) const
{
    core::Time pos = begin;

    while (true)
    {
        core::Time next = pos.start_of_next_month();
        if (next >= end)
        {
            f(Interval(pos, end));
            break;
        } else {
            f(Interval(pos, next));
            pos = next;
        }
    }
}

bool Interval::intersect(const Interval& other)
{
    // Check for disjoint sets
    if (begin.is_set() && other.end.is_set() && other.end <= begin)
        return false;
    if (end.is_set() && other.begin.is_set() && end <= other.begin)
        return false;

    if (!other.begin.is_set())
    {
        // other is open ended at the beginning
        if (!other.end.is_set())
        {
            // other is open ended at both ends, we stay unchanged
            return true;
        } else {
            // other has an endpoint, and we know we are not disjoint, so it
            // ends after we begin
            if (!end.is_set() || end > other.end)
                end = other.end;
            return true;
        }
    }

    if (!other.end.is_set())
    {
        // other is open ended at the end /only/, and we know we are not
        // disjoint, so it begins before we end
        if (!begin.is_set() || begin < other.begin)
            begin = other.begin;
        return true;
    }

    // other is not open ended, and the two sets are not disjoint

    if (!end.is_set() || end > other.end)
        end = other.end;
    if (!begin.is_set() || begin < other.begin)
        begin = other.begin;
    return true;
}

void Interval::extend(const Interval& other)
{
    if (begin.is_set())
        if (!other.begin.is_set() || other.begin < begin)
            begin = other.begin;

    if (end.is_set())
        if (!other.end.is_set() || other.end > end)
            end = other.end;
}

std::string Interval::to_string() const
{
    std::string res;
    if (begin.is_set())
        res = begin.to_iso8601();
    res += "…";
    if (end.is_set())
        res += end.to_iso8601();
    return res;
}

}
}
