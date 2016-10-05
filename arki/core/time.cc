#include "time.h"
#include "arki/exceptions.h"
#include "arki/binary.h"
#include "arki/emitter.h"
#include "arki/emitter/memory.h"
#include "arki/utils/lua.h"
#include "config.h"
#include <sstream>
#include <cmath>
#include <cstring>
#include <ctime>
#include <cstdio>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace core {

TimeBase::TimeBase(struct tm& t) { set_tm(t); }

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
    return Time(ye, mo, 1, 0, 0, 0);
}

Time Time::start_of_next_month() const
{
    return Time(ye + mo / 12, (mo % 12) + 1, 1, 0, 0, 0);
}

Time Time::end_of_month() const
{
    return Time(ye, mo, days_in_month(ye, mo), 23, 59, 59);
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
    //cerr << "TOZERO " << tostringall(vals) << endl;

    // Normalise seconds
    normalN(se, mi, 60);
    //cerr << "ADJSEC " << tostringall(vals) << endl;

    // Normalise minutes
    normalN(mi, ho, 60);
    //cerr << "ADJMIN " << tostringall(vals) << endl;

    // Normalise hours
    normalN(ho, da, 24);
    //cerr << "ADJHOUR " << tostringall(vals) << endl;

    // Normalise days
    while (da < 0)
    {
        --mo;
        normalN(mo, ye, 12);
        da += days_in_month(ye, mo + 1);
    }
    //cerr << "ADJDAY1 " << tostringall(vals) << endl;
    while (true)
    {
        normalN(mo, ye, 12);
        int dim = days_in_month(ye, mo + 1);
        if (da < dim) break;
        da -= dim;
        ++mo;
    }
    //cerr << "ADJDAY2 " << tostringall(vals) << endl;

    // Normalise months
    normalN(mo, ye, 12);
    //cerr << "ADJMONYEAR " << tostringall(vals) << endl;

    ++mo;
    ++da;
    //cerr << "FROMZERO " << tostringall(vals) << endl;
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

Time Time::decode(BinaryDecoder& dec)
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

Time Time::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    return decodeList(val["v"].want_list("decoding Time value"));
}

Time Time::decodeList(const emitter::memory::List& val)
{
    using namespace emitter::memory;

    if (val.size() < 6)
    {
        stringstream ss;
        ss << "cannot decode item: list has " << val.size() << " elements instead of 6";
        throw std::runtime_error(ss.str());
    }

    Time res;
    res.ye = val[0].want_int("decoding year component of time value");
    res.mo = val[1].want_int("decoding month component of time value");
    res.da = val[2].want_int("decoding day component of time value");
    res.ho = val[3].want_int("decoding hour component of time value");
    res.mi = val[4].want_int("decoding minute component of time value");
    res.se = val[5].want_int("decoding second component of time value");
    return res;
}

void Time::encodeWithoutEnvelope(BinaryEncoder& enc) const
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

void Time::serialise(Emitter& e) const
{
    e.start_mapping();
    e.add("t", "time");
    serialiseLocal(e);
    e.end_mapping();
}

void Time::serialiseLocal(Emitter& e) const
{
    e.add("v");
    serialiseList(e);
}

void Time::serialiseList(Emitter& e) const
{
    e.start_list();
    e.add(ye);
    e.add(mo);
    e.add(da);
    e.add(ho);
    e.add(mi);
    e.add(se);
    e.end_list();
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

    if (!ts1) return !ts2 || ts2->compare(*te1) <= 0;
    if (!te1) return !te2 || te2->compare(*ts1) >= 0;

    if (!ts2) return te2->compare(*ts1) >= 0;
    if (!te2) return ts2->compare(*te1) <= 0;

    return !(te1->compare(*ts2) < 0 || ts1->compare(*te2) > 0);
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


#ifdef HAVE_LUA
static Time* lua_time_check(lua_State* L, int idx)
{
    luaL_checktype(L, idx, LUA_TUSERDATA);
    return (Time*)luaL_checkudata(L, idx, "arki_time");
}

static int lua_time_index(lua_State *L)
{
    Time* item = lua_time_check(L, 1);
    if (lua_type(L, 2) == LUA_TNUMBER) 
    {
        // Lua array indices start at 1
        int idx = lua_tointeger(L, 2);
        switch (idx)
        {
            case 1: lua_pushnumber(L, item->ye); break;
            case 2: lua_pushnumber(L, item->mo); break;
            case 3: lua_pushnumber(L, item->da); break;
            case 4: lua_pushnumber(L, item->ho); break;
            case 5: lua_pushnumber(L, item->mi); break;
            case 6: lua_pushnumber(L, item->se); break;
            default: lua_pushnil(L); break;
        }
    } else {
        const char* name = lua_tostring(L, 2);
        luaL_argcheck(L, name != NULL, 2, "`string' expected");

        if (strcmp(name, "year") == 0)
            lua_pushnumber(L, item->ye);
        else if (strcmp(name, "month") == 0)
            lua_pushnumber(L, item->mo);
        else if (strcmp(name, "day") == 0)
            lua_pushnumber(L, item->da);
        else if (strcmp(name, "hour") == 0)
            lua_pushnumber(L, item->ho);
        else if (strcmp(name, "minute") == 0)
            lua_pushnumber(L, item->mi);
        else if (strcmp(name, "second") == 0)
            lua_pushnumber(L, item->se);
        else
            lua_pushnil(L);
    }
    return 1;
}

Time Time::lua_check(lua_State* L, int idx)
{
    return *lua_time_check(L, idx);
}

static int lua_time_tostring(lua_State* L)
{
    std::string s = lua_time_check(L, 1)->to_iso8601();
    lua_pushlstring(L, s.data(), s.size());
    return 1;
}

static int lua_time_le(lua_State* L)
{
    Time* time1 = lua_time_check(L, 1);
    Time* time2 = lua_time_check(L, 2);
    lua_pushboolean(L, *time1 <= *time2);
    return 1;
}

static int lua_time_lt(lua_State* L)
{
    Time* time1 = lua_time_check(L, 1);
    Time* time2 = lua_time_check(L, 2);
    lua_pushboolean(L, *time1 < *time2);
    return 1;
}

static int lua_time_eq(lua_State* L)
{
    Time* time1 = lua_time_check(L, 1);
    Time* time2 = lua_time_check(L, 2);
    lua_pushboolean(L, *time1 == *time2);
    return 1;
}

void Time::lua_push(lua_State* L) const
{
    // The userdata will be a Type*, holding a clone of this object
    Time* s = (Time*)lua_newuserdata(L, sizeof(Time));
    *s = *this;

    // Create the metatable for the time methods
    if (luaL_newmetatable(L, "arki_time"))
    {
        static const struct luaL_Reg lib [] = {
            { "__index", lua_time_index },
            { "__tostring", lua_time_tostring },
            { "__eq", lua_time_eq },
            { "__lt", lua_time_lt },
            { "__le", lua_time_le },
            { NULL, NULL }
        };
        utils::lua::add_functions(L, lib);
    }
    lua_setmetatable(L, -2);
}

static int arkilua_new_time(lua_State* L)
{
    int ye = luaL_checkint(L, 1);
    int mo = luaL_checkint(L, 2);
    int da = luaL_checkint(L, 3);
    int ho = luaL_checkint(L, 4);
    int mi = luaL_checkint(L, 5);
    int se = luaL_checkint(L, 6);
    Time(ye, mo, da, ho, mi, se).lua_push(L);
    return 1;
}

static int arkilua_new_now(lua_State* L)
{
    Time::create_now().lua_push(L);
    return 1;
}

static int arkilua_new_iso8601(lua_State* L)
{
    const char* str = luaL_checkstring(L, 1);
    Time::create_iso8601(str).lua_push(L);
    return 1;
}

static int arkilua_new_sql(lua_State* L)
{
    const char* str = luaL_checkstring(L, 1);
    Time::create_sql(str).lua_push(L);
    return 1;
}

void Time::lua_loadlib(lua_State* L)
{
    static const struct luaL_Reg lib [] = {
        { "time", arkilua_new_time },
        { "now", arkilua_new_now },
        { "iso8601", arkilua_new_iso8601 },
        { "sql", arkilua_new_sql },
        { NULL, NULL }
    };
    utils::lua::add_global_library(L, "arki_time", lib);

}
#endif

}
}
