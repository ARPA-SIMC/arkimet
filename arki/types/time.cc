#include <arki/wibble/exception.h>
#include <arki/types/time.h>
#include <arki/types/utils.h>
#include <arki/binary.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include <arki/formatter.h>
#include <arki/wibble/grcal/grcal.h>
#include "config.h"
#include <sstream>
#include <cmath>
#include <cstring>
#include <ctime>
#include <cstdio>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE TYPE_TIME
#define TAG "time"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Time>::type_tag = TAG;
const types::Code traits<Time>::type_code = CODE;
const size_t traits<Time>::type_sersize_bytes = SERSIZELEN;
const char* traits<Time>::type_lua_tag = LUATAG_TYPES ".time";

Time::Time() {}
Time::Time(int ye, int mo, int da, int ho, int mi, int se) { set(ye, mo, da, ho, mi, se); }
Time::Time(const int (&vals)[6]) { set(vals); }
Time::Time(const Time& t) { set(t.vals); }
Time::Time(struct tm& t) { set(t); }

Time& Time::operator=(const Time& t)
{
	memcpy(this->vals, t.vals, 6*sizeof(const int));
	return *this;
}

int Time::compare_raw(const int (&other)[6]) const
{
    for (unsigned int i = 0; i < 6; ++i)
        if (int res = vals[i] - other[i]) return res;
    return 0;
}

int Time::compare(const Type& o) const
{
    if (int res = Type::compare(o)) return res;

    // We should be the same kind, so upcast
    const Time* v = dynamic_cast<const Time*>(&o);
    if (!v)
    {
        stringstream ss;
        ss << "cannot compare metadata types: second element claims to be Time, but is `" << typeid(&o).name() << "' instead",
           throw std::runtime_error(ss.str());
    }

    return compare_raw(v->vals);
}

Time Time::start_of_month() const
{
    return Time(vals[0], vals[1], 1, 0, 0, 0);
}
Time Time::start_of_next_month() const
{
    return Time(
            vals[0] + vals[1] / 12,
            (vals[1] % 12) + 1,
            1,
            0, 0, 0);
}
Time Time::end_of_month() const
{
    return Time(vals[0], vals[1], wibble::grcal::date::daysinmonth(vals[0], vals[1]), 23, 59, 59);
}

std::string Time::toISO8601(char sep) const
{
	char buf[25];
	snprintf(buf, 25, "%04d-%02d-%02d%c%02d:%02d:%02dZ",
			vals[0], vals[1], vals[2], sep, vals[3], vals[4], vals[5]);
	return buf;
}

std::string Time::toSQL() const
{
	char buf[25];
	snprintf(buf, 25, "%04d-%02d-%02d %02d:%02d:%02d",
			vals[0], vals[1], vals[2], vals[3], vals[4], vals[5]);
	return buf;
}

unique_ptr<Time> Time::decode(BinaryDecoder& dec)
{
    uint32_t a = dec.pop_uint(4, "first 32 bits of encoded time");
    uint32_t b = dec.pop_uint(1, "last 8 bits of encoded time");
    return Time::create(
        a >> 18,
        (a >> 14) & 0xf,
        (a >> 9) & 0x1f,
        (a >> 4) & 0x1f,
        ((a & 0xf) << 2) | ((b >> 6) & 0x3),
        b & 0x3f);
}

unique_ptr<Time> Time::decodeString(const std::string& val)
{
    return Time::createFromISO8601(val);
}

unique_ptr<Time> Time::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    return decodeList(val["v"].want_list("decoding Time value"));
}

unique_ptr<Time> Time::decodeList(const emitter::memory::List& val)
{
    using namespace emitter::memory;

    if (val.size() < 6)
    {
        stringstream ss;
        ss << "cannot decode item: list has " << val.size() << " elements instead of 6";
        throw std::runtime_error(ss.str());
    }

    unique_ptr<Time> res(new Time);
    for (unsigned i = 0; i < 6; ++i)
        res->vals[i] = val[i].want_int("decoding component of time value");
    return res;
}

void Time::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    uint32_t a = ((vals[0] & 0x3fff) << 18)
               | ((vals[1] & 0xf)    << 14)
               | ((vals[2] & 0x1f)   << 9)
               | ((vals[3] & 0x1f)   << 4)
               | ((vals[4] >> 2) & 0xf);
    uint32_t b = ((vals[4] & 0x3) << 6)
               | (vals[5] & 0x3f);
    enc.add_unsigned(a, 4);
    enc.add_unsigned(b, 1);
}

std::ostream& Time::writeToOstream(std::ostream& o) const
{
	return o << toISO8601();
}

void Time::serialiseLocal(Emitter& e, const Formatter* f) const
{
    e.add("v");
    serialiseList(e);
}

void Time::serialiseList(Emitter& e) const
{
    e.start_list();
    for (unsigned i = 0; i < 6; ++i)
        e.add(vals[i]);
    e.end_list();
}

bool Time::equals(const Type& t) const
{
    const Time* v = dynamic_cast<const Time*>(&t);
    if (!v) return false;
    return memcmp(vals, v->vals, 6*sizeof(int)) == 0;
}

#ifdef HAVE_LUA

static int arkilua_lookup(lua_State *L)
{
    const Time* item = dynamic_cast<const Time*>(Type::lua_check(L, 1, "arki.types.time"));
	if (lua_type(L, 2) == LUA_TNUMBER) 
	{
		// Lua array indices start at 1
		int idx = lua_tointeger(L, 2);
		if (idx >= 1 && idx <= 6)
			lua_pushnumber(L, item->vals[idx-1]);
		else
			lua_pushnil(L);
	} else {
		const char* name = lua_tostring(L, 2);
		luaL_argcheck(L, name != NULL, 2, "`string' expected");

		if (strcmp(name, "year") == 0)
			lua_pushnumber(L, item->vals[0]);
		else if (strcmp(name, "month") == 0)
			lua_pushnumber(L, item->vals[1]);
		else if (strcmp(name, "day") == 0)
			lua_pushnumber(L, item->vals[2]);
		else if (strcmp(name, "hour") == 0)
			lua_pushnumber(L, item->vals[3]);
		else if (strcmp(name, "minute") == 0)
			lua_pushnumber(L, item->vals[4]);
		else if (strcmp(name, "second") == 0)
			lua_pushnumber(L, item->vals[5]);
		else
			lua_pushnil(L);
	}

	return 1;
}

void Time::lua_register_methods(lua_State* L) const
{
	Type::lua_register_methods(L);

	static const struct luaL_Reg lib [] = {
		{ "__index", arkilua_lookup },
		{ NULL, NULL }
	};
    utils::lua::add_functions(L, lib);
}

static int arkilua_new_time(lua_State* L)
{
	int ye = luaL_checkint(L, 1);
	int mo = luaL_checkint(L, 2);
	int da = luaL_checkint(L, 3);
	int ho = luaL_checkint(L, 4);
	int mi = luaL_checkint(L, 5);
	int se = luaL_checkint(L, 6);
	Time::create(ye, mo, da, ho, mi, se)->lua_push(L);
	return 1;
}

static int arkilua_new_now(lua_State* L)
{
    Time::createNow().lua_push(L);
    return 1;
}

static int arkilua_new_iso8601(lua_State* L)
{
	const char* str = luaL_checkstring(L, 1);
	Time::createFromISO8601(str)->lua_push(L);
	return 1;
}

static int arkilua_new_sql(lua_State* L)
{
    const char* str = luaL_checkstring(L, 1);
    Time::create_from_SQL(str).lua_push(L);
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

Time* Time::clone() const
{
    Time* res = new Time;
    memcpy(res->vals, vals, 6*sizeof(const int));
    return res;
}

void Time::set(int ye, int mo, int da, int ho, int mi, int se)
{
    vals[0] = ye;
    vals[1] = mo;
    vals[2] = da;
    vals[3] = ho;
    vals[4] = mi;
    vals[5] = se;
}

void Time::set(const int (&vals)[6])
{
    memcpy(this->vals, vals, 6*sizeof(const int));
}

void Time::set(struct tm& t)
{
    vals[0] = t.tm_year + 1900;
    vals[1] = t.tm_mon + 1;
    vals[2] = t.tm_mday;
    vals[3] = t.tm_hour;
    vals[4] = t.tm_min;
    vals[5] = t.tm_sec;
}

void Time::setFromISO8601(const std::string& str)
{
    int* v = vals;
    int count = sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
    sscanf(str.c_str(), "%d-%d-%dT%d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
    if (count == 0)
        throw wibble::exception::Consistency(
                "Invalid datetime specification: '"+str+"'",
                "Parsing ISO-8601 string");
}

void Time::setFromSQL(const std::string& str)
{
    int* v = vals;
    int count = sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
    if (count == 0)
        throw wibble::exception::Consistency(
                "Invalid datetime specification: '"+str+"'",
                "Parsing SQL string");
}

void Time::setNow()
{
    time_t timet_now = time(0);
    struct tm now;
    gmtime_r(&timet_now, &now);
    set(now);
}

unique_ptr<Time> Time::create(int ye, int mo, int da, int ho, int mi, int se)
{
    return unique_ptr<Time>(new Time(ye, mo, da, ho, mi, se));
}

unique_ptr<Time> Time::create(const int (&vals)[6])
{
    return unique_ptr<Time>(new Time(vals));
}

unique_ptr<Time> Time::create(struct tm& t)
{
    unique_ptr<Time> res(new Time);
    res->vals[0] = t.tm_year + 1900;
    res->vals[1] = t.tm_mon + 1;
    res->vals[2] = t.tm_mday;
    res->vals[3] = t.tm_hour;
    res->vals[4] = t.tm_min;
    res->vals[5] = t.tm_sec;
    return res;
}

unique_ptr<Time> Time::createFromISO8601(const std::string& str)
{
    unique_ptr<Time> res(new Time);
    int* v = res->vals;
    int count = sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
    if (count < 6)
        count = sscanf(str.c_str(), "%d-%d-%dT%d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
    if (count < 6)
        throw wibble::exception::Consistency(
                "Invalid datetime specification: '"+str+"'",
                "Parsing ISO-8601 string");
    return res;
}

Time Time::create_from_SQL(const std::string& str)
{
    Time res;
    int* v = res.vals;
    int count = sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
    if (count == 0)
        throw wibble::exception::Consistency(
                "Invalid datetime specification: '"+str+"'",
                "Parsing SQL string");
    return res;
}

Time Time::createNow()
{
    time_t timet_now = time(0);
    struct tm now;
    gmtime_r(&timet_now, &now);
    return Time(now);
}

#if 0
static int _daysinmonth(int month, int year)
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
		default: {
			stringstream str;
			str << "Month '" << month << "' is not between 1 and 12";
			throw wibble::exception::Consistency("computing number of days in month", str.str());
		}
	}
}
#endif

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

std::vector<Time> Time::generate(const types::Time& begin, const types::Time& end, int step)
{
    vector<Time> res;
    for (Time cur = begin; cur < end; )
    {
        res.push_back(cur);
        cur.vals[5] += step;
        wibble::grcal::date::normalise(cur.vals);
    }
    return res;
}


void Time::init()
{
    MetadataType::register_type<Time>();
}

}
}
#include <arki/types.tcc>
