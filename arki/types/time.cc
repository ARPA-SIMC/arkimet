/*
 * types/time - Time
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/exception.h>
#include <wibble/string.h>
#include <arki/types/time.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include <arki/formatter.h>
#include <wibble/grcal/grcal.h>
#include "config.h"
#include <sstream>
#include <cmath>
#include <cstring>
#include <ctime>
#include <cstdio>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_TIME
#define TAG "time"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;
using namespace wibble;

namespace arki {
namespace types {

const char* traits<Time>::type_tag = TAG;
const types::Code traits<Time>::type_code = CODE;
const size_t traits<Time>::type_sersize_bytes = SERSIZELEN;
const char* traits<Time>::type_lua_tag = LUATAG_TYPES ".time";

Time::Time() { setInvalid(); }
Time::Time(int ye, int mo, int da, int ho, int mi, int se) { set(ye, mo, da, ho, mi, se); }
Time::Time(const int (&vals)[6]) { set(vals); }
Time::Time(const Time& t) { set(t.vals); }
Time::Time(struct tm& t) { set(t); }

Time& Time::operator=(const Time& t)
{
	memcpy(this->vals, t.vals, 6*sizeof(const int));
	return *this;
}

bool Time::isValid() const
{
	for (unsigned i = 0; i < 6; ++i)
		if (vals[i]) return false;
	return true;
}

int Time::compare(const Type& o) const
{
    if (int res = Type::compare(o)) return res;

	// We should be the same kind, so upcast
	const Time* v = dynamic_cast<const Time*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			str::fmtf("second element claims to be Type, but is `%s' instead",
				typeid(&o).name()));

	// "Now" times sort bigger than everything else, so if at least one of the
	// dates is 'now', reverse the sorting
	if (vals[0] == 0 || v->vals[0] == 0)
		return v->vals[0] - vals[0];

	for (unsigned int i = 0; i < 6; ++i)
		if (int res = vals[i] - v->vals[i]) return res;

	return 0;
}

auto_ptr<Time> Time::start_of_month() const
{
    auto_ptr<Time> t(new Time);
    t->vals[0] = vals[0];
    t->vals[1] = vals[1];
    t->vals[2] = 1;
    t->vals[3] = t->vals[4] = t->vals[5] = 0;
    return t;
}
auto_ptr<Time> Time::start_of_next_month() const
{
    auto_ptr<Time> t(new Time);
    t->vals[0] = vals[0] + vals[1] / 12;
    t->vals[1] = (vals[1] % 12) + 1;
    t->vals[2] = 1;
    t->vals[3] = t->vals[4] = t->vals[5] = 0;
    return t;
}
auto_ptr<Time> Time::end_of_month() const
{
    auto_ptr<Time> t(new Time);
    t->vals[0] = vals[0];
    t->vals[1] = vals[1];
    t->vals[2] = grcal::date::daysinmonth(vals[0], vals[1]);
    t->vals[3] = 23;
    t->vals[4] = 59;
    t->vals[5] = 59;
    return t;
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

auto_ptr<Time> Time::decode(const unsigned char* buf, size_t len)
{
    using namespace utils::codec;
    ensureSize(len, 5, "Time");
    uint32_t a = decodeUInt(buf, 4);
    uint32_t b = decodeUInt(buf + 4, 1);
    return Time::create(
        a >> 18,
        (a >> 14) & 0xf,
        (a >> 9) & 0x1f,
        (a >> 4) & 0x1f,
        ((a & 0xf) << 2) | ((b >> 6) & 0x3),
        b & 0x3f);
}

auto_ptr<Time> Time::decodeString(const std::string& val)
{
    return Time::createFromISO8601(val);
}

auto_ptr<Time> Time::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    return decodeList(val["v"].want_list("decoding Time value"));
}

auto_ptr<Time> Time::decodeList(const emitter::memory::List& val)
{
    using namespace emitter::memory;

    if (val.size() < 6)
        throw wibble::exception::Consistency(
                "decoding item",
                str::fmtf("list has %zd elements instead of 6", val.size()));

    auto_ptr<Time> res(new Time);
    for (unsigned i = 0; i < 6; ++i)
        res->vals[i] = val[i].want_int("decoding component of time value");
    return res;
}

void Time::encodeWithoutEnvelope(Encoder& enc) const
{
	uint32_t a = ((vals[0] & 0x3fff) << 18)
	           | ((vals[1] & 0xf)    << 14)
			   | ((vals[2] & 0x1f)   << 9)
			   | ((vals[3] & 0x1f)   << 4)
			   | ((vals[4] >> 2) & 0xf);
	uint32_t b = ((vals[4] & 0x3) << 6)
	           | (vals[5] & 0x3f);
	enc.addUInt(a, 4);
	enc.addUInt(b, 1);
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
	Time::createNow()->lua_push(L);
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
	Time::createFromSQL(str)->lua_push(L);
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

void Time::setInvalid()
{
    memset(vals, 0, 6*sizeof(int));
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

auto_ptr<Time> Time::createInvalid()
{
    return auto_ptr<Time>(new Time);
}

auto_ptr<Time> Time::create(int ye, int mo, int da, int ho, int mi, int se)
{
    return auto_ptr<Time>(new Time(ye, mo, da, ho, mi, se));
}

auto_ptr<Time> Time::create(const int (&vals)[6])
{
    return auto_ptr<Time>(new Time(vals));
}

auto_ptr<Time> Time::create(struct tm& t)
{
    auto_ptr<Time> res(new Time);
    res->vals[0] = t.tm_year + 1900;
    res->vals[1] = t.tm_mon + 1;
    res->vals[2] = t.tm_mday;
    res->vals[3] = t.tm_hour;
    res->vals[4] = t.tm_min;
    res->vals[5] = t.tm_sec;
    return res;
}

auto_ptr<Time> Time::createFromISO8601(const std::string& str)
{
    auto_ptr<Time> res(new Time);
	int* v = res->vals;
	int count = sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
	sscanf(str.c_str(), "%d-%d-%dT%d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
	if (count == 0)
		throw wibble::exception::Consistency(
			"Invalid datetime specification: '"+str+"'",
			"Parsing ISO-8601 string");
    return res;
}

auto_ptr<Time> Time::createFromSQL(const std::string& str)
{
    auto_ptr<Time> res(new Time);
	int* v = res->vals;
	int count = sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
	if (count == 0)
		throw wibble::exception::Consistency(
			"Invalid datetime specification: '"+str+"'",
			"Parsing SQL string");
    return res;
}

auto_ptr<Time> Time::createNow()
{
	time_t timet_now = time(0);
	struct tm now;
	gmtime_r(&timet_now, &now);
	return create(now);
}

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

bool Time::range_overlaps(
        const Time& ts1, const Time& te1,
        const Time& ts2, const Time& te2)
{
    // If any of the intervals are open at both ends, they obviously overlap
    if (!ts1.isValid() && !te1.isValid()) return true;
    if (!ts2.isValid() && !te2.isValid()) return true;

    if (!ts1.isValid()) return !ts2.isValid() || ts2.compare(te1) <= 0;
    if (!te1.isValid()) return !te2.isValid() || te2.compare(ts1) >= 0;

    if (!ts2.isValid()) return te2.compare(ts1) >= 0;
    if (!te2.isValid()) return ts2.compare(te1) <= 0;

    return !(te1.compare(ts2) < 0 || ts1.compare(te2) > 0);
}

std::vector<Time> Time::generate(const types::Time& begin, const types::Time& end, int step)
{
    vector<Time> res;
    for (Time cur = begin; cur < end; )
    {
        res.push_back(cur);
        cur.vals[5] += step;
        grcal::date::normalise(cur.vals);
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

// vim:set ts=4 sw=4:
