/*
 * types/time - Time
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

Time::Time()
{
	memset(vals, 0, 6*sizeof(int));
}
Time::Time(int ye, int mo, int da, int ho, int mi, int se)
{
	vals[0] = ye;
	vals[1] = mo;
	vals[2] = da;
	vals[3] = ho;
	vals[4] = mi;
	vals[5] = se;
}
Time::Time(const int (&vals)[6])
{
	memcpy(this->vals, vals, 6*sizeof(const int));
}
Time::Time(const Time& t)
{
	memcpy(this->vals, t.vals, 6*sizeof(const int));
}

Time& Time::operator=(const Time& t)
{
	memcpy(this->vals, t.vals, 6*sizeof(const int));
	return *this;
}

bool Time::isNow() const
{
	for (unsigned i = 0; i < 6; ++i)
		if (vals[i]) return false;
	return true;
}

int Time::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Time* v = dynamic_cast<const Time*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Time, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Time::compare(const Time& o) const
{
	// "Now" times sort bigger than everything else, so if at least one of the
	// dates is 'now', reverse the sorting
	if (vals[0] == 0 || o.vals[0] == 0)
		return o.vals[0] - vals[0];

	for (unsigned int i = 0; i < 6; ++i)
		if (int res = vals[i] - o.vals[i]) return res;

	return 0;
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

types::Code Time::serialisationCode() const { return CODE; }
size_t Time::serialisationSizeLength() const { return SERSIZELEN; }
std::string Time::tag() const { return TAG; }

Item<Time> Time::decode(const unsigned char* buf, size_t len)
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

Item<Time> Time::decodeString(const std::string& val)
{
	return Time::createFromISO8601(val);
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
const char* Time::lua_type_name() const { return "arki.types.time"; }

bool Time::operator==(const Type& o) const
{
	const Time* v = dynamic_cast<const Time*>(&o);
	if (!v) return false;
	return operator==(*v);
}

bool Time::operator==(const Time& t) const
{
	return memcmp(vals, t.vals, 6*sizeof(int)) == 0;
}

#ifdef HAVE_LUA
int Time::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Time reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const Time& v = **(const Time**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "0" || name == "year")
	{
		lua_pushnumber(L, v.vals[0]);
		return 1;
	}
	else if (name == "1" || name == "month")
	{
		lua_pushnumber(L, v.vals[1]);
		return 1;
	}
	else if (name == "2" || name == "day")
	{
		lua_pushnumber(L, v.vals[2]);
		return 1;
	}
	else if (name == "3" || name == "hour")
	{
		lua_pushnumber(L, v.vals[3]);
		return 1;
	}
	else if (name == "4" || name == "minute")
	{
		lua_pushnumber(L, v.vals[4]);
		return 1;
	}
	else if (name == "5" || name == "second")
	{
		lua_pushnumber(L, v.vals[5]);
		return 1;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_time(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Time::lua_lookup, 2);
	return 1;
}
void Time::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Time** s = (const Time**)lua_newuserdata(L, sizeof(const Time*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_time);
		lua_settable(L, -3);
		/* set the __tostring metamethod */
		lua_pushstring(L, "__tostring");
		lua_pushcfunction(L, utils::lua::tostring_arkitype<Time>);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

Item<Time> Time::create()
{
	return new Time;
}

Item<Time> Time::create(int ye, int mo, int da, int ho, int mi, int se)
{
	return new Time(ye, mo, da, ho, mi, se);
}

Item<Time> Time::create(const int (&vals)[6])
{
	return new Time(vals);
}

Item<Time> Time::create(struct tm& t)
{
	Time* res;
	Item<Time> itemres = res = new Time;
	res->vals[0] = t.tm_year + 1900;
	res->vals[1] = t.tm_mon + 1;
	res->vals[2] = t.tm_mday;
	res->vals[3] = t.tm_hour;
	res->vals[4] = t.tm_min;
	res->vals[5] = t.tm_sec;
	return itemres;
}

Item<Time> Time::createFromISO8601(const std::string& str)
{
	Time* res;
	Item<Time> itemres = res = new Time;
	int* v = res->vals;
	int count = sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
	sscanf(str.c_str(), "%d-%d-%dT%d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
	if (count == 0)
		throw wibble::exception::Consistency(
			"Invalid datetime specification: '"+str+"'",
			"Parsing ISO-8601 string");
	return itemres;
}

Item<Time> Time::createFromSQL(const std::string& str)
{
	Time* res;
	Item<Time> itemres = res = new Time;
	int* v = res->vals;
	int count = sscanf(str.c_str(), "%d-%d-%d %d:%d:%d", &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]);
	if (count == 0)
		throw wibble::exception::Consistency(
			"Invalid datetime specification: '"+str+"'",
			"Parsing SQL string");
	return itemres;
}

Item<Time> Time::createNow()
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

Item<Time> Time::createDifference(const Item<Time>& a, const Item<Time>& b)
{
	int res[6];
	for (int i = 0; i < 6; ++i)
		res[i] = a->vals[i];

	// Normalise days and months
	--res[1];
	--res[2];

	// Seconds
	res[5] -= b->vals[5];
	while (res[5] < 0)
	{
		res[5] += 60;
		--res[4];
	}
	// Minutes
	res[4] -= b->vals[4];
	while (res[4] < 0)
	{
		res[4] += 60;
		--res[3];
	}
	// Hours
	res[3] -= b->vals[3];
	while (res[3] < 0)
	{
		res[3] += 24;
		--res[2];
	}
	// Days
	res[2] -= b->vals[2] - 1;
	while (res[2] < 0)
	{
		--res[1];
		while (res[1] < 0)
		{
			--res[0];
			res[1] += 12;
		}
		res[2] += _daysinmonth(res[1]+1, res[0]);
	}
	// Months
	res[1] -= b->vals[1] - 1;
	while (res[1] < 0)
	{
		res[1] += 12;
		--res[0];
	}
	// Years
	res[0] -= b->vals[0];
	if (res[0] < 0)
		return Time::create();
	else
		return Time::create(res[0], res[1], res[2], res[3], res[4], res[5]);
}

std::vector< Item<Time> > Time::generate(
		const types::Time& begin, const types::Time& end, int step)
{
	vector< Item<Time> > res;
	for (Time cur = begin; cur < end; )
	{
		res.push_back(Time::create(cur.vals));
		cur.vals[5] += step;
		grcal::date::normalise(cur.vals);
	}
	return res;
}


namespace time {

static MetadataType timeType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Time::decode),
	(MetadataType::string_decoder)(&Time::decodeString));

}
}
}
// vim:set ts=4 sw=4:
