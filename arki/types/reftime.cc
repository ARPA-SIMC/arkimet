/*
 * types/reftime - Vertical reftime or layer
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
#include <arki/types/reftime.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include "config.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_REFTIME
#define TAG "reftime"
#define SERSIZELEN 1

using namespace std;
using namespace wibble;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki {
namespace types {

// Style constants
const unsigned char Reftime::POSITION;
const unsigned char Reftime::PERIOD;

Reftime::Style Reftime::parseStyle(const std::string& str)
{
	if (str == "POSITION") return POSITION;
	if (str == "GRIB1") return PERIOD;
	throw wibble::exception::Consistency("parsing Reftime style", "cannot parse Reftime style '"+str+"': only POSITION and PERIOD are supported");
}

std::string Reftime::formatStyle(Reftime::Style s)
{
	switch (s)
	{
		case Reftime::POSITION: return "POSITION";
		case Reftime::PERIOD: return "PERIOD";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

int Reftime::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Reftime* v = dynamic_cast<const Reftime*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Reftime, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Reftime::compare(const Reftime& o) const
{
	return style() - o.style();
}

types::Code Reftime::serialisationCode() const { return CODE; }
size_t Reftime::serialisationSizeLength() const { return SERSIZELEN; }
std::string Reftime::tag() const { return TAG; }
types::Code Reftime::typecode() { return CODE; }

void Reftime::encodeWithoutEnvelope(Encoder& enc) const
{
	enc.addUInt(style(), 1);
}

Item<Reftime> Reftime::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	ensureSize(len, 1, "Reftime");
	Style s = (Style)decodeUInt(buf, 1);
	switch (s)
	{
		case POSITION:
			ensureSize(len, 6, "Reftime");
			return reftime::Position::create(Time::decode(buf+1, 5));
		case PERIOD:
			ensureSize(len, 11, "Reftime");
			return reftime::Period::create(Time::decode(buf+1, 5), Time::decode(buf+6, 5));
		default:
			throw wibble::exception::Consistency("parsing reference time", "style " + str::fmt(s) + "but we can only decode POSITION and PERIOD");
	}
}


Item<Reftime> Reftime::decodeString(const std::string& val)
{
	size_t pos = val.find(" to ");
	if (pos == string::npos)
		return reftime::Position::create(Time::decodeString(val));

	return reftime::Period::create(
			Time::decodeString(val.substr(0, pos)),
			Time::decodeString(val.substr(pos + 4)));
}

#ifdef HAVE_LUA
int Reftime::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Reftime reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const Reftime& v = **(const Reftime**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "style")
	{
		string s = Reftime::formatStyle(v.style());
		lua_pushlstring(L, s.data(), s.size());
		return 1;
	}
	else if (name == "position" && v.style() == Reftime::POSITION)
	{
		const reftime::Position* v1 = v.upcast<reftime::Position>();
		v1->time->lua_push(L);
		return 1;
	}
	else if (name == "period" && v.style() == Reftime::PERIOD)
	{
		const reftime::Period* v1 = v.upcast<reftime::Period>();
		v1->begin->lua_push(L);
		v1->end->lua_push(L);
		return 2;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_reftime(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Reftime::lua_lookup, 2);
	return 1;
}
void Reftime::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Reftime** s = (const Reftime**)lua_newuserdata(L, sizeof(const Reftime*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_reftime);
		lua_settable(L, -3);
		/* set the __tostring metamethod */
		lua_pushstring(L, "__tostring");
		lua_pushcfunction(L, utils::lua::tostring_arkitype<Reftime>);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

namespace reftime {

Position::Position(const Item<types::Time>& time) : time(time) {}

Reftime::Style Position::style() const { return Reftime::POSITION; }

void Position::encodeWithoutEnvelope(Encoder& enc) const
{
	Reftime::encodeWithoutEnvelope(enc);
	time->encodeWithoutEnvelope(enc);
}

std::ostream& Position::writeToOstream(std::ostream& o) const
{
	return time->writeToOstream(o);
}

std::string Position::exactQuery() const
{
	return "=" + time->toISO8601();
}

const char* Position::lua_type_name() const { return "arki.types.reftime.position"; }


int Position::compare(const Reftime& o) const
{
	int res = Reftime::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Position* v = dynamic_cast<const Position*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Position Reftime, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int Position::compare(const Position& o) const
{
	return time->compare(*o.time);
}

bool Position::operator==(const Type& o) const
{
	const Position* v = dynamic_cast<const Position*>(&o);
	if (!v) return false;
	return time == v->time;
}

Item<Position> Position::create(const Item<Time>& position)
{
	return new Position(position);
}

Period::Period(const Item<types::Time>& begin, const Item<types::Time>& end)
	: begin(begin), end(end) {}

Reftime::Style Period::style() const { return Reftime::PERIOD; }

bool Period::setEndtimeToNow(int secondsAgo)
{
	// Compute a time vector with the treshold time
	time_t treshold = time(NULL) - secondsAgo;
	struct tm t;
	gmtime_r(&treshold, &t);
	Item<types::Time> now(Time::create(t));
	if (end > now)
	{
		end = Time::createNow();
		return true;
	}
	return false;
}

void Period::encodeWithoutEnvelope(Encoder& enc) const
{
	Reftime::encodeWithoutEnvelope(enc);
	begin->encodeWithoutEnvelope(enc);
	end->encodeWithoutEnvelope(enc);
}

std::ostream& Period::writeToOstream(std::ostream& o) const
{
	begin->writeToOstream(o);
	o << " to ";
	return end->writeToOstream(o);
}

std::string Period::exactQuery() const
{
	return "=" + begin->toISO8601();
}

const char* Period::lua_type_name() const { return "arki.types.reftime.period"; }

int Period::compare(const Reftime& o) const
{
	int res = Reftime::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Period* v = dynamic_cast<const Period*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Period Reftime, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int Period::compare(const Period& o) const
{
	if (int res = begin->compare(*o.begin)) return res;
	return end->compare(*o.end);
}

bool Period::operator==(const Type& o) const
{
	const Period* v = dynamic_cast<const Period*>(&o);
	if (!v) return false;
	return begin == v->begin && end == v->end;
}

Item<Period> Period::create(const Item<Time>& begin, const Item<Time>& end)
{
	return new Period(begin, end);
}

void Collector::clear()
{
	begin.clear();
	end.clear();
}

int Collector::compare(const Collector& c) const
{
	if (int res = begin.compare(c.begin)) return res;
	return end.compare(c.end);
}
bool Collector::operator==(const Collector& c) const
{
	return begin == c.begin && end == c.end;
}
bool Collector::operator<(const Collector& c) const
{
	if (int res = begin.compare(c.begin)) return res < 0;
	return end.compare(c.end) < 0;
}

void Collector::mergeTime(const Item<types::Time>& t)
{
	// If we're empty, just copy
	if (!begin.defined())
	{
		begin = t;
		return;
	}

	// If if it's the same as 'begin', we're done
	// (this catches the case where end is not defined, and we get the same
	// as begin)
	if (begin == t)
		return;

	// Now we know we have to be an interval, so if we are not an interval
	// already, we become it
	if (!end.defined())
		end = begin;

	if (t < begin)
		begin = t;
	else if (end < t)
		end = t;
}

void Collector::mergeTime(const Item<types::Time>& tbegin, const Item<types::Time>& tend)
{
	// If we're empty, just copy
	if (!begin.defined())
	{
		begin = tbegin;
		end = tend;
		return;
	}

	// We have to be an interval, so if we are not an interval already, we
	// become it
	if (!end.defined())
		end = begin;

	if (tbegin < begin)
		begin = tbegin;
	if (end < tend)
		end = tend;
}

void Collector::merge(const Reftime* rt)
{
	if (const Position* po = dynamic_cast<const Position*>(rt))
		mergeTime(po->time);
	else if (const Period* pe = dynamic_cast<const Period*>(rt))
		mergeTime(pe->begin, pe->end);
	else
		throw wibble::exception::Consistency(
			"merging reference times",
			string("unsupported reference time to merge: ") + typeid(rt).name());
}

void Collector::merge(const Collector& c)
{
	if (!c.begin.defined())
		return;
	else if (!c.end.defined())
		mergeTime(c.begin);
	else
		mergeTime(c.begin, c.end);
}

Item<Reftime> Collector::makeReftime() const
{
	if (!begin.defined())
		throw wibble::exception::Consistency(
			"merging reference times",
			"no data has been given to merge");
		
	if (!end.defined() || begin == end)
		return Position::create(begin);

	return Period::create(begin, end);
}

static MetadataType reftimeType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Reftime::decode),
	(MetadataType::string_decoder)(&Reftime::decodeString));

}

}
}
// vim:set ts=4 sw=4:
