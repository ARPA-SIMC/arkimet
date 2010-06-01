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
#include <cstring>

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
	
const char* traits<Reftime>::type_tag = TAG;
const types::Code traits<Reftime>::type_code = CODE;
const size_t traits<Reftime>::type_sersize_bytes = SERSIZELEN;
const char* traits<Reftime>::type_lua_tag = LUATAG_TYPES ".reftime";

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

static int arkilua_new_position(lua_State* L)
{
	Item<types::Time> time = types::Type::lua_check<Time>(L, 1);
	reftime::Position::create(time)->lua_push(L);
	return 1;
}

static int arkilua_new_period(lua_State* L)
{
	Item<types::Time> beg = types::Type::lua_check<Time>(L, 1);
	Item<types::Time> end = types::Type::lua_check<Time>(L, 2);
	reftime::Period::create(beg, end)->lua_push(L);
	return 1;
}

void Reftime::lua_loadlib(lua_State* L)
{
	static const struct luaL_reg lib [] = {
		{ "position", arkilua_new_position },
		{ "period", arkilua_new_period },
		{ NULL, NULL }
	};
	luaL_openlib(L, "arki_reftime", lib, 0);
	lua_pop(L, 1);
}

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

bool Position::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "time")
		time->lua_push(L);
	else
		return Reftime::lua_lookup(L, name);
	return true;
}

int Position::compare_local(const Reftime& o) const
{
	// We should be the same kind, so upcast
	const Position* v = dynamic_cast<const Position*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Position Reftime, but is a ") + typeid(&o).name() + " instead");

	return time->compare(*(v->time));
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

bool Period::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "begin")
		begin->lua_push(L);
	else if (name == "end")
		end->lua_push(L);
	else
		return Reftime::lua_lookup(L, name);
	return true;
}

int Period::compare_local(const Reftime& o) const
{
	// We should be the same kind, so upcast
	const Period* v = dynamic_cast<const Period*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Period Reftime, but is a ") + typeid(&o).name() + " instead");

	if (int res = begin->compare(*(v->begin))) return res;
	return end->compare(*(v->end));
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

static MetadataType reftimeType = MetadataType::create<Reftime>();

}

}
}

#include <arki/types.tcc>

// vim:set ts=4 sw=4:
