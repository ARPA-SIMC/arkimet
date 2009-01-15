/*
 * types/run - Daily run identification for a periodic data source
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

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <arki/types/run.h>
#include <arki/types/utils.h>
#include <arki/utils.h>
#include <config.h>
#include <iomanip>
#include <sstream>

#ifdef HAVE_LUA
#include <arki/utils-lua.h>
#endif

#define CODE types::TYPE_RUN
#define TAG "run"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;
using namespace wibble;

namespace arki {
namespace types {

// Style constants
//const unsigned char Run::NONE;
const unsigned char Run::MINUTE;

Run::Style Run::parseStyle(const std::string& str)
{
	if (str == "MINUTE") return MINUTE;
	throw wibble::exception::Consistency("parsing Run style", "cannot parse Run style '"+str+"': only MINUTE is supported");
}

std::string Run::formatStyle(Run::Style s)
{
	switch (s)
	{
		//case Run::NONE: return "NONE";
		case Run::MINUTE: return "MINUTE";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

int Run::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Run* v = dynamic_cast<const Run*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be an Run, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Run::compare(const Run& o) const
{
	return style() - o.style();
}

types::Code Run::serialisationCode() const { return CODE; }
size_t Run::serialisationSizeLength() const { return SERSIZELEN; }
std::string Run::tag() const { return TAG; }

std::string Run::encodeWithoutEnvelope() const
{
	using namespace utils;
	return encodeUInt(style(), 1);
}

Item<Run> Run::decode(const unsigned char* buf, size_t len)
{
	using namespace utils;
	ensureSize(len, 1, "Run");
	Style s = (Style)decodeUInt(buf, 1);
	switch (s)
	{
		case MINUTE: {
			Item<run::Minute> res(new run::Minute);
			res->minute = decodeUInt(buf+1, 2);
			return res;
		}
		default:
			throw wibble::exception::Consistency("parsing Run", "style is " + formatStyle(s) + " but we can only decode MINUTE and BOX");
	}
}
    
Item<Run> Run::decodeString(const std::string& val)
{
	string inner;
	Run::Style style = outerParse<Run>(val, inner);
	switch (style)
	{
		case Run::MINUTE: {
			size_t sep = inner.find(':');
			int hour, minute;
			if (sep == string::npos)
			{
				// 12
				hour = strtoul(inner.c_str(), 0, 10);
				minute = 0;
			} else {
				// 12:00
				hour = strtoul(inner.substr(0, sep).c_str(), 0, 10);
				minute = strtoul(inner.substr(sep+1).c_str(), 0, 10);
			}
			return run::Minute::create(hour, minute);
		}
		default:
			throw wibble::exception::Consistency("parsing Run", "unknown Run style " + formatStyle(style));
	}
}

#ifdef HAVE_LUA
int Run::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Run reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const Run& v = **(const Run**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "style")
	{
		string s = Run::formatStyle(v.style());
		lua_pushlstring(L, s.data(), s.size());
		return 1;
	}
	else if (name == "minute" && v.style() == Run::MINUTE)
	{
		const run::Minute* v1 = v.upcast<run::Minute>();
		lua_pushnumber(L, v1->minute / 60);
		lua_pushnumber(L, v1->minute % 60);
		return 2;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_run(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Run::lua_lookup, 2);
	return 1;
}
void Run::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Run** s = (const Run**)lua_newuserdata(L, sizeof(const Run*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_run);
		lua_settable(L, -3);
		/* set the __tostring metamethod */
		lua_pushstring(L, "__tostring");
		lua_pushcfunction(L, utils::lua::tostring_arkitype<Run>);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

namespace run {

Run::Style Minute::style() const { return Run::MINUTE; }

std::string Minute::encodeWithoutEnvelope() const
{
	using namespace utils;
	return Run::encodeWithoutEnvelope() + encodeUInt(minute, 2);
}
std::ostream& Minute::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sis(o);
    return o << formatStyle(style()) << "("
			 << setfill('0') << fixed
			 << setw(2) << (minute / 60) << ":"
			 << setw(2) << (minute % 60) << ")";
}

int Minute::compare(const Run& o) const
{
	int res = Run::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Minute* v = dynamic_cast<const Minute*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Run, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Minute::compare(const Minute& o) const
{
	return minute - o.minute;
}

bool Minute::operator==(const Type& o) const
{
	const Minute* v = dynamic_cast<const Minute*>(&o);
	if (!v) return false;
	return minute == v->minute;
}

Item<Minute> Minute::create(unsigned int hour, unsigned int minute)
{
	Item<Minute> res(new Minute);
	res->minute = hour * 60 + minute;
	return res;
}

}

static MetadataType runType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Run::decode),
	(MetadataType::string_decoder)(&Run::decodeString));

}
}
// vim:set ts=4 sw=4:
