/*
 * types/note - Metadata annotation
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
#include <arki/types/note.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include "config.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_NOTE
#define TAG "note"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki {
namespace types {

int Note::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Note* v = dynamic_cast<const Note*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Note, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Note::compare(const Note& o) const
{
	if (int res = time.compare(o.time)) return res;
	if (content < o.content) return -1;
	if (content > o.content) return 1;
	return 0;
}

bool Note::operator==(const Type& o) const
{
	const Note* v = dynamic_cast<const Note*>(&o);
	if (!v) return false;
	return time == v->time && content == v->content;
}

types::Code Note::serialisationCode() const { return CODE; }
size_t Note::serialisationSizeLength() const { return SERSIZELEN; }
std::string Note::tag() const { return TAG; }

void Note::encodeWithoutEnvelope(Encoder& enc) const
{
	time->encodeWithoutEnvelope(enc);
	enc.addVarint(content.size());
	enc.addString(content);
}

Item<Note> Note::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;

	ensureSize(len, 5, "note time");
	Item<Time> t = Time::decode(buf, 5);
	Decoder dec(buf+5, len-5);
	size_t msg_len = dec.popVarint<size_t>("note text size");
	string msg = dec.popString(msg_len, "note text");
	return Note::create(t, msg);
}

std::ostream& Note::writeToOstream(std::ostream& o) const
{
	return o << "[" << time << "]" << content;
}
const char* Note::lua_type_name() const { return "arki.types.note"; }

Item<Note> Note::decodeString(const std::string& val)
{
	if (val.empty())
		throw wibble::exception::Consistency("parsing Note", "string is empty");
	if (val[0] != '[')
		throw wibble::exception::Consistency("parsing Note", "string does not start with open square bracket");
	size_t pos = val.find(']');
	if (pos == string::npos)
		throw wibble::exception::Consistency("parsing Note", "no closed square bracket found");
	return Note::create(Time::createFromISO8601(val.substr(1, pos-1)), val.substr(pos+1));
}

#ifdef HAVE_LUA
int Note::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Note reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const Note& v = **(const Note**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "time")
	{
		v.time->lua_push(L);
		return 1;
	}
	else if (name == "content")
	{
		lua_pushlstring(L, v.content.data(), v.content.size());
		return 1;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_note(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Note::lua_lookup, 2);
	return 1;
}
void Note::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Note** s = (const Note**)lua_newuserdata(L, sizeof(const Note*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_note);
		lua_settable(L, -3);
		/* set the __tostring metamethod */
		lua_pushstring(L, "__tostring");
		lua_pushcfunction(L, utils::lua::tostring_arkitype<Note>);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

Item<Note> Note::create(const std::string& content)
{
	return new Note(Time::createNow(), content);
}

Item<Note> Note::create(const Item<types::Time>& time, const std::string& content)
{
	return new Note(time, content);
}

static MetadataType noteType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Note::decode),
	(MetadataType::string_decoder)(&Note::decodeString));

}
}
// vim:set ts=4 sw=4:
