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

const char* traits<Note>::type_tag = TAG;
const types::Code traits<Note>::type_code = CODE;
const size_t traits<Note>::type_sersize_bytes = SERSIZELEN;
const char* traits<Note>::type_lua_tag = LUATAG_TYPES ".note";

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
bool Note::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "time")
		time->lua_push(L);
	else if (name == "content")
		lua_pushlstring(L, content.data(), content.size());
	else
		return CoreType<Note>::lua_lookup(L, name);
	return true;
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
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
