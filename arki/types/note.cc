#include <arki/exceptions.h>
#include <arki/types/note.h>
#include <arki/types/utils.h>
#include <arki/binary.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include "config.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE TYPE_NOTE
#define TAG "note"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;

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
		throw_consistency_error(
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

bool Note::equals(const Type& o) const
{
	const Note* v = dynamic_cast<const Note*>(&o);
	if (!v) return false;
	return time == v->time && content == v->content;
}

void Note::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    time.encodeWithoutEnvelope(enc);
    enc.add_varint(content.size());
    enc.add_raw(content);
}

unique_ptr<Note> Note::decode(BinaryDecoder& dec)
{
    Time t = Time::decode(dec);
    size_t msg_len = dec.pop_varint<size_t>("note text size");
    string msg = dec.pop_string(msg_len, "note text");
    return Note::create(t, msg);
}

std::ostream& Note::writeToOstream(std::ostream& o) const
{
	return o << "[" << time << "]" << content;
}

void Note::serialiseLocal(Emitter& e, const Formatter* f) const
{
    e.add("ti"); time.serialiseList(e);
    e.add("va", content);
}

unique_ptr<Note> Note::decodeMapping(const emitter::memory::Mapping& val)
{
    return Note::create(
            Time::decodeList(val["ti"].want_list("parsing Note time")),
            val["va"].want_string("parsing Note content"));
}

unique_ptr<Note> Note::decodeString(const std::string& val)
{
    if (val.empty())
        throw_consistency_error("parsing Note", "string is empty");
    if (val[0] != '[')
        throw_consistency_error("parsing Note", "string does not start with open square bracket");
    size_t pos = val.find(']');
    if (pos == string::npos)
        throw_consistency_error("parsing Note", "no closed square bracket found");
    return Note::create(Time::createFromISO8601(val.substr(1, pos-1)), val.substr(pos+1));
}

#ifdef HAVE_LUA
bool Note::lua_lookup(lua_State* L, const std::string& name) const
{
    if (name == "time")
        time.lua_push(L);
    else if (name == "content")
        lua_pushlstring(L, content.data(), content.size());
    else
        return CoreType<Note>::lua_lookup(L, name);
    return true;
}
#endif

Note* Note::clone() const
{
    return new Note(time, content);
}

unique_ptr<Note> Note::create(const std::string& content)
{
    return unique_ptr<Note>(new Note(content));
}

unique_ptr<Note> Note::create(const Time& time, const std::string& content)
{
    return unique_ptr<Note>(new Note(time, content));
}

static MetadataType noteType = MetadataType::create<Note>();

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
