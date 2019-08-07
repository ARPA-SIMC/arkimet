#include "note.h"
#include "utils.h"
#include "arki/exceptions.h"
#include "arki/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/utils/lua.h"
#include "config.h"
#include <sstream>
#include <cmath>

#define CODE TYPE_NOTE
#define TAG "note"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;
using arki::core::Time;

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

void Note::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.note_time); e.add(time);
    e.add(keys.note_value, content);
}

std::unique_ptr<Note> Note::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return Note::create(
            val.as_time(keys.note_time, "Note time"),
            val.as_string(keys.note_value, "Note content"));
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
    return Note::create(Time::create_iso8601(val.substr(1, pos-1)), val.substr(pos+1));
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

void Note::init()
{
    MetadataType::register_type<Note>();
}

}
}

#include <arki/types/core.tcc>
