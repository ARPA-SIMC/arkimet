#include "note.h"
#include "utils.h"
#include "arki/exceptions.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/reader.h"
#include "arki/structured/keys.h"
#include <sstream>

#define CODE TYPE_NOTE
#define TAG "note"
#define SERSIZELEN 2

using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace types {

const char* traits<Note>::type_tag = TAG;
const types::Code traits<Note>::type_code = CODE;
const size_t traits<Note>::type_sersize_bytes = SERSIZELEN;

void Note::get(core::Time& time, std::string& content) const
{
    core::BinaryDecoder dec(data, size);
    time = Time::decode(dec);
    size_t len = dec.pop_varint<size_t>("note text size");
    content = dec.pop_string(len, "note text");
}

int Note::compare(const Type& o) const
{
    int res = Type::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Note* v = dynamic_cast<const Note*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            std::string("second element claims to be a Note, but it is a ") + typeid(&o).name() + " instead");

    core::Time time, vtime;
    std::string content, vcontent;
    get(time, content);
    v->get(vtime, vcontent);

    if (int res = time.compare(vtime)) return res;
    if (content < vcontent) return -1;
    if (content > vcontent) return 1;
    return 0;
}

std::unique_ptr<Note> Note::decode(core::BinaryDecoder& dec, bool reuse_buffer)
{
    dec.ensure_size(6, "Note data");
    std::unique_ptr<Note> res;
    if (reuse_buffer)
        res.reset(new Note(dec.buf, dec.size, false));
    else
        res.reset(new Note(dec.buf, dec.size));
    dec.skip(dec.size);
    return res;
}

std::ostream& Note::writeToOstream(std::ostream& o) const
{
    core::Time time;
    std::string content;
    get(time, content);
    return o << "[" << time << "]" << content;
}

void Note::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    core::Time time;
    std::string content;
    get(time, content);
    e.add(keys.note_time); e.add(time);
    e.add(keys.note_value, content);
}

std::unique_ptr<Note> Note::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return Note::create(
            val.as_time(keys.note_time, "Note time"),
            val.as_string(keys.note_value, "Note content"));
}

std::unique_ptr<Note> Note::decodeString(const std::string& val)
{
    if (val.empty())
        throw_consistency_error("parsing Note", "string is empty");
    if (val[0] != '[')
        throw_consistency_error("parsing Note", "string does not start with open square bracket");
    size_t pos = val.find(']');
    if (pos == std::string::npos)
        throw_consistency_error("parsing Note", "no closed square bracket found");
    return Note::create(Time::create_iso8601(val.substr(1, pos-1)), val.substr(pos+1));
}

std::unique_ptr<Note> Note::create(const std::string& content)
{
    return create(core::Time::create_now(), content);
}

std::unique_ptr<Note> Note::create(const Time& time, const std::string& content)
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    time.encodeWithoutEnvelope(enc);
    enc.add_varint(content.size());
    enc.add_raw(content);
    return std::unique_ptr<Note>(new Note(buf));
}

void Note::init()
{
    MetadataType::register_type<Note>();
}

}
}
