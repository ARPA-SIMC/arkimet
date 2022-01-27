#include "reftime.h"
#include "utils.h"
#include "arki/exceptions.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/reader.h"
#include "arki/structured/keys.h"
#include "arki/stream/text.h"
#include <sstream>
#include <cstring>

#define CODE TYPE_REFTIME
#define TAG "reftime"
#define SERSIZELEN 1

namespace arki {
namespace types {
	
const char* traits<Reftime>::type_tag = TAG;
const types::Code traits<Reftime>::type_code = CODE;
const size_t traits<Reftime>::type_sersize_bytes = SERSIZELEN;

Reftime::Style Reftime::parseStyle(const std::string& str)
{
	if (str == "POSITION") return reftime::Style::POSITION;
	if (str == "PERIOD") return reftime::Style::PERIOD;
	throw_consistency_error("parsing Reftime style", "cannot parse Reftime style '"+str+"': only POSITION and PERIOD are supported");
}

std::string Reftime::formatStyle(Reftime::Style s)
{
	switch (s)
	{
		case Style::POSITION: return "POSITION";
		case Style::PERIOD: return "PERIOD";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

int Reftime::compare(const Type& o) const
{
    int res = Encoded::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Reftime* v = dynamic_cast<const Reftime*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `Reftime`, but it is `" << typeid(&o).name() << "' instead";
        throw std::runtime_error(ss.str());
    }

    auto sty = style();

    // Compare style
    if (int res = (int)sty - (int)v->style()) return res;
    if (int res = size - v->size) return res;

    // All styles of reftime are encoded in a way that lexicographically
    // compares
    return memcmp(data, v->data, size);
}

reftime::Style Reftime::style(const uint8_t* data, unsigned size)
{
    return (reftime::Style)data[0];
}

core::Time Reftime::get_Position(const uint8_t* data, unsigned size)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    return core::Time::decode(dec);
}

void Reftime::get_Period(const uint8_t* data, unsigned size, core::Time& begin, core::Time& end)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    begin = core::Time::decode(dec);
    end = core::Time::decode(dec);
}

std::unique_ptr<Reftime> Reftime::decode(core::BinaryDecoder& dec, bool reuse_buffer)
{
    dec.ensure_size(1, "Reftime style");
    Style sty = static_cast<reftime::Style>(dec.buf[0]);
    std::unique_ptr<Reftime> res;
    switch (sty)
    {
        case Style::POSITION:
            if (reuse_buffer)
                res.reset(new reftime::Position(dec.buf, dec.size, false));
            else
                res.reset(new reftime::Position(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        default:
            throw std::runtime_error("cannot parse Reftime: found unsupported style " + formatStyle(sty));
    }
    return res;
}

std::unique_ptr<Reftime> Reftime::decodeString(const std::string& val)
{
    return createPosition(core::Time::decodeString(val));
}

std::unique_ptr<Reftime> Reftime::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    Style sty = parseStyle(val.as_string(keys.type_style, "type style"));
    std::unique_ptr<Reftime> res;
    switch (sty)
    {
        case Style::POSITION:
            return createPosition(val.as_time(keys.reftime_position_time, "time"));
        default:
            throw std::runtime_error("unknown reftime style");
    }
}

std::unique_ptr<Reftime> Reftime::createPosition(const core::Time& position)
{
    uint8_t* buf = new uint8_t[6];
    buf[0] = (uint8_t)reftime::Style::POSITION;
    position.encode_binary(buf + 1);
    return std::unique_ptr<Reftime>(new reftime::Position(buf, 6, true));
}

void Reftime::write_documentation(stream::Text& out, unsigned heading_level)
{
    out.rst_header("Reftime", heading_level);
    out.print(Reftime::doc);
}


namespace reftime {

/*
 * Position
 */

std::ostream& Position::writeToOstream(std::ostream& o) const
{
    auto time = get_Position();
    return o << time;
}

void Position::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    auto time = get_Position();
    e.add(keys.type_style, formatStyle(Style::POSITION));
    e.add(keys.reftime_position_time);
    e.add(time);
}

std::string Position::exactQuery() const
{
    return "=" + get_Position().to_iso8601();
}

void Position::expand_date_range(core::Interval& interval) const
{
    auto time = get_Position();
    if (interval.begin.ye == 0 || interval.begin > time)
        interval.begin = time;

    if (interval.end.ye == 0 || interval.end <= time)
    {
        interval.end = time;
        interval.end.se += 1;
        interval.end.normalise();
    }
}

}

void Reftime::init()
{
    MetadataType::register_type<Reftime>();
}

}
}
