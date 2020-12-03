#include "arki/exceptions.h"
#include "arki/types/run.h"
#include "arki/types/utils.h"
#include "arki/utils/iostream.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <iomanip>
#include <sstream>

#define CODE TYPE_RUN
#define TAG "run"
#define SERSIZELEN 1

using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Run>::type_tag = TAG;
const types::Code traits<Run>::type_code = CODE;
const size_t traits<Run>::type_sersize_bytes = SERSIZELEN;

Run::~Run() {}

Run::Style Run::parseStyle(const std::string& str)
{
    if (str == "MINUTE") return Style::MINUTE;
    throw_consistency_error("parsing Run style", "cannot parse Run style '"+str+"': only MINUTE is supported");
}

std::string Run::formatStyle(Run::Style s)
{
    switch (s)
    {
        case Style::MINUTE: return "MINUTE";
        default:
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

int Run::compare(const Type& o) const
{
    int res = Encoded::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Run* v = dynamic_cast<const Run*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `Run`, but it is `" << typeid(&o).name() << "' instead";
        throw std::runtime_error(ss.str());
    }

    auto sty = style();

    // Compare style
    if (int res = (int)sty - (int)v->style()) return res;

    // Styles are the same, compare the rest.
    //
    // We can safely reinterpret_cast, avoiding an expensive dynamic_cast,
    // since we checked the style.
    switch (sty)
    {
        case run::Style::MINUTE:
            return reinterpret_cast<const run::Minute*>(this)->compare_local(
                    *reinterpret_cast<const run::Minute*>(v));
        default:
            throw_consistency_error("parsing Run", "unknown Run style " + formatStyle(sty));
    }
}

run::Style Run::style(const uint8_t* data, unsigned size)
{
    return (run::Style)data[0];
}


unsigned Run::get_Minute(const uint8_t* data, unsigned size)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    return dec.pop_varint<unsigned>("run minute");
}

std::unique_ptr<Run> Run::decode(core::BinaryDecoder& dec, bool reuse_buffer)
{
    dec.ensure_size(1, "run style");
    Style sty = static_cast<run::Style>(dec.buf[0]);
    std::unique_ptr<Run> res;
    switch (sty)
    {
        case Style::MINUTE:
            if (reuse_buffer)
                res.reset(new run::Minute(dec.buf, dec.size, false));
            else
                res.reset(new run::Minute(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        default:
            throw std::runtime_error("cannot parse Run: unknown style " + formatStyle(sty));
    }
    return res;
}

std::unique_ptr<Run> Run::decodeString(const std::string& val)
{
    std::string inner;
    Run::Style sty = outerParse<Run>(val, inner);
    switch (sty)
    {
        case Style::MINUTE:
        {
            size_t sep = inner.find(':');
            int hour, minute;
            if (sep == std::string::npos)
            {
                // 12
                hour = strtoul(inner.c_str(), 0, 10);
                minute = 0;
            } else {
                // 12:00
                hour = strtoul(inner.substr(0, sep).c_str(), 0, 10);
                minute = strtoul(inner.substr(sep+1).c_str(), 0, 10);
            }
            return createMinute(hour, minute);
        }
        default:
            throw_consistency_error("parsing Run", "unknown Run style " + formatStyle(sty));
    }
}

std::unique_ptr<Run> Run::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    run::Style sty = parseStyle(val.as_string(keys.type_style, "type style"));
    std::unique_ptr<Run> res;
    switch (sty)
    {
        case Style::MINUTE:
        {
            unsigned int m = val.as_int(keys.run_value, "run value");
            return createMinute(m / 60, m % 60);
        }
        default: throw std::runtime_error("Unknown Run style");
    }
}

std::unique_ptr<Run> Run::createMinute(unsigned int hour, unsigned int minute)
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned(static_cast<unsigned>(run::Style::MINUTE), 1);
    enc.add_varint(hour * 60 + minute);
    return std::unique_ptr<Run>(new run::Minute(buf));
}

namespace run {

std::ostream& Minute::writeToOstream(std::ostream& o) const
{
    using namespace std;
    utils::SaveIOState sis(o);
    auto minute = get_Minute();
    return o << formatStyle(style()) << "("
             << setfill('0') << fixed
             << setw(2) << (minute / 60) << ":"
             << setw(2) << (minute % 60) << ")";
}

void Minute::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    auto minute = get_Minute();
    e.add(keys.type_style, formatStyle(Style::MINUTE));
    e.add(keys.run_value, minute);
}

std::string Minute::exactQuery() const
{
    using namespace std;
    auto minute = get_Minute();
    std::stringstream res;
    res << "MINUTE," << setfill('0') << setw(2) << (minute/60) << ":" << setw(2) << (minute % 60);
    return res.str();
}

int Minute::compare_local(const Minute& o) const
{
    int minute = get_Minute();
    int vminute = o.get_Minute();
    return minute - vminute;
}

}

void Run::init()
{
    MetadataType::register_type<Run>();
}

}
}
