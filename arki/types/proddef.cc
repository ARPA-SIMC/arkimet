#include "arki/exceptions.h"
#include "arki/types/proddef.h"
#include "arki/types/utils.h"
#include "arki/types/values.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/reader.h"
#include "arki/structured/keys.h"
#include <sstream>

#define CODE TYPE_PRODDEF
#define TAG "proddef"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Proddef>::type_tag = TAG;
const types::Code traits<Proddef>::type_code = CODE;
const size_t traits<Proddef>::type_sersize_bytes = SERSIZELEN;

Proddef::Style Proddef::parseStyle(const std::string& str)
{
    if (str == "GRIB") return Style::GRIB;
    throw_consistency_error("parsing Proddef style", "cannot parse Proddef style '"+str+"': only GRIB is supported");
}

std::string Proddef::formatStyle(Proddef::Style s)
{
    switch (s)
    {
        case Style::GRIB: return "GRIB";
        default:
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

proddef::Style Proddef::style(const uint8_t* data, unsigned size)
{
    return (proddef::Style)data[0];
}

ValueBag Proddef::get_GRIB(const uint8_t* data, unsigned size)
{
    core::BinaryDecoder dec(data + 1, size - 1);
    return ValueBag::decode_reusing_buffer(dec);
}

proddef::Style Proddef::style() const { return style(data, size); }
ValueBag Proddef::get_GRIB() const { return get_GRIB(data, size); }

int Proddef::compare(const Type& o) const
{
    int res = Encoded::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Proddef* v = dynamic_cast<const Proddef*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `Proddef`, but it is `" << typeid(&o).name() << "' instead";
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
        case proddef::Style::GRIB:
            return reinterpret_cast<const proddef::GRIB*>(this)->compare_local(
                    *reinterpret_cast<const proddef::GRIB*>(v));
        default:
            throw_consistency_error("parsing Proddef", "unknown Proddef style " + formatStyle(sty));
    }
}

std::unique_ptr<Proddef> Proddef::decode(core::BinaryDecoder& dec, bool reuse_buffer)
{
    dec.ensure_size(1, "Proddef style");
    Style sty = static_cast<proddef::Style>(dec.buf[0]);
    std::unique_ptr<Proddef> res;
    switch (sty)
    {
        case Style::GRIB:
            if (reuse_buffer)
                res.reset(new proddef::GRIB(dec.buf, dec.size, false));
            else
                res.reset(new proddef::GRIB(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        default:
            throw std::runtime_error("cannot parse Proddef: style is " + formatStyle(sty) + " but we can only decode GRIB");
    }
    return res;
}

unique_ptr<Proddef> Proddef::decodeString(const std::string& val)
{
    std::string inner;
    Proddef::Style sty = outerParse<Proddef>(val, inner);
    switch (sty)
    {
        case Style::GRIB: return createGRIB(ValueBag::parse(inner));
        default:
            throw_consistency_error("parsing Proddef", "unknown Proddef style " + formatStyle(sty));
    }
}

std::unique_ptr<Proddef> Proddef::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    proddef::Style sty = parseStyle(val.as_string(keys.type_style, "type style"));
    std::unique_ptr<Proddef> res;
    switch (sty)
    {
        case Style::GRIB:
            val.sub(keys.proddef_value, "proddef value", [&](const structured::Reader& reader) {
                res = createGRIB(ValueBag::parse(reader));
            });
            return res;
        default:
            throw std::runtime_error("unknown proddef style");
    }
}

unique_ptr<Proddef> Proddef::createGRIB(const ValueBag& values)
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned(static_cast<unsigned>(proddef::Style::GRIB), 1);
    values.encode(enc);
    return std::unique_ptr<Proddef>(new proddef::GRIB(buf));
}

namespace proddef {

GRIB::~GRIB() { /* cache_grib.uncache(this); */ }

std::ostream& GRIB::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(Style::GRIB) << "(" << get_GRIB().toString() << ")";
}

void GRIB::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    auto values = get_GRIB();
    e.add(keys.type_style, formatStyle(Style::GRIB));
    e.add(keys.proddef_value);
    values.serialise(e);
}

std::string GRIB::exactQuery() const
{
    return "GRIB:" + get_GRIB().toString();
}

int GRIB::compare_local(const GRIB& o) const
{
    return get_GRIB().compare(o.get_GRIB());
}

}

void Proddef::init()
{
    MetadataType::register_type<Proddef>();
}

}
}
