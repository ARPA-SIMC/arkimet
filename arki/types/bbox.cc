#include "arki/exceptions.h"
#include "arki/types/bbox.h"
#include "arki/types/utils.h"
#include "arki/core/binary.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/libconfig.h"
#include <iomanip>
#include <sstream>

#define CODE TYPE_BBOX
#define TAG "bbox"
#define SERSIZELEN 1

using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<BBox>::type_tag = TAG;
const types::Code traits<BBox>::type_code = CODE;
const size_t traits<BBox>::type_sersize_bytes = SERSIZELEN;

BBox::Style BBox::parseStyle(const std::string& str)
{
    if (str == "INVALID") return Style::INVALID;
    if (str == "POINT") return Style::POINT;
    if (str == "BOX") return Style::BOX;
    if (str == "HULL") return Style::HULL;
    throw_consistency_error("parsing BBox style", "cannot parse BBox style '"+str+"': only INVALID and BOX are supported");
}

std::string BBox::formatStyle(BBox::Style s)
{
    switch (s)
    {
        //case BBox::NONE: return "NONE";
        case Style::INVALID: return "INVALID";
        case Style::POINT: return "POINT";
        case Style::BOX: return "BOX";
        case Style::HULL: return "HULL";
        default:
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

bool BBox::equals(const Type& o) const
{
    if (type_code() != o.type_code()) return false;
    // This can be a reinterpret_cast for performance, since we just validated
    // the type code
    const BBox* v = reinterpret_cast<const BBox*>(&o);
    if (!v) return false;
    return true;
}

int BBox::compare(const Type& o) const
{
    int res = Encoded::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const BBox* v = dynamic_cast<const BBox*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `BBox`, but it is `" << typeid(&o).name() << "' instead";
        throw std::runtime_error(ss.str());
    }

    auto sty = style();

    // Compare style
    if (int res = (int)sty - (int)v->style()) return res;

    return 0;
}

bbox::Style BBox::style() const
{
    return (bbox::Style)data[0];
}

std::unique_ptr<BBox> BBox::decode(core::BinaryDecoder& dec, bool reuse_buffer)
{
    dec.ensure_size(1, "bbox style");
    std::unique_ptr<BBox> res;
    if (reuse_buffer)
        res.reset(new BBox(dec.buf, dec.size, false));
    else
        res.reset(new BBox(dec.buf, dec.size));
    dec.skip(dec.size);
    return res;
}

std::unique_ptr<BBox> BBox::decodeString(const std::string& val)
{
    return createInvalid();
}

std::unique_ptr<BBox> BBox::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return createInvalid();
}

std::unique_ptr<BBox> BBox::createInvalid()
{
    uint8_t* buf = new uint8_t[1];
    buf[0] = static_cast<uint8_t>(bbox::Style::INVALID);
    return std::unique_ptr<BBox>(new BBox(buf, 1, true));
}

std::ostream& BBox::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(Style::INVALID) << "()";
}

void BBox::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    e.add(keys.type_style, formatStyle(Style::INVALID));
}

void BBox::init()
{
    MetadataType::register_type<BBox>();
}

}
}
