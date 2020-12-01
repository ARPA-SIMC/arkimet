#include "arki/exceptions.h"
#include "arki/types/area.h"
#include "arki/types/utils.h"
#include "arki/core/binary.h"
#include "arki/utils/geos.h"
#include "arki/utils/string.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include "arki/bbox.h"
#include "arki/libconfig.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_VM2
#include "arki/utils/vm2.h"
#endif

#define CODE TYPE_AREA
#define TAG "area"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Area>::type_tag = TAG;
const types::Code traits<Area>::type_code = CODE;
const size_t traits<Area>::type_sersize_bytes = SERSIZELEN;

Area::Style Area::parseStyle(const std::string& str)
{
    if (str == "GRIB") return Style::GRIB;
    if (str == "ODIMH5") return Style::ODIMH5;
    if (str == "VM2") return Style::VM2;
    throw_consistency_error("parsing Area style", "cannot parse Area style '"+str+"': only GRIB,ODIMH5 is supported");
}

std::string Area::formatStyle(Area::Style s)
{
    switch (s)
    {
        case Style::GRIB: return "GRIB";
        case Style::ODIMH5: return "ODIMH5";
        case Style::VM2: return "VM2";
        default:
            std::stringstream str;
            str << "(unknown " << (int)s << ")";
            return str.str();
    }
}

ValueBag Area::get_GRIB() const
{
    core::BinaryDecoder dec(data + 1, size - 1);
    return ValueBag::decode(dec);
}
ValueBag Area::get_ODIMH5() const
{
    core::BinaryDecoder dec(data + 1, size - 1);
    return ValueBag::decode(dec);
}
unsigned Area::get_VM2() const
{
    core::BinaryDecoder dec(data + 1, size - 1);
    return dec.pop_uint(4, "VM station id");
}

int Area::compare(const Type& o) const
{
    int res = Encoded::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Area* v = dynamic_cast<const Area*>(&o);
    if (!v)
    {
        std::stringstream ss;
        ss << "cannot compare metadata types: second element claims to be `Area`, but it is `" << typeid(&o).name() << "' instead";
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
        case area::Style::GRIB:
            return reinterpret_cast<const area::GRIB*>(this)->compare_local(
                    *reinterpret_cast<const area::GRIB*>(v));
        case area::Style::ODIMH5:
            return reinterpret_cast<const area::ODIMH5*>(this)->compare_local(
                    *reinterpret_cast<const area::ODIMH5*>(v));
        case area::Style::VM2:
            return reinterpret_cast<const area::VM2*>(this)->compare_local(
                    *reinterpret_cast<const area::VM2*>(v));
        default:
            throw_consistency_error("parsing Area", "unknown Area style " + formatStyle(sty));
    }
}

area::Style Area::style() const
{
    return (area::Style)data[0];
}

const arki::utils::geos::Geometry* Area::bbox() const
{
static thread_local std::unique_ptr<BBox> bbox;

#ifdef HAVE_GEOS
    if (!cached_bbox)
    {
        if (!bbox)
            bbox = BBox::create();
        std::unique_ptr<arki::utils::geos::Geometry> res = bbox->compute(*this);
        if (res.get())
            cached_bbox = res.release();
    }
#endif
    return cached_bbox;
}

std::unique_ptr<Area> Area::decode(core::BinaryDecoder& dec)
{
    dec.ensure_size(1, "Area style");
    Style sty = static_cast<area::Style>(dec.buf[0]);
    std::unique_ptr<Area> res;
    switch (sty)
    {
        case Style::GRIB:
            res.reset(new area::GRIB(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        case Style::ODIMH5:
            res.reset(new area::ODIMH5(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        case Style::VM2:
            dec.ensure_size(5, "VM data");
            res.reset(new area::VM2(dec.buf, dec.size));
            dec.skip(dec.size);
            break;
        default:
            throw std::runtime_error("cannot parse Area: style is " + formatStyle(sty) + " but we can only decode GRIB, ODIMH5 and VM2");
    }
    return res;
}

unique_ptr<Area> Area::decodeString(const std::string& val)
{
    std::string inner;
    Area::Style sty = outerParse<Area>(val, inner);
    switch (sty)
    {
        case Style::GRIB: return createGRIB(ValueBag::parse(inner));
        case Style::ODIMH5: return createODIMH5(ValueBag::parse(inner));
        case Style::VM2: {
            const char* innerptr = inner.c_str();
            char* endptr;
            unsigned long station_id = strtoul(innerptr, &endptr, 10); 
            if (innerptr == endptr)
                throw std::runtime_error("cannot parse" + inner + ": expected a number, but found \"" + inner +"\"");
            return createVM2(station_id);
        }
        default:
            throw_consistency_error("parsing Area", "unknown Area style " + formatStyle(sty));
    }
}

std::unique_ptr<Area> Area::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    area::Style sty = parseStyle(val.as_string(keys.type_style, "type style"));
    std::unique_ptr<Area> res;
    switch (sty)
    {
        case Style::GRIB:
            val.sub(keys.area_value, "area value", [&](const structured::Reader& reader) {
                res = createGRIB(ValueBag::parse(reader));
            });
            return res;
        case Style::ODIMH5:
            val.sub(keys.area_value, "area value", [&](const structured::Reader& reader) {
                res = createODIMH5(ValueBag::parse(reader));
            });
            return res;
        case Style::VM2:
            return createVM2(val.as_int(keys.area_id, "vm2 id"));
        default:
            throw std::runtime_error("unknown area style");
    }
}

std::unique_ptr<Area> Area::createGRIB(const ValueBag& values)
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned(static_cast<unsigned>(area::Style::GRIB), 1);
    values.encode(enc);
    return std::unique_ptr<Area>(new area::GRIB(buf));
}

std::unique_ptr<Area> Area::createODIMH5(const ValueBag& values)
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned(static_cast<unsigned>(area::Style::ODIMH5), 1);
    values.encode(enc);
    return std::unique_ptr<Area>(new area::ODIMH5(buf));
}

std::unique_ptr<Area> Area::createVM2(unsigned station_id)
{
    std::vector<uint8_t> buf;
    core::BinaryEncoder enc(buf);
    enc.add_unsigned(static_cast<unsigned>(area::Style::VM2), 1);
    enc.add_unsigned(station_id, 4);
    // TODO: add derived values? dv.encode(enc);
    // but then implement encode_for_indexing without
    // better: reverse encoding: do not keep derived values in ram, pull them
    // in only when serializing for transmission
    return std::unique_ptr<Area>(new area::VM2(buf));
}


namespace area {

/*
 * GRIB
 */

GRIB::~GRIB() {}

int GRIB::compare_local(const GRIB& o) const
{
    return get_GRIB().compare(o.get_GRIB());
}

std::ostream& GRIB::writeToOstream(std::ostream& o) const
{
    auto values = get_GRIB();
    return o << formatStyle(area::Style::GRIB) << "(" << values.toString() << ")";
}

void GRIB::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    auto values = get_GRIB();
    e.add(keys.type_style, formatStyle(Style::GRIB));
    e.add(keys.area_value);
    values.serialise(e);
}

std::string GRIB::exactQuery() const
{
    return "GRIB:" + get_GRIB().toString();
}


/*
 * ODIMH5
 */

ODIMH5::~ODIMH5() {}

int ODIMH5::compare_local(const ODIMH5& o) const
{
    return get_ODIMH5().compare(o.get_ODIMH5());
}

std::ostream& ODIMH5::writeToOstream(std::ostream& o) const
{
    auto values = get_ODIMH5();
    return o << formatStyle(area::Style::ODIMH5) << "(" << values.toString() << ")";
}

void ODIMH5::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    auto values = get_ODIMH5();
    e.add(keys.type_style, formatStyle(Style::ODIMH5));
    e.add(keys.area_value);
    values.serialise(e);
}

std::string ODIMH5::exactQuery() const
{
    return "ODIMH5:" + get_ODIMH5().toString();
}


/*
 * VM2
 */

VM2::~VM2() {}

int VM2::compare_local(const VM2& o) const
{
    return get_VM2() - o.get_VM2();
}

std::ostream& VM2::writeToOstream(std::ostream& o) const
{
    auto station_id = get_VM2();
    o << formatStyle(area::Style::VM2) << "(" << station_id;

    auto dv = get_VM2_derived_values(station_id);
    if (!dv.empty())
        o << "," << dv.toString();
    return o << ")";
}

void VM2::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    auto station_id = get_VM2();
    e.add(keys.type_style, formatStyle(Style::VM2));
    e.add(keys.area_id, station_id);

    auto dv = get_VM2_derived_values(station_id);
    if (!dv.empty()) {
        e.add(keys.area_value);
        dv.serialise(e);
    }
}

std::string VM2::exactQuery() const
{
    return "VM2," + std::to_string(get_VM2());
}

void VM2::encode_for_indexing(core::BinaryEncoder& enc) const
{
    enc.add_raw(data, 5);
}

ValueBag VM2::get_VM2_derived_values(unsigned station_id)
{
#ifdef HAVE_VM2
    return ValueBag(utils::vm2::get_station(station_id));
#else
    return ValueBag();
#endif
}

}

void Area::init()
{
    MetadataType::register_type<Area>();
}

}
}

#include <arki/types/styled.tcc>
