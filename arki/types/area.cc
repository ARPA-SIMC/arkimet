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

Area::Area()
{
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

unique_ptr<Area> Area::decode(core::BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "area");
    switch (s)
    {
        case Style::GRIB:
            return createGRIB(ValueBag::decode(dec));
        case Style::ODIMH5:
            return createODIMH5(ValueBag::decode(dec));
        case Style::VM2:
            return createVM2(dec.pop_uint(4, "VM station id"));
        default:
            throw std::runtime_error("cannot parse Area: style is " + formatStyle(s) + " but we can only decode GRIB, ODIMH5 and VM2");
    }
}

unique_ptr<Area> Area::decodeString(const std::string& val)
{
	string inner;
	Area::Style style = outerParse<Area>(val, inner);
    switch (style)
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
            throw_consistency_error("parsing Area", "unknown Area style " + formatStyle(style));
    }
}

std::unique_ptr<Area> Area::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    switch (style_from_structure(keys, val))
    {
        case Style::GRIB: return upcast<Area>(area::GRIB::decode_structure(keys, val));
        case Style::ODIMH5: return upcast<Area>(area::ODIMH5::decode_structure(keys, val));
        case Style::VM2: return upcast<Area>(area::VM2::decode_structure(keys, val));
        default: throw std::runtime_error("unknown area style");
    }
}

unique_ptr<Area> Area::createGRIB(const ValueBag& values)
{
    return upcast<Area>(area::GRIB::create(values));
}
unique_ptr<Area> Area::createODIMH5(const ValueBag& values)
{
    return upcast<Area>(area::ODIMH5::create(values));
}
unique_ptr<Area> Area::createVM2(unsigned station_id)
{
    return upcast<Area>(area::VM2::create(station_id));
}

namespace area {

GRIB::~GRIB() { /* cache_grib.uncache(this); */ }

Area::Style GRIB::style() const { return Style::GRIB; }

void GRIB::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Area::encodeWithoutEnvelope(enc);
    m_values.encode(enc);
}
std::ostream& GRIB::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "(" << m_values.toString() << ")";
}
void GRIB::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Area::serialise_local(e, keys, f);
    e.add(keys.area_value);
    m_values.serialise(e);
}

std::unique_ptr<GRIB> GRIB::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    std::unique_ptr<GRIB> res;
    val.sub(keys.area_value, "area value", [&](const structured::Reader& reader) {
        res = GRIB::create(ValueBag::parse(reader));
    });
    return res;
}
std::string GRIB::exactQuery() const
{
    return "GRIB:" + m_values.toString();
}

int GRIB::compare_local(const Area& o) const
{
    if (int res = Area::compare_local(o)) return res;
    // We should be the same kind, so upcast
    const GRIB* v = dynamic_cast<const GRIB*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            string("second element claims to be a GRIB Area, but is a ") + typeid(&o).name() + " instead");

    return m_values.compare(v->m_values);
}

bool GRIB::equals(const Type& o) const
{
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v) return false;
	return m_values == v->m_values;
}

GRIB* GRIB::clone() const
{
    GRIB* res = new GRIB;
    res->m_values = m_values;
    return res;
}

unique_ptr<GRIB> GRIB::create(const ValueBag& values)
{
    GRIB* res = new GRIB;
    res->m_values = values;
    return unique_ptr<GRIB>(res);
}

ODIMH5::~ODIMH5() { /* cache_odimh5.uncache(this); */ }

Area::Style ODIMH5::style() const { return Style::ODIMH5; }

void ODIMH5::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Area::encodeWithoutEnvelope(enc);
    m_values.encode(enc);
}
std::ostream& ODIMH5::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "(" << m_values.toString() << ")";
}
void ODIMH5::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Area::serialise_local(e, keys, f);
    e.add(keys.area_value);
    m_values.serialise(e);
}

std::unique_ptr<ODIMH5> ODIMH5::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    std::unique_ptr<ODIMH5> res;
    val.sub(keys.area_value, "area value", [&](const structured::Reader& reader) {
        res = ODIMH5::create(ValueBag::parse(reader));
    });
    return res;
}
std::string ODIMH5::exactQuery() const
{
    return "ODIMH5:" + m_values.toString();
}

int ODIMH5::compare_local(const Area& o) const
{
    if (int res = Area::compare_local(o)) return res;
    // We should be the same kind, so upcast
    const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            string("second element claims to be a ODIMH5 Area, but is a ") + typeid(&o).name() + " instead");

    return m_values.compare(v->m_values);
}

bool ODIMH5::equals(const Type& o) const
{
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v) return false;
	return m_values == v->m_values;
}

ODIMH5* ODIMH5::clone() const
{
    ODIMH5* res = new ODIMH5;
    res->m_values = m_values;
    return res;
}

unique_ptr<ODIMH5> ODIMH5::create(const ValueBag& values)
{
    ODIMH5* res = new ODIMH5;
    res->m_values = values;
    return unique_ptr<ODIMH5>(res);
}

VM2::~VM2() {}

const ValueBag& VM2::derived_values() const {
    if (m_derived_values.get() == 0) {
#ifdef HAVE_VM2
        m_derived_values.reset(new ValueBag(utils::vm2::get_station(m_station_id)));
#else
        m_derived_values.reset(new ValueBag);
#endif
    }
    return *m_derived_values;
}

Area::Style VM2::style() const { return Style::VM2; }

void VM2::encodeWithoutEnvelope(core::BinaryEncoder& enc) const
{
    Area::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_station_id, 4);
    derived_values().encode(enc);
}

void VM2::encode_for_indexing(core::BinaryEncoder& enc) const
{
    Area::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_station_id, 4);
}

std::ostream& VM2::writeToOstream(std::ostream& o) const
{
    o << formatStyle(style()) << "(" << m_station_id;
    if (!derived_values().empty())
        o << "," << derived_values().toString();
    return o << ")";
}
void VM2::serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f) const
{
    Area::serialise_local(e, keys, f);
    e.add(keys.area_id, m_station_id);
    if (!derived_values().empty()) {
        e.add(keys.area_value);
        derived_values().serialise(e);
    }
}

std::string VM2::exactQuery() const
{
    stringstream ss;
    ss << "VM2," << m_station_id;
    if (!derived_values().empty())
        ss << ":" << derived_values().toString();
    return ss.str();
}

int VM2::compare_local(const Area& o) const
{
    if (int res = Area::compare_local(o)) return res;
    const VM2* v = dynamic_cast<const VM2*>(&o);
    if (!v)
        throw_consistency_error(
            "comparing metadata types",
            string("second element claims to be a VM2 Area, but is a ") + typeid(&o).name() + " instead");
    if (m_station_id == v->m_station_id) return 0;
    return (m_station_id > v->m_station_id ? 1 : -1);
}

bool VM2::equals(const Type& o) const
{
    const VM2* v = dynamic_cast<const VM2*>(&o);
    if (!v) return false;
    return m_station_id == v->m_station_id;
}

VM2* VM2::clone() const
{
    VM2* res = new VM2;
    res->m_station_id = m_station_id;
    return res;
}

unique_ptr<VM2> VM2::create(unsigned station_id)
{
    VM2* res = new VM2;
    res->m_station_id = station_id;
    return unique_ptr<VM2>(res);
}

std::unique_ptr<VM2> VM2::decode_structure(const structured::Keys& keys, const structured::Reader& val)
{
    return VM2::create(val.as_int(keys.area_id, "vm2 id"));
}

}

void Area::init()
{
    MetadataType::register_type<Area>();
}

}
}

#include <arki/types/styled.tcc>
