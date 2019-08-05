#include "arki/exceptions.h"
#include "arki/types/area.h"
#include "arki/types/utils.h"
#include "arki/binary.h"
#include "arki/utils/geos.h"
#include "arki/utils/string.h"
#include "arki/emitter.h"
#include "arki/emitter/memory.h"
#include "arki/emitter/keys.h"
#include "arki/bbox.h"
#include "arki/libconfig.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include "arki/utils/lua.h"
#endif

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
const char* traits<Area>::type_lua_tag = LUATAG_TYPES ".area";

// Style constants
const unsigned char Area::GRIB;
const unsigned char Area::ODIMH5;
const unsigned char Area::VM2;

Area::Style Area::parseStyle(const std::string& str)
{
    if (str == "GRIB") return GRIB;
    if (str == "ODIMH5") return ODIMH5;
    if (str == "VM2") return VM2;
    throw_consistency_error("parsing Area style", "cannot parse Area style '"+str+"': only GRIB,ODIMH5 is supported");
}

std::string Area::formatStyle(Area::Style s)
{
	switch (s)
	{
		case Area::GRIB: return "GRIB";
		case Area::ODIMH5: return "ODIMH5";
        case Area::VM2: return "VM2";
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
#ifdef HAVE_GEOS
    if (!cached_bbox)
    {
        std::unique_ptr<arki::utils::geos::Geometry> res = BBox::get_singleton()(*this);
        if (res.get())
            cached_bbox = res.release();
    }
#endif
    return cached_bbox;
}

unique_ptr<Area> Area::decode(BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "area");
    switch (s)
    {
        case GRIB:
            return createGRIB(ValueBag::decode(dec));
        case ODIMH5:
            return createODIMH5(ValueBag::decode(dec));
        case VM2:
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
        case Area::GRIB: return createGRIB(ValueBag::parse(inner));
        case Area::ODIMH5: return createODIMH5(ValueBag::parse(inner));
        case Area::VM2: {
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

unique_ptr<Area> Area::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case Area::GRIB: return upcast<Area>(area::GRIB::decodeMapping(val));
        case Area::ODIMH5: return upcast<Area>(area::ODIMH5::decodeMapping(val));
        case Area::VM2: return upcast<Area>(area::VM2::decodeMapping(val));
        default:
            throw_consistency_error("parsing Area", "unknown Area style " + val.get_string());
    }
}

#ifdef HAVE_LUA
static int arkilua_new_grib(lua_State* L)
{
	luaL_checktype(L, 1, LUA_TTABLE);
	ValueBag vals;
	vals.load_lua_table(L);
	area::GRIB::create(vals)->lua_push(L);
	return 1;
}

static int arkilua_new_odimh5(lua_State* L)
{
	luaL_checktype(L, 1, LUA_TTABLE);
	ValueBag vals;
	vals.load_lua_table(L);
	area::ODIMH5::create(vals)->lua_push(L);
	return 1;
}

static int arkilua_new_vm2(lua_State* L)
{
	int type = luaL_checkint(L, 1);
	area::VM2::create(type)->lua_push(L);
	return 1;
}

void Area::lua_loadlib(lua_State* L)
{
	static const struct luaL_Reg lib [] = {
		{ "grib", arkilua_new_grib },
		{ "odimh5", arkilua_new_odimh5 },
		{ "vm2", arkilua_new_vm2 },
		{ NULL, NULL }
	};
    utils::lua::add_global_library(L, "arki_area", lib);
}
#endif

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

Area::Style GRIB::style() const { return Area::GRIB; }

void GRIB::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Area::encodeWithoutEnvelope(enc);
    m_values.encode(enc);
}
std::ostream& GRIB::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "(" << m_values.toString() << ")";
}
void GRIB::serialise_local(Emitter& e, const emitter::Keys& keys, const Formatter* f) const
{
    Area::serialise_local(e, keys, f);
    e.add(keys.area_value);
    m_values.serialise(e);
}
unique_ptr<GRIB> GRIB::decodeMapping(const emitter::memory::Mapping& val)
{
    return GRIB::create(ValueBag::parse(val["va"].get_mapping()));
}
std::string GRIB::exactQuery() const
{
    return "GRIB:" + m_values.toString();
}

const char* GRIB::lua_type_name() const { return "arki.types.area.grib"; }

#ifdef HAVE_LUA
bool GRIB::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "val")
		values().lua_push(L);
	else
		return Area::lua_lookup(L, name);
	return true;
}
#endif

int GRIB::compare_local(const Area& o) const
{
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

Area::Style ODIMH5::style() const { return Area::ODIMH5; }

void ODIMH5::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Area::encodeWithoutEnvelope(enc);
    m_values.encode(enc);
}
std::ostream& ODIMH5::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "(" << m_values.toString() << ")";
}
void ODIMH5::serialise_local(Emitter& e, const emitter::Keys& keys, const Formatter* f) const
{
    Area::serialise_local(e, keys, f);
    e.add(keys.area_value);
    m_values.serialise(e);
}
unique_ptr<ODIMH5> ODIMH5::decodeMapping(const emitter::memory::Mapping& val)
{
    return ODIMH5::create(ValueBag::parse(val["va"].get_mapping()));
}
std::string ODIMH5::exactQuery() const
{
    return "ODIMH5:" + m_values.toString();
}

const char* ODIMH5::lua_type_name() const { return "arki.types.area.odimh5"; }

#ifdef HAVE_LUA
bool ODIMH5::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "val")
		values().lua_push(L);
	else
		return Area::lua_lookup(L, name);
	return true;
}
#endif

int ODIMH5::compare_local(const Area& o) const
{
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

Area::Style VM2::style() const { return Area::VM2; }

void VM2::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Area::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_station_id, 4);
    derived_values().encode(enc);
}

void VM2::encode_for_indexing(BinaryEncoder& enc) const
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
void VM2::serialise_local(Emitter& e, const emitter::Keys& keys, const Formatter* f) const
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

const char* VM2::lua_type_name() const { return "arki.types.area.vm2"; }
#ifdef HAVE_LUA
bool VM2::lua_lookup(lua_State* L, const std::string& name) const
{
    if (name == "id") {
        lua_pushnumber(L, station_id());
#ifdef HAVE_VM2
    } else if (name == "dval") {
        derived_values().lua_push(L);
#endif
    } else {
        return Area::lua_lookup(L, name);
    }
	return true;
}
#endif

int VM2::compare_local(const Area& o) const
{
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
unique_ptr<VM2> VM2::decodeMapping(const emitter::memory::Mapping& val)
{
    return VM2::create(val["id"].want_int("parsing VM2 area station id"));
}

}

void Area::init()
{
    MetadataType::register_type<Area>();
}

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
