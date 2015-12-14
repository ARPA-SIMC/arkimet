#include <arki/wibble/exception.h>
#include <arki/types/origin.h>
#include <arki/types/utils.h>
#include <arki/binary.h>
#include <arki/utils/string.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include <arki/utils/lua.h>
#include <iomanip>
#include <sstream>
#include <cstring>
#include <stdexcept>

#define CODE TYPE_ORIGIN
#define TAG "origin"
#define SERSIZELEN 1
#define LUATAG_ORIGIN LUATAG_TYPES ".origin"
#define LUATAG_GRIB1 LUATAG_ORIGIN ".grib1"
#define LUATAG_GRIB2 LUATAG_ORIGIN ".grib2"
#define LUATAG_BUFR LUATAG_ORIGIN ".bufr"
#define LUATAG_ODIMH5 LUATAG_ORIGIN ".odimh5"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Origin>::type_tag = TAG;
const types::Code traits<Origin>::type_code = CODE;
const size_t traits<Origin>::type_sersize_bytes = SERSIZELEN;
const char* traits<Origin>::type_lua_tag = LUATAG_ORIGIN;

// Style constants
//const unsigned char Origin::NONE;
const unsigned char Origin::GRIB1;
const unsigned char Origin::GRIB2;
const unsigned char Origin::BUFR;
const unsigned char Origin::ODIMH5;


// Deprecated
int Origin::getMaxIntCount() { return 5; }

Origin::Style Origin::parseStyle(const std::string& str)
{
	if (str == "GRIB1") return GRIB1;
	if (str == "GRIB2") return GRIB2;
	if (str == "BUFR") return BUFR;
	if (str == "ODIMH5") 	return ODIMH5;
	throw wibble::exception::Consistency("parsing Origin style", "cannot parse Origin style '"+str+"': only GRIB1, GRIB2 and BUFR are supported");
}

std::string Origin::formatStyle(Origin::Style s)
{
	switch (s)
	{
		//case Origin::NONE: return "NONE";
		case Origin::GRIB1: return "GRIB1";
		case Origin::GRIB2: return "GRIB2";
		case Origin::BUFR: return "BUFR";
		case Origin::ODIMH5: 	return "ODIMH5";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

unique_ptr<Origin> Origin::decode(BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "origin style");
    switch (s)
    {
        case GRIB1:
        {
            uint8_t ce = dec.pop_uint(1, "GRIB1 origin centre");
            uint8_t sc = dec.pop_uint(1, "GRIB1 origin subcentre");
            uint8_t pr = dec.pop_uint(1, "GRIB1 origin process");
            return upcast<Origin>(origin::GRIB1::create(ce, sc, pr));
        }
        case GRIB2:
        {
            unsigned short ce = dec.pop_uint(2, "GRIB2 origin centre");
            unsigned short sc = dec.pop_uint(2, "GRIB2 origin subcentre");
            unsigned char pt = dec.pop_uint(1, "GRIB2 origin process type");
            unsigned char bgid = dec.pop_uint(1, "GRIB2 origin background process ID");
            unsigned char prid = dec.pop_uint(1, "GRIB2 origin process ID");
            return upcast<Origin>(origin::GRIB2::create(ce, sc, pt, bgid, prid));
        }
        case BUFR:
        {
            uint8_t ce = dec.pop_uint(1, "BUFR origin centre");
            uint8_t sc = dec.pop_uint(1, "BUFR origin subcentre");
            return upcast<Origin>(origin::BUFR::create(ce, sc));
        }
        case ODIMH5:
        {
            uint16_t wmosize = dec.pop_varint<uint16_t>("ODIMH5 wmo length");
            std::string wmo = dec.pop_string(wmosize, "ODIMH5 wmo");

            uint16_t radsize = dec.pop_varint<uint16_t>("ODIMH5 rad length");
            std::string rad = dec.pop_string(radsize, "ODIMH5 rad");

            uint16_t plcsize = dec.pop_varint<uint16_t>("ODIMH5 plc length");
            std::string plc = dec.pop_string(plcsize, "ODIMH5 plc");

            return upcast<Origin>(origin::ODIMH5::create(wmo, rad, plc));
        }
		default:
			throw wibble::exception::Consistency("parsing Origin", "style is " + formatStyle(s) + " but we can only decode GRIB1, GRIB2 and BUFR");
	}
}

unique_ptr<Origin> Origin::decodeString(const std::string& val)
{
	string inner;
	Origin::Style style = outerParse<Origin>(val, inner);
	switch (style)
	{
        //case Origin::NONE: return Origin();
        case Origin::GRIB1: {
            NumberList<3> nums(inner, "Origin");
            return upcast<Origin>(origin::GRIB1::create(nums.vals[0], nums.vals[1], nums.vals[2]));
        }
        case Origin::GRIB2: {
            NumberList<5> nums(inner, "Origin");
            return upcast<Origin>(origin::GRIB2::create(nums.vals[0], nums.vals[1], nums.vals[2], nums.vals[3], nums.vals[4]));
        }
        case Origin::BUFR: {
            NumberList<2> nums(inner, "Origin");
            return upcast<Origin>(origin::BUFR::create(nums.vals[0], nums.vals[1]));
        }
        case Origin::ODIMH5: {
            std::vector<std::string> values;
            str::Split split(inner, ",");
            for (str::Split::const_iterator i = split.begin(); i != split.end(); ++i)
                values.push_back(*i);

            if (values.size()!=3)
                throw std::logic_error("OdimH5 origin has not enough values");

            values[0] = str::strip(values[0]);
            values[1] = str::strip(values[1]);
            values[2] = str::strip(values[2]);

            return upcast<Origin>(origin::ODIMH5::create(values[0], values[1], values[2]));
        }
		default:
			throw wibble::exception::Consistency("parsing Origin", "unknown Origin style " + formatStyle(style));
	}
}

unique_ptr<Origin> Origin::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case Origin::GRIB1: return upcast<Origin>(origin::GRIB1::decodeMapping(val));
        case Origin::GRIB2: return upcast<Origin>(origin::GRIB2::decodeMapping(val));
        case Origin::BUFR: return upcast<Origin>(origin::BUFR::decodeMapping(val));
        case Origin::ODIMH5: return upcast<Origin>(origin::ODIMH5::decodeMapping(val));
        default:
            throw wibble::exception::Consistency("parsing Origin", "unknown Origin style " + val.get_string());
    }
}

static int arkilua_new_grib1(lua_State* L)
{
	int centre = luaL_checkint(L, 1);
	int subcentre = luaL_checkint(L, 2);
	int process = luaL_checkint(L, 3);
    origin::GRIB1::create(centre, subcentre, process)->lua_push(L);
    return 1;
}

static int arkilua_new_grib2(lua_State* L)
{
	int centre = luaL_checkint(L, 1);
	int subcentre = luaL_checkint(L, 2);
	int processtype = luaL_checkint(L, 3);
	int bgprocessid = luaL_checkint(L, 4);
	int processid = luaL_checkint(L, 5);
    origin::GRIB2::create(centre, subcentre, processtype, bgprocessid, processid)->lua_push(L);
    return 1;
}

static int arkilua_new_bufr(lua_State* L)
{
	int centre = luaL_checkint(L, 1);
	int subcentre = luaL_checkint(L, 2);
    origin::BUFR::create(centre, subcentre)->lua_push(L);
    return 1;
}

static int arkilua_new_odimh5(lua_State* L)
{
	const char* wmo = luaL_checkstring(L, 1);
	const char* rad = luaL_checkstring(L, 2);
	const char* plc = luaL_checkstring(L, 3);
    origin::ODIMH5::create(wmo, rad, plc)->lua_push(L);
    return 1;
}

void Origin::lua_loadlib(lua_State* L)
{
	static const struct luaL_Reg lib [] = {
		{ "grib1", arkilua_new_grib1 },
		{ "grib2", arkilua_new_grib2 },
		{ "bufr", arkilua_new_bufr },
		{ "odimh5", arkilua_new_odimh5 },
		{ NULL, NULL }
	};
    utils::lua::add_global_library(L, "arki_origin", lib);
}

unique_ptr<Origin> Origin::createGRIB1(unsigned char centre, unsigned char subcentre, unsigned char process)
{
    return upcast<Origin>(origin::GRIB1::create(centre, subcentre, process));
}
unique_ptr<Origin> Origin::createGRIB2(unsigned short centre, unsigned short subcentre,
                                             unsigned char processtype, unsigned char bgprocessid, unsigned char processid)
{
    return upcast<Origin>(origin::GRIB2::create(centre, subcentre, processtype, bgprocessid, processid));
}
unique_ptr<Origin> Origin::createBUFR(unsigned char centre, unsigned char subcentre)
{
    return upcast<Origin>(origin::BUFR::create(centre, subcentre));
}
unique_ptr<Origin> Origin::createODIMH5(const std::string& wmo, const std::string& rad, const std::string& plc)
{
    return upcast<Origin>(origin::ODIMH5::create(wmo, rad, plc));
}

namespace origin {

GRIB1::~GRIB1() { /* cache_grib1.uncache(this); */ }

Origin::Style GRIB1::style() const { return Origin::GRIB1; }

void GRIB1::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Origin::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_centre, 1);
    enc.add_unsigned(m_subcentre, 1);
    enc.add_unsigned(m_process, 1);
}
std::ostream& GRIB1::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
		 << setfill('0')
	         << setw(3) << (int)m_centre << ", "
		 << setw(3) << (int)m_subcentre << ", "
		 << setw(3) << (int)m_process
		 << setfill(' ')
		 << ")";
}
void GRIB1::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Origin::serialiseLocal(e, f);
    e.add("ce", m_centre);
    e.add("sc", m_subcentre);
    e.add("pr", m_process);
}
unique_ptr<GRIB1> GRIB1::decodeMapping(const emitter::memory::Mapping& val)
{
    return GRIB1::create(
            val["ce"].want_int("parsing GRIB1 origin centre"),
            val["sc"].want_int("parsing GRIB1 origin subcentre"),
            val["pr"].want_int("parsing GRIB1 origin process"));
}
std::string GRIB1::exactQuery() const
{
    char buf[64];
    snprintf(buf, 64, "GRIB1,%d,%d,%d", (int)m_centre, (int)m_subcentre, (int)m_process);
    return buf;
}
const char* GRIB1::lua_type_name() const { return LUATAG_GRIB1; }

bool GRIB1::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "centre")
		lua_pushnumber(L, centre());
	else if (name == "subcentre")
		lua_pushnumber(L, subcentre());
	else if (name == "process")
		lua_pushnumber(L, process());
	else
		return Origin::lua_lookup(L, name);
	return true;
}

int GRIB1::compare_local(const Origin& o) const
{
	// We should be the same kind, so upcast
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Origin, but is a ") + typeid(&o).name() + " instead");

	if (int res = m_centre - v->m_centre) return res;
	if (int res = m_subcentre - v->m_subcentre) return res;
	return m_process - v->m_process;
}

bool GRIB1::equals(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;
	return m_centre == v->m_centre && m_subcentre == v->m_subcentre && m_process == v->m_process;
}

GRIB1* GRIB1::clone() const
{
    GRIB1* res = new GRIB1;
    res->m_centre = m_centre;
    res->m_subcentre = m_subcentre;
    res->m_process = m_process;
    return res;
}

unique_ptr<GRIB1> GRIB1::create(unsigned char centre, unsigned char subcentre, unsigned char process)
{
    GRIB1* res = new GRIB1;
    res->m_centre = centre;
    res->m_subcentre = subcentre;
    res->m_process = process;
    return unique_ptr<GRIB1>(res);
}

std::vector<int> GRIB1::toIntVector() const
{
	vector<int> res;
	res.push_back(m_centre);
	res.push_back(m_subcentre);
	res.push_back(m_process);
	return res;
}

GRIB2::~GRIB2() { /* cache_grib2.uncache(this); */ }

Origin::Style GRIB2::style() const { return Origin::GRIB2; }

void GRIB2::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Origin::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_centre, 2);
    enc.add_unsigned(m_subcentre, 2);
    enc.add_unsigned(m_processtype, 1);
    enc.add_unsigned(m_bgprocessid, 1);
    enc.add_unsigned(m_processid, 1);
}
std::ostream& GRIB2::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
		 << setfill('0')
		 << setw(5) << (int)m_centre << ", "
		 << setw(5) << (int)m_subcentre << ", "
		 << setw(3) << (int)m_processtype << ", "
		 << setw(3) << (int)m_bgprocessid << ", "
		 << setw(3) << (int)m_processid
		 << setfill(' ')
		 << ")";
}
void GRIB2::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Origin::serialiseLocal(e, f);
    e.add("ce", m_centre);
    e.add("sc", m_subcentre);
    e.add("pt", m_processtype);
    e.add("bi", m_bgprocessid);
    e.add("pi", m_processid);
}
unique_ptr<GRIB2> GRIB2::decodeMapping(const emitter::memory::Mapping& val)
{
    return GRIB2::create(
            val["ce"].want_int("parsing GRIB1 origin centre"),
            val["sc"].want_int("parsing GRIB1 origin subcentre"),
            val["pt"].want_int("parsing GRIB1 origin process type"),
            val["bi"].want_int("parsing GRIB1 origin bg process id"),
            val["pi"].want_int("parsing GRIB1 origin process id"));
}
std::string GRIB2::exactQuery() const
{
    char buf[64];
    snprintf(buf, 64, "GRIB2,%d,%d,%d,%d,%d", (int)m_centre, (int)m_subcentre, (int)m_processtype, (int)m_bgprocessid, (int)m_processid);
    return buf;
}
const char* GRIB2::lua_type_name() const { return LUATAG_GRIB2; }

bool GRIB2::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "centre")
		lua_pushnumber(L, centre());
	else if (name == "subcentre")
		lua_pushnumber(L, subcentre());
	else if (name == "processtype")
		lua_pushnumber(L, processtype());
	else if (name == "bgprocessid")
		lua_pushnumber(L, bgprocessid());
	else if (name == "processid")
		lua_pushnumber(L, processid());
	else
		return Origin::lua_lookup(L, name);
	return true;
}

int GRIB2::compare_local(const Origin& o) const
{
	// We should be the same kind, so upcast
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB2 Origin, but is a ") + typeid(&o).name() + " instead");

	if (int res = m_centre - v->m_centre) return res;
	if (int res = m_subcentre - v->m_subcentre) return res;
	if (int res = m_processtype - v->m_processtype) return res;
	if (int res = m_bgprocessid - v->m_bgprocessid) return res;
	return m_processid - v->m_processid;
}
bool GRIB2::equals(const Type& o) const
{
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v) return false;
	return m_centre == v->m_centre && m_subcentre == v->m_subcentre 
		&& m_processtype == v->m_processtype && m_bgprocessid == v->m_bgprocessid
		&& m_processid == v->m_processid;
}

GRIB2* GRIB2::clone() const
{
    GRIB2* res = new GRIB2;
    res->m_centre = m_centre;
    res->m_subcentre = m_subcentre;
    res->m_processtype = m_processtype;
    res->m_bgprocessid = m_bgprocessid;
    res->m_processid = m_processid;
    return res;
}

unique_ptr<GRIB2> GRIB2::create(
			  unsigned short centre, unsigned short subcentre,
			  unsigned char processtype, unsigned char bgprocessid, unsigned char processid)
{
    GRIB2* res = new GRIB2;
    res->m_centre = centre;
    res->m_subcentre = subcentre;
    res->m_processtype = processtype;
    res->m_bgprocessid = bgprocessid;
    res->m_processid = processid;
    return unique_ptr<GRIB2>(res);
}

std::vector<int> GRIB2::toIntVector() const
{
	vector<int> res;
	res.push_back(m_centre);
	res.push_back(m_subcentre);
	res.push_back(m_processtype);
	res.push_back(m_bgprocessid);
	res.push_back(m_processid);
	return res;
}

BUFR::~BUFR() { /* cache_bufr.uncache(this); */ }

Origin::Style BUFR::style() const { return Origin::BUFR; }

void BUFR::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Origin::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_centre, 1);
    enc.add_unsigned(m_subcentre, 1);
}
std::ostream& BUFR::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
		 << setfill('0')
	         << setw(3) << (int)m_centre << ", "
		 << setw(3) << (int)m_subcentre
		 << setfill(' ')
		 << ")";
}
void BUFR::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Origin::serialiseLocal(e, f);
    e.add("ce", m_centre);
    e.add("sc", m_subcentre);
}
unique_ptr<BUFR> BUFR::decodeMapping(const emitter::memory::Mapping& val)
{
    return BUFR::create(
            val["ce"].want_int("parsing BUFR origin centre"),
            val["sc"].want_int("parsing BUFR origin subcentre"));
}
std::string BUFR::exactQuery() const
{
    char buf[32];
    snprintf(buf, 32, "BUFR,%d,%d", (int)m_centre, (int)m_subcentre);
    return buf;
}
const char* BUFR::lua_type_name() const { return LUATAG_BUFR; }

bool BUFR::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "centre")
		lua_pushnumber(L, centre());
	else if (name == "subcentre")
		lua_pushnumber(L, subcentre());
	else
		return Origin::lua_lookup(L, name);
	return true;
}

int BUFR::compare_local(const Origin& o) const
{
	// We should be the same kind, so upcast
	// TODO: if upcast fails, we might still be ok as we support comparison
	// between origins of different style: we do need a two-phase upcast
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a BUFR Origin, but is a ") + typeid(&o).name() + " instead");

	if (int res = m_centre - v->m_centre) return res;
	return m_subcentre - v->m_subcentre;
}
bool BUFR::equals(const Type& o) const
{
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v) return false;
	return m_centre == v->m_centre && m_subcentre == v->m_subcentre;
}

BUFR* BUFR::clone() const
{
    BUFR* res = new BUFR;
    res->m_centre = m_centre;
    res->m_subcentre = m_subcentre;
    return res;
}

unique_ptr<BUFR> BUFR::create(unsigned char centre, unsigned char subcentre)
{
    BUFR* res = new BUFR;
    res->m_centre = centre;
    res->m_subcentre = subcentre;
    return unique_ptr<BUFR>(res);
}

std::vector<int> BUFR::toIntVector() const
{
	vector<int> res;
	res.push_back(m_centre);
	res.push_back(m_subcentre);
	return res;
}

ODIMH5::~ODIMH5() { /* cache_grib1.uncache(this); */ }

Origin::Style ODIMH5::style() const { return Origin::ODIMH5; }

void ODIMH5::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Origin::encodeWithoutEnvelope(enc);
    enc.add_varint(m_WMO.size());
    enc.add_raw(m_WMO);
    enc.add_varint(m_RAD.size());
    enc.add_raw(m_RAD);
    enc.add_varint(m_PLC.size());
    enc.add_raw(m_PLC);
}
std::ostream& ODIMH5::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
	         << m_WMO << ", "
		 << m_RAD << ", "
		 << m_PLC << ")";
}
void ODIMH5::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Origin::serialiseLocal(e, f);
    e.add("wmo", m_WMO);
    e.add("rad", m_RAD);
    e.add("plc", m_PLC);
}
unique_ptr<ODIMH5> ODIMH5::decodeMapping(const emitter::memory::Mapping& val)
{
    return ODIMH5::create(
            val["wmo"].want_string("parsing ODIMH5 origin WMO"),
            val["rad"].want_string("parsing ODIMH5 origin RAD"),
            val["plc"].want_string("parsing ODIMH5 origin PLC"));
}
std::string ODIMH5::exactQuery() const
{
    stringstream res;
    res << "ODIMH5," << m_WMO << "," << m_RAD << "," << m_PLC;
    return res.str();
}
const char* ODIMH5::lua_type_name() const { return LUATAG_ODIMH5; }

bool ODIMH5::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "wmo")
		lua_pushlstring(L, m_WMO.data(), m_WMO.size());
	else if (name == "rad")                
		lua_pushlstring(L, m_RAD.data(), m_RAD.size());
	else if (name == "plc")                
		lua_pushlstring(L, m_PLC.data(), m_PLC.size());
	else
		return Origin::lua_lookup(L, name);
	return true;
}

int ODIMH5::compare_local(const Origin& o) const
{
	// We should be the same kind, so upcast
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Origin, but is a ") + typeid(&o).name() + " instead");

	if (int res = m_WMO.compare(v->m_WMO)) return res;
	if (int res = m_RAD.compare(v->m_RAD)) return res;
	return m_PLC.compare(v->m_PLC);
}

bool ODIMH5::equals(const Type& o) const
{
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v) return false;
	return m_WMO == v->m_WMO && m_RAD == v->m_RAD && m_PLC == v->m_PLC;
}

ODIMH5* ODIMH5::clone() const
{
    ODIMH5* res = new ODIMH5;
    res->m_WMO = m_WMO;
    res->m_RAD = m_RAD;
    res->m_PLC = m_PLC;
    return res;
}

unique_ptr<ODIMH5> ODIMH5::create(const std::string& wmo, const std::string& rad, const std::string& plc)
{
    ODIMH5* res = new ODIMH5;
    res->m_WMO = wmo;
    res->m_RAD = rad;
    res->m_PLC = plc;
    return unique_ptr<ODIMH5>(res);
}

std::vector<int> ODIMH5::toIntVector() const
{
	vector<int> res;

/* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */
/* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */
/* XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX */

	//res.push_back(m_centre);
	//res.push_back(m_subcentre);
	//res.push_back(m_process);
	return res;
}

}

void Origin::init()
{
    MetadataType::register_type<Origin>();
}

}
}

#include <arki/types.tcc>
