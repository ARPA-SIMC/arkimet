/*
 * types/origin - Originating centre metadata item
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include <wibble/exception.h>
#include <wibble/string.h>
#include <arki/types/origin.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <iomanip>
#include <sstream>
#include <cstring>
#include <stdexcept>

#define CODE types::TYPE_ORIGIN
#define TAG "origin"
#define SERSIZELEN 1
#define LUATAG_ORIGIN LUATAG_TYPES ".origin"
#define LUATAG_GRIB1 LUATAG_ORIGIN ".grib1"
#define LUATAG_GRIB2 LUATAG_ORIGIN ".grib2"
#define LUATAG_BUFR LUATAG_ORIGIN ".bufr"
#define LUATAG_ODIMH5 LUATAG_ORIGIN ".odimh5"

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;
using namespace wibble;

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

Item<Origin> Origin::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	ensureSize(len, 1, "Origin");
	Style s = (Style)decodeUInt(buf, 1);
	switch (s)
	{
		case GRIB1:
			ensureSize(len, 4, "Origin");
			return origin::GRIB1::create(decodeUInt(buf+1, 1), decodeUInt(buf+2, 1), decodeUInt(buf+3, 1));
		case GRIB2:
			ensureSize(len, 8, "Origin");
			return origin::GRIB2::create(
				decodeUInt(buf+1, 2), decodeUInt(buf+3, 2), decodeUInt(buf+5, 1),
				decodeUInt(buf+6, 1), decodeUInt(buf+7, 1));
		case BUFR:
			ensureSize(len, 3, "Origin");
			return origin::BUFR::create(decodeUInt(buf+1, 1), decodeUInt(buf+2, 1));
		case ODIMH5:
			{
			ensureSize(len, 4, "Origin");
			Decoder dec(buf, len);

			Style s2 = (Style)dec.popUInt(1, "product"); //saltiamo il primo byte

			uint16_t 	wmosize = dec.popVarint<uint16_t>("ODIMH5 wmo length");
			std::string 	wmo 	= dec.popString(wmosize, "ODIMH5 wmo");

			uint16_t 	radsize = dec.popVarint<uint16_t>("ODIMH5 rad length");
			std::string 	rad 	= dec.popString(radsize, "ODIMH5 rad");

			uint16_t 	plcsize = dec.popVarint<uint16_t>("ODIMH5 plc length");
			std::string 	plc 	= dec.popString(plcsize, "ODIMH5 plc");

			return origin::ODIMH5::create(wmo, rad, plc);
			}
		default:
			throw wibble::exception::Consistency("parsing Origin", "style is " + formatStyle(s) + " but we can only decode GRIB1, GRIB2 and BUFR");
	}
}

Item<Origin> Origin::decodeString(const std::string& val)
{
	string inner;
	Origin::Style style = outerParse<Origin>(val, inner);
	switch (style)
	{
		//case Origin::NONE: return Origin();
		case Origin::GRIB1: {
			NumberList<3> nums(inner, "Origin");
			return origin::GRIB1::create(nums.vals[0], nums.vals[1], nums.vals[2]);
		}
		case Origin::GRIB2: {
			NumberList<5> nums(inner, "Origin");
			return origin::GRIB2::create(nums.vals[0], nums.vals[1], nums.vals[2], nums.vals[3], nums.vals[4]);
		}
		case Origin::BUFR: {
			NumberList<2> nums(inner, "Origin");
			return origin::BUFR::create(nums.vals[0], nums.vals[1]);
		}
		case Origin::ODIMH5: {

			std::vector<std::string> values;

			arki::types::split(inner, values, ",");

			if (values.size()!=3)
				throw std::logic_error("OdimH5 origin has not enogh values");

			values[0] = wibble::str::trim(values[0]);
			values[1] = wibble::str::trim(values[1]);
			values[2] = wibble::str::trim(values[2]);

			return origin::ODIMH5::create(values[0], values[1], values[2]);
		}
		default:
			throw wibble::exception::Consistency("parsing Origin", "unknown Origin style " + formatStyle(style));
	}
}

static int arkilua_new_grib1(lua_State* L)
{
	int centre = luaL_checkint(L, 1);
	int subcentre = luaL_checkint(L, 2);
	int process = luaL_checkint(L, 3);
	Item<> res = origin::GRIB1::create(centre, subcentre, process);
	res->lua_push(L);
	return 1;
}

static int arkilua_new_grib2(lua_State* L)
{
	int centre = luaL_checkint(L, 1);
	int subcentre = luaL_checkint(L, 2);
	int processtype = luaL_checkint(L, 3);
	int bgprocessid = luaL_checkint(L, 4);
	int processid = luaL_checkint(L, 5);
	Item<> res = origin::GRIB2::create(centre, subcentre, processtype, bgprocessid, processid);
	res->lua_push(L);
	return 1;
}

static int arkilua_new_bufr(lua_State* L)
{
	int centre = luaL_checkint(L, 1);
	int subcentre = luaL_checkint(L, 2);
	Item<> res = origin::BUFR::create(centre, subcentre);
	res->lua_push(L);
	return 1;
}

static int arkilua_new_odimh5(lua_State* L)
{
	const char* wmo = luaL_checkstring(L, 1);
	const char* rad = luaL_checkstring(L, 2);
	const char* plc = luaL_checkstring(L, 3);
	Item<> res = origin::ODIMH5::create(wmo, rad, plc);
	res->lua_push(L);
	return 1;
}

void Origin::lua_loadlib(lua_State* L)
{
	static const struct luaL_reg lib [] = {
		{ "grib1", arkilua_new_grib1 },
		{ "grib2", arkilua_new_grib2 },
		{ "bufr", arkilua_new_bufr },
		{ "odimh5", arkilua_new_odimh5 },
		{ NULL, NULL }
	};
	luaL_openlib(L, "arki_origin", lib, 0);
	lua_pop(L, 1);
}

namespace origin {

static TypeCache<GRIB1> cache_grib1;
static TypeCache<GRIB2> cache_grib2;
static TypeCache<BUFR> cache_bufr;
static TypeCache<ODIMH5>	cache_odimh5;

GRIB1::~GRIB1() { /* cache_grib1.uncache(this); */ }

Origin::Style GRIB1::style() const { return Origin::GRIB1; }

void GRIB1::encodeWithoutEnvelope(Encoder& enc) const
{
	Origin::encodeWithoutEnvelope(enc);
	enc.addUInt(m_centre, 1);
	enc.addUInt(m_subcentre, 1);
	enc.addUInt(m_process, 1);
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
std::string GRIB1::exactQuery() const
{
    return str::fmtf("GRIB1,%d,%d,%d", (int)m_centre, (int)m_subcentre, (int)m_process);
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

bool GRIB1::operator==(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;
	return m_centre == v->m_centre && m_subcentre == v->m_subcentre && m_process == v->m_process;
}

Item<GRIB1> GRIB1::create(unsigned char centre, unsigned char subcentre, unsigned char process)
{
	GRIB1* res = new GRIB1;
	res->m_centre = centre;
	res->m_subcentre = subcentre;
	res->m_process = process;
	return cache_grib1.intern(res);
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

void GRIB2::encodeWithoutEnvelope(Encoder& enc) const
{
	Origin::encodeWithoutEnvelope(enc);
	enc.addUInt(m_centre, 2);
	enc.addUInt(m_subcentre, 2);
	enc.addUInt(m_processtype, 1);
	enc.addUInt(m_bgprocessid, 1);
	enc.addUInt(m_processid, 1);
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
std::string GRIB2::exactQuery() const
{
    return str::fmtf("GRIB2,%d,%d,%d,%d,%d", (int)m_centre, (int)m_subcentre, (int)m_processtype, (int)m_bgprocessid, (int)m_processid);
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
bool GRIB2::operator==(const Type& o) const
{
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v) return false;
	return m_centre == v->m_centre && m_subcentre == v->m_subcentre 
		&& m_processtype == v->m_processtype && m_bgprocessid == v->m_bgprocessid
		&& m_processid == v->m_processid;
}

Item<GRIB2> GRIB2::create(
			  unsigned short centre, unsigned short subcentre,
			  unsigned char processtype, unsigned char bgprocessid, unsigned char processid)
{
	GRIB2* res = new GRIB2;
	res->m_centre = centre;
	res->m_subcentre = subcentre;
	res->m_processtype = processtype;
	res->m_bgprocessid = bgprocessid;
	res->m_processid = processid;
	return cache_grib2.intern(res);
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

void BUFR::encodeWithoutEnvelope(Encoder& enc) const
{
	Origin::encodeWithoutEnvelope(enc);
	enc.addUInt(m_centre, 1);
	enc.addUInt(m_subcentre, 1);
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
std::string BUFR::exactQuery() const
{
    return str::fmtf("BUFR,%d,%d", (int)m_centre, (int)m_subcentre);
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
bool BUFR::operator==(const Type& o) const
{
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v) return false;
	return m_centre == v->m_centre && m_subcentre == v->m_subcentre;
}

Item<BUFR> BUFR::create(unsigned char centre, unsigned char subcentre)
{
	BUFR* res = new BUFR;
	res->m_centre = centre;
	res->m_subcentre = subcentre;
	return cache_bufr.intern(res);
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

void ODIMH5::encodeWithoutEnvelope(Encoder& enc) const
{
	Origin::encodeWithoutEnvelope(enc);
	enc.addVarint(m_WMO.size());
	enc.addString(m_WMO);
	enc.addVarint(m_RAD.size());
	enc.addString(m_RAD);
	enc.addVarint(m_PLC.size());
	enc.addString(m_PLC);
}
std::ostream& ODIMH5::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
	         << m_WMO << ", "
		 << m_RAD << ", "
		 << m_PLC << ")";
}
std::string ODIMH5::exactQuery() const
{
    return str::fmtf("ODIMH5,%s,%s,%s", m_WMO.c_str(), m_RAD.c_str(), m_PLC.c_str());
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

bool ODIMH5::operator==(const Type& o) const
{
	const ODIMH5* v = dynamic_cast<const ODIMH5*>(&o);
	if (!v) return false;
	return m_WMO == v->m_WMO && m_RAD == v->m_RAD && m_PLC == v->m_PLC;
}

Item<ODIMH5> ODIMH5::create(const std::string& wmo, const std::string& rad, const std::string& plc)
{
	ODIMH5* res = new ODIMH5;
	res->m_WMO = wmo;
	res->m_RAD = rad;
	res->m_PLC = plc;
	return cache_odimh5.intern(res);
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


static void debug_interns()
{
	fprintf(stderr, "origin GRIB1: sz %zd reused %zd\n", cache_grib1.size(), cache_grib1.reused());
	fprintf(stderr, "origin GRIB2: sz %zd reused %zd\n", cache_grib2.size(), cache_grib2.reused());
	fprintf(stderr, "origin BUFR: sz %zd reused %zd\n", cache_bufr.size(), cache_bufr.reused());
	fprintf(stderr, "origin ODIMH5: sz %zd reused %zd\n", cache_odimh5.size(), cache_odimh5.reused());
}

}

static MetadataType originType = MetadataType::create<Origin>(origin::debug_interns);

}
}

#include <arki/types.tcc>

// vim:set ts=4 sw=4:
