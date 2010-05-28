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
 */

#include <wibble/exception.h>
#include <wibble/string.h>
#include <arki/types/origin.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include "config.h"
#include <iomanip>
#include <sstream>
#include <cstring>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_ORIGIN
#define TAG "origin"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;
using namespace wibble;

namespace arki {
namespace types {

// Style constants
//const unsigned char Origin::NONE;
const unsigned char Origin::GRIB1;
const unsigned char Origin::GRIB2;
const unsigned char Origin::BUFR;

// Deprecated
int Origin::getMaxIntCount() { return 5; }

Origin::Style Origin::parseStyle(const std::string& str)
{
	if (str == "GRIB1") return GRIB1;
	if (str == "GRIB2") return GRIB2;
	if (str == "BUFR") return BUFR;
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
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

int Origin::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Origin* v = dynamic_cast<const Origin*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be an Origin, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Origin::compare(const Origin& o) const
{
	return style() - o.style();
}

types::Code Origin::serialisationCode() const { return CODE; }
size_t Origin::serialisationSizeLength() const { return SERSIZELEN; }
std::string Origin::tag() const { return TAG; }
types::Code Origin::typecode() { return CODE; }

void Origin::encodeWithoutEnvelope(Encoder& enc) const
{
	enc.addUInt(style(), 1);
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
		default:
			throw wibble::exception::Consistency("parsing Origin", "unknown Origin style " + formatStyle(style));
	}
}

#ifdef HAVE_LUA
static int arkilua_lookup(lua_State *L)
{
        // querySummary(self, matcher="", summary)
        Item<Origin> item = Type::lua_check(L, 1, "arki.types.origin.").upcast<Origin>();
	const char* sname = lua_tostring(L, 2);
        luaL_argcheck(L, sname != NULL, 2, "`string' expected");

	if (strcmp(sname, "style") == 0)
	{
		string s = Origin::formatStyle(item->style());
		lua_pushlstring(L, s.data(), s.size());
		return 1;
	}

	return item->lua_lookup(L, sname);
}

void Origin::lua_register_methods(lua_State* L) const
{
	Type::lua_register_methods(L);

	static const struct luaL_reg lib [] = {
		{ "__index", arkilua_lookup },
		{ NULL, NULL }
	};
	luaL_register(L, NULL, lib);
}
#endif

namespace origin {

static TypeCache<GRIB1> cache_grib1;
static TypeCache<GRIB2> cache_grib2;
static TypeCache<BUFR> cache_bufr;

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
const char* GRIB1::lua_type_name() const { return "arki.types.origin.grib1"; }

int GRIB1::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "centre")
		lua_pushnumber(L, centre());
	else if (name == "subcentre")
		lua_pushnumber(L, subcentre());
	else if (name == "process")
		lua_pushnumber(L, process());
	else
		lua_pushnil(L);
	return 1;
}

int GRIB1::compare(const Origin& o) const
{
	int res = Origin::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Origin, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int GRIB1::compare(const GRIB1& o) const
{
	if (int res = m_centre - o.m_centre) return res;
	if (int res = m_subcentre - o.m_subcentre) return res;
	return m_process - o.m_process;
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
const char* GRIB2::lua_type_name() const { return "arki.types.origin.grib2"; }

int GRIB2::lua_lookup(lua_State* L, const std::string& name) const
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
		lua_pushnil(L);
	return 1;
}

int GRIB2::compare(const Origin& o) const
{
	int res = Origin::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB2 Origin, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int GRIB2::compare(const GRIB2& o) const
{
	if (int res = m_centre - o.m_centre) return res;
	if (int res = m_subcentre - o.m_subcentre) return res;
	if (int res = m_processtype - o.m_processtype) return res;
	if (int res = m_bgprocessid - o.m_bgprocessid) return res;
	return m_processid - o.m_processid;
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
const char* BUFR::lua_type_name() const { return "arki.types.origin.bufr"; }

int BUFR::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "centre")
		lua_pushnumber(L, centre());
	else if (name == "subcentre")
		lua_pushnumber(L, subcentre());
	else
		lua_pushnil(L);
	return 1;
}

int BUFR::compare(const Origin& o) const
{
	int res = Origin::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a BUFR Origin, but is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int BUFR::compare(const BUFR& o) const
{
	if (int res = m_centre - o.m_centre) return res;
	return m_subcentre - o.m_subcentre;
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

static void debug_interns()
{
	fprintf(stderr, "origin GRIB1: sz %zd reused %zd\n", cache_grib1.size(), cache_grib1.reused());
	fprintf(stderr, "origin GRIB2: sz %zd reused %zd\n", cache_grib2.size(), cache_grib2.reused());
	fprintf(stderr, "origin BUFR: sz %zd reused %zd\n", cache_bufr.size(), cache_bufr.reused());
}

}

static MetadataType originType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Origin::decode),
	(MetadataType::string_decoder)(&Origin::decodeString),
	origin::debug_interns);

}
}
// vim:set ts=4 sw=4:
