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
int Origin::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Origin reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const Origin& v = **(const Origin**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "style")
	{
		string s = Origin::formatStyle(v.style());
		lua_pushlstring(L, s.data(), s.size());
		return 1;
	}
	else if (name == "grib1" && v.style() == Origin::GRIB1)
	{
		const origin::GRIB1* v1 = v.upcast<origin::GRIB1>();
		lua_pushnumber(L, v1->centre);
		lua_pushnumber(L, v1->subcentre);
		lua_pushnumber(L, v1->process);
		return 3;
	}
	else if (name == "grib2" && v.style() == Origin::GRIB2)
	{
		const origin::GRIB2* v1 = v.upcast<origin::GRIB2>();
		lua_pushnumber(L, v1->centre);
		lua_pushnumber(L, v1->subcentre);
		lua_pushnumber(L, v1->processtype);
		lua_pushnumber(L, v1->bgprocessid);
		lua_pushnumber(L, v1->processid);
		return 5;
	}
	else if (name == "bufr" && v.style() == Origin::BUFR)
	{
		const origin::GRIB2* v1 = v.upcast<origin::GRIB2>();
		lua_pushnumber(L, v1->centre);
		lua_pushnumber(L, v1->subcentre);
		return 2;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_origin(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Origin::lua_lookup, 2);
	return 1;
}
void Origin::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Origin** s = (const Origin**)lua_newuserdata(L, sizeof(const Origin*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_origin);
		lua_settable(L, -3);
		/* set the __tostring metamethod */
		lua_pushstring(L, "__tostring");
		lua_pushcfunction(L, utils::lua::tostring_arkitype<Origin>);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif

namespace origin {

Origin::Style GRIB1::style() const { return Origin::GRIB1; }

void GRIB1::encodeWithoutEnvelope(Encoder& enc) const
{
	Origin::encodeWithoutEnvelope(enc);
	enc.addUInt(centre, 1);
	enc.addUInt(subcentre, 1);
	enc.addUInt(process, 1);
}
std::ostream& GRIB1::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << setfill('0')
	         << setw(3) << (int)centre << ", " << setw(3) << (int)subcentre << ", " << setw(3) << (int)process
			 << setfill(' ')
			 << ")";
}
std::string GRIB1::exactQuery() const
{
    return str::fmtf("GRIB1,%d,%d,%d", (int)centre, (int)subcentre, (int)process);
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
	if (int res = centre - o.centre) return res;
	if (int res = subcentre - o.subcentre) return res;
	return process - o.process;
}

bool GRIB1::operator==(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;
	return centre == v->centre && subcentre == v->subcentre && process == v->process;
}

Item<GRIB1> GRIB1::create(unsigned char centre, unsigned char subcentre, unsigned char process)
{
	GRIB1* res = new GRIB1;
	res->centre = centre;
	res->subcentre = subcentre;
	res->process = process;
	return res;
}

std::vector<int> GRIB1::toIntVector() const
{
	vector<int> res;
	res.push_back(centre);
	res.push_back(subcentre);
	res.push_back(process);
	return res;
}


Origin::Style GRIB2::style() const { return Origin::GRIB2; }

void GRIB2::encodeWithoutEnvelope(Encoder& enc) const
{
	Origin::encodeWithoutEnvelope(enc);
	enc.addUInt(centre, 2);
	enc.addUInt(subcentre, 2);
	enc.addUInt(processtype, 1);
	enc.addUInt(bgprocessid, 1);
	enc.addUInt(processid, 1);
}
std::ostream& GRIB2::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << setfill('0')
		     << setw(5) << (int)centre << ", " << setw(5) << (int)subcentre << ", " << setw(3) << (int)processtype << ", "
			 << setw(3) << (int)bgprocessid << ", " << setw(3) << (int)processid
			 << setfill(' ')
			 << ")";
}
std::string GRIB2::exactQuery() const
{
    return str::fmtf("GRIB2,%d,%d,%d,%d,%d", (int)centre, (int)subcentre, (int)processtype, (int)bgprocessid, (int)processid);
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
	if (int res = centre - o.centre) return res;
	if (int res = subcentre - o.subcentre) return res;
	if (int res = processtype - o.processtype) return res;
	if (int res = bgprocessid - o.bgprocessid) return res;
	return processid - o.processid;
}
bool GRIB2::operator==(const Type& o) const
{
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v) return false;
	return centre == v->centre && subcentre == v->subcentre 
		&& processtype == v->processtype && bgprocessid == v->bgprocessid
		&& processid == v->processid;
}

Item<GRIB2> GRIB2::create(
			  unsigned short centre, unsigned short subcentre,
			  unsigned char processtype, unsigned char bgprocessid, unsigned char processid)
{
	GRIB2* res = new GRIB2;
	res->centre = centre;
	res->subcentre = subcentre;
	res->processtype = processtype;
	res->bgprocessid = bgprocessid;
	res->processid = processid;
	return res;
}

std::vector<int> GRIB2::toIntVector() const
{
	vector<int> res;
	res.push_back(centre);
	res.push_back(subcentre);
	res.push_back(processtype);
	res.push_back(bgprocessid);
	res.push_back(processid);
	return res;
}


Origin::Style BUFR::style() const { return Origin::BUFR; }

void BUFR::encodeWithoutEnvelope(Encoder& enc) const
{
	Origin::encodeWithoutEnvelope(enc);
	enc.addUInt(centre, 1);
	enc.addUInt(subcentre, 1);
}
std::ostream& BUFR::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << setfill('0')
	         << setw(3) << (int)centre << ", " << setw(3) << (int)subcentre
			 << setfill(' ')
			 << ")";
}
std::string BUFR::exactQuery() const
{
    return str::fmtf("BUFR,%d,%d", (int)centre, (int)subcentre);
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
	if (int res = centre - o.centre) return res;
	return subcentre - o.subcentre;
}
bool BUFR::operator==(const Type& o) const
{
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v) return false;
	return centre == v->centre && subcentre == v->subcentre;
}

Item<BUFR> BUFR::create(unsigned char centre, unsigned char subcentre)
{
	BUFR* res = new BUFR;
	res->centre = centre;
	res->subcentre = subcentre;
	return res;
}

std::vector<int> BUFR::toIntVector() const
{
	vector<int> res;
	res.push_back(centre);
	res.push_back(subcentre);
	return res;
}

}

static MetadataType originType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Origin::decode),
	(MetadataType::string_decoder)(&Origin::decodeString));

}
}
// vim:set ts=4 sw=4:
