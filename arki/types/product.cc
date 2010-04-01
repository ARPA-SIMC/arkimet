/*
 * types/product - Product metadata item
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
#include <arki/types/product.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <sstream>
#include <iomanip>
#include "config.h"

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_PRODUCT
#define TAG "product"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;
using namespace wibble;

namespace arki {
namespace types {

// Style constants
//const unsigned char Product::NONE;
const unsigned char Product::GRIB1;
const unsigned char Product::GRIB2;
const unsigned char Product::BUFR;

// Deprecated
int Product::getMaxIntCount() { return 4; }

Product::Style Product::parseStyle(const std::string& str)
{
	if (str == "GRIB1") return GRIB1;
	if (str == "GRIB2") return GRIB2;
	if (str == "BUFR") return BUFR;
	throw wibble::exception::Consistency("parsing Product style", "cannot parse Product style '"+str+"': only GRIB1, GRIB2 and BUFR are supported");
}

std::string Product::formatStyle(Product::Style s)
{
	switch (s)
	{
		case Product::GRIB1: return "GRIB1";
		case Product::GRIB2: return "GRIB2";
		case Product::BUFR: return "BUFR";
		default: 
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

int Product::compare(const Type& o) const
{
	int res = Type::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const Product* v = dynamic_cast<const Product*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a Product, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int Product::compare(const Product& o) const
{
	return style() - o.style();
}

types::Code Product::serialisationCode() const { return CODE; }
size_t Product::serialisationSizeLength() const { return SERSIZELEN; }
std::string Product::tag() const { return TAG; }
types::Code Product::typecode() { return CODE; }

void Product::encodeWithoutEnvelope(Encoder& enc) const
{
	enc.addUInt(style(), 1);
}

Item<Product> Product::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	ensureSize(len, 1, "Product");
	Style s = (Style)decodeUInt(buf, 1);
	switch (s)
	{
		case GRIB1:
			ensureSize(len, 4, "Product");
			return product::GRIB1::create(decodeUInt(buf+1, 1), decodeUInt(buf+2, 1), decodeUInt(buf+3, 1));
		case GRIB2:
			ensureSize(len, 6, "Product");
			return product::GRIB2::create(decodeUInt(buf+1, 2), decodeUInt(buf+3, 1), decodeUInt(buf+4, 1), decodeUInt(buf+5, 1));
		case BUFR:
			ensureSize(len, 4, "Product");
			return product::BUFR::create(decodeUInt(buf+1, 1), decodeUInt(buf+2, 1), decodeUInt(buf+3, 1));
		default:
			throw wibble::exception::Consistency("parsing Product", "style is " + formatStyle(s) + "but we can only decode GRIB1, GRIB2 and BUFR");
	}
}

Item<Product> Product::decodeString(const std::string& val)
{
	string inner;
	Product::Style style = outerParse<Product>(val, inner);
	switch (style)
	{
		//case Product::NONE: return Product();
		case Product::GRIB1: {
			NumberList<3> nums(inner, "Product");
			return product::GRIB1::create(nums.vals[0], nums.vals[1], nums.vals[2]);
		}
		case Product::GRIB2: {
			NumberList<4> nums(inner, "Product");
			return product::GRIB2::create(nums.vals[0], nums.vals[1], nums.vals[2], nums.vals[3]);
		}
		case Product::BUFR: {
			NumberList<3> nums(inner, "Product");
			return product::BUFR::create(nums.vals[0], nums.vals[1], nums.vals[2]);
		}
		default:
			throw wibble::exception::Consistency("parsing Product", "unknown Product style " + formatStyle(style));
	}
}

#ifdef HAVE_LUA
int Product::lua_lookup(lua_State* L)
{
	int udataidx = lua_upvalueindex(1);
	int keyidx = lua_upvalueindex(2);
	// Fetch the Origin reference from the userdata value
	luaL_checkudata(L, udataidx, "arki_" TAG);
	void* userdata = lua_touserdata(L, udataidx);
	const Product& v = **(const Product**)userdata;

	// Get the name to lookup from lua
	// (we use 2 because 1 is the table, since we are a __index function)
	luaL_checkstring(L, keyidx);
	string name = lua_tostring(L, keyidx);

	if (name == "style")
	{
		string s = Product::formatStyle(v.style());
		lua_pushlstring(L, s.data(), s.size());
		return 1;
	}
	else if (name == "grib1" && v.style() == Product::GRIB1)
	{
		const product::GRIB1* v1 = v.upcast<product::GRIB1>();
		lua_pushnumber(L, v1->origin());
		lua_pushnumber(L, v1->table());
		lua_pushnumber(L, v1->product());
		return 3;
	}
	else if (name == "grib2" && v.style() == Product::GRIB2)
	{
		const product::GRIB2* v1 = v.upcast<product::GRIB2>();
		lua_pushnumber(L, v1->centre());
		lua_pushnumber(L, v1->discipline());
		lua_pushnumber(L, v1->category());
		lua_pushnumber(L, v1->number());
		return 4;
	}
	else if (name == "bufr" && v.style() == Product::BUFR)
	{
		const product::BUFR* v1 = v.upcast<product::BUFR>();
		lua_pushnumber(L, v1->type());
		lua_pushnumber(L, v1->subtype());
		lua_pushnumber(L, v1->localsubtype());
		return 3;
	}
	else
	{
		lua_pushnil(L);
		return 1;
	}
}
static int arkilua_lookup_product(lua_State* L)
{
	// build a closure with the parameters passed, and return it
	lua_pushcclosure(L, Product::lua_lookup, 2);
	return 1;
}
void Product::lua_push(lua_State* L) const
{
	// The 'grib' object is a userdata that holds a pointer to this Grib structure
	const Product** s = (const Product**)lua_newuserdata(L, sizeof(const Product*));
	*s = this;

	// Set the metatable for the userdata
	if (luaL_newmetatable(L, "arki_" TAG));
	{
		// If the metatable wasn't previously created, create it now
		// Set the __index metamethod to the lookup function
		lua_pushstring(L, "__index");
		lua_pushcfunction(L, arkilua_lookup_product);
		lua_settable(L, -3);
		/* set the __tostring metamethod */
		lua_pushstring(L, "__tostring");
		lua_pushcfunction(L, utils::lua::tostring_arkitype<Product>);
		lua_settable(L, -3);
	}

	lua_setmetatable(L, -2);
}
#endif


namespace product {

static TypeCache<GRIB1> cache_grib1;
static TypeCache<GRIB2> cache_grib2;
static TypeCache<BUFR> cache_bufr;


Product::Style GRIB1::style() const { return Product::GRIB1; }
void GRIB1::encodeWithoutEnvelope(Encoder& enc) const
{
	Product::encodeWithoutEnvelope(enc);
	enc.addUInt(m_origin, 1);
	enc.addUInt(m_table, 1);
	enc.addUInt(m_product, 1);
}
std::ostream& GRIB1::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
	     << setfill('0')
	     << setw(3) << (int)m_origin << ", "
	     << setw(3) << (int)m_table << ", "
	     << setw(3) << (int)m_product
	     << setfill(' ')
	     << ")";
}
std::string GRIB1::exactQuery() const
{
    return str::fmtf("GRIB1,%d,%d,%d", (int)m_origin, (int)m_table, (int)m_product);
}

int GRIB1::compare(const Product& o) const
{
	int res = Product::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Product, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int GRIB1::compare(const GRIB1& o) const
{
	if (int res = m_origin - o.m_origin) return res;
	if (int res = m_table - o.m_table) return res;
	return m_product - o.m_product;
}

bool GRIB1::operator==(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;
	return m_origin == v->m_origin && m_table == v->m_table && m_product == v->m_product;
}

Item<GRIB1> GRIB1::create(unsigned char origin, unsigned char table, unsigned char product)
{
	GRIB1* res = new GRIB1;
	res->m_origin = origin;
	res->m_table = table;
	res->m_product = product;
	return cache_grib1.intern(res);
}

std::vector<int> GRIB1::toIntVector() const
{
	vector<int> res;
	res.push_back(m_origin);
	res.push_back(m_table);
	res.push_back(m_product);
	return res;
}


Product::Style GRIB2::style() const { return Product::GRIB2; }
void GRIB2::encodeWithoutEnvelope(Encoder& enc) const
{
	Product::encodeWithoutEnvelope(enc);
	enc.addUInt(m_centre, 2);
	enc.addUInt(m_discipline, 1);
	enc.addUInt(m_category, 1);
	enc.addUInt(m_number, 1); 
}
std::ostream& GRIB2::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
	     << setfill('0')
	     << setw(5) << (int)m_centre << ", "
	     << setw(3) << (int)m_discipline << ", "
	     << setw(3) << (int)m_category << ", "
	     << setw(3) << (int)m_number
	     << setfill(' ')
	     << ")";
}
std::string GRIB2::exactQuery() const
{
    return str::fmtf("GRIB2,%d,%d,%d,%d", (int)m_centre, (int)m_discipline, (int)m_category, (int)m_number);
}
int GRIB2::compare(const Product& o) const
{
	int res = Product::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB2 Product, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}

int GRIB2::compare(const GRIB2& o) const
{
	if (int res = m_centre - o.m_centre) return res;
	if (int res = m_discipline - o.m_discipline) return res;
	if (int res = m_category - o.m_category) return res;
	return m_number - o.m_number;
}

bool GRIB2::operator==(const Type& o) const
{
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v) return false;
	return m_centre == v->m_centre
	    && m_discipline == v->m_discipline
	    && m_category == v->m_category
	    && m_number == v->m_number;
}

Item<GRIB2> GRIB2::create(unsigned short centre, unsigned char discipline, unsigned char category, unsigned char number)
{
	GRIB2* res = new GRIB2;
	res->m_centre = centre;
	res->m_discipline = discipline;
	res->m_category = category;
	res->m_number = number;
	return cache_grib2.intern(res);
}

std::vector<int> GRIB2::toIntVector() const
{
	vector<int> res;
	res.push_back(m_centre);
	res.push_back(m_discipline);
	res.push_back(m_category);
	res.push_back(m_number);
	return res;
}


Product::Style BUFR::style() const { return Product::BUFR; }
void BUFR::encodeWithoutEnvelope(Encoder& enc) const
{
	Product::encodeWithoutEnvelope(enc);
	enc.addUInt(m_type, 1);
	enc.addUInt(m_subtype, 1);
	enc.addUInt(m_localsubtype, 1);
}
std::ostream& BUFR::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << setfill('0')
			 << setw(3) << (int)m_type << ", "
			 << setw(3) << (int)m_subtype << ", "
			 << setw(3) << (int)m_localsubtype
			 << setfill(' ')
			 << ")";
}
std::string BUFR::exactQuery() const
{
    return str::fmtf("BUFR,%d,%d,%d", (int)m_type, (int)m_subtype, (int)m_localsubtype);
}
int BUFR::compare(const Product& o) const
{
	int res = Product::compare(o);
	if (res != 0) return res;

	// We should be the same kind, so upcast
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a BUFR Product, but it is a ") + typeid(&o).name() + " instead");

	return compare(*v);
}
int BUFR::compare(const BUFR& o) const
{
	if (int res = m_type - o.m_type) return res;
	if (int res = m_subtype - o.m_subtype) return res;
	return m_localsubtype - o.m_localsubtype;
}
bool BUFR::operator==(const Type& o) const
{
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v) return false;
	return m_type == v->m_type
	    && m_subtype == v->m_subtype
	    && m_localsubtype == v->m_localsubtype;
}

Item<BUFR> BUFR::create(unsigned char type, unsigned char subtype, unsigned char localsubtype)
{
	BUFR* res = new BUFR;
	res->m_type = type;
	res->m_subtype = subtype;
	res->m_localsubtype = localsubtype;
	return cache_bufr.intern(res);
}

std::vector<int> BUFR::toIntVector() const
{
	vector<int> res;
	res.push_back(m_type);
	res.push_back(m_subtype);
	res.push_back(m_localsubtype);
	return res;
}

static void debug_interns()
{
	fprintf(stderr, "product GRIB1: sz %zd reused %zd\n", cache_grib1.size(), cache_grib1.reused());
	fprintf(stderr, "product GRIB2: sz %zd reused %zd\n", cache_grib2.size(), cache_grib2.reused());
	fprintf(stderr, "product BUFR: sz %zd reused %zd\n", cache_bufr.size(), cache_bufr.reused());
}

}

static MetadataType productType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Product::decode),
	(MetadataType::string_decoder)(&Product::decodeString),
	product::debug_interns);

}
}
// vim:set ts=4 sw=4:
