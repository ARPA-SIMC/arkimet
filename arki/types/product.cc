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

const char* traits<Product>::type_tag = TAG;
const types::Code traits<Product>::type_code = CODE;
const size_t traits<Product>::type_sersize_bytes = SERSIZELEN;
const char* traits<Product>::type_lua_tag = LUATAG_TYPES ".product";

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

Item<Product> Product::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	Decoder dec(buf, len);
	Style s = (Style)dec.popUInt(1, "product");
	switch (s)
	{
		case GRIB1: {
			uint8_t origin  = dec.popUInt(1, "GRIB1 origin"),
				table   = dec.popUInt(1, "GRIB1 table"),
				product = dec.popUInt(1, "GRIB1 product");
			return product::GRIB1::create(origin, table, product);
		}
		case GRIB2: {
			uint16_t centre     = dec.popUInt(2, "GRIB2 centre");
			uint8_t	 discipline = dec.popUInt(1, "GRIB2 discipline"),
				 category   = dec.popUInt(1, "GRIB2 category"),
				 number     = dec.popUInt(1, "GRIB2 number");
			return product::GRIB2::create(centre, discipline, category, number);
		}
		case BUFR: {
			uint8_t type         = dec.popUInt(1, "GRIB1 type"),
				subtype      = dec.popUInt(1, "GRIB1 subtype"),
				localsubtype = dec.popUInt(1, "GRIB1 localsubtype");
			ValueBag values = ValueBag::decode(dec.buf, dec.len);
			return product::BUFR::create(type, subtype, localsubtype, values);
		}
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
			NumberList<3> nums(inner, "Product", true);
			inner = str::trim(nums.tail);
			if (!inner.empty() && inner[0] == ',')
				inner = str::trim(nums.tail.substr(1));
			return product::BUFR::create(nums.vals[0], nums.vals[1], nums.vals[2], ValueBag::parse(inner));
		}
		default:
			throw wibble::exception::Consistency("parsing Product", "unknown Product style " + formatStyle(style));
	}
}

static int arkilua_new_grib1(lua_State* L)
{
	int origin = luaL_checkint(L, 1);
	int table = luaL_checkint(L, 2);
	int product = luaL_checkint(L, 3);
	Item<> res = product::GRIB1::create(origin, table, product);
	res->lua_push(L);
	return 1;
}

static int arkilua_new_grib2(lua_State* L)
{
	int centre = luaL_checkint(L, 1);
	int discipline = luaL_checkint(L, 2);
	int category = luaL_checkint(L, 3);
	int number = luaL_checkint(L, 4);
	Item<> res = product::GRIB2::create(centre, discipline, category, number);
	res->lua_push(L);
	return 1;
}

static int arkilua_new_bufr(lua_State* L)
{
	int type = luaL_checkint(L, 1);
	int subtype = luaL_checkint(L, 2);
	int localsubtype = luaL_checkint(L, 3);
	if (lua_gettop(L) > 3)
	{
		luaL_checktype(L, 4, LUA_TTABLE);
		ValueBag values;
		values.load_lua_table(L, 4);
		product::BUFR::create(type, subtype, localsubtype, values)->lua_push(L);
	}
	else
		product::BUFR::create(type, subtype, localsubtype)->lua_push(L);
	return 1;
}

void Product::lua_loadlib(lua_State* L)
{
	static const struct luaL_reg lib [] = {
		{ "grib1", arkilua_new_grib1 },
		{ "grib2", arkilua_new_grib2 },
		{ "bufr", arkilua_new_bufr },
		{ NULL, NULL }
	};
	luaL_openlib(L, "arki_product", lib, 0);
	lua_pop(L, 1);
}

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
const char* GRIB1::lua_type_name() const { return "arki.types.product.grib1"; }

int GRIB1::compare_local(const Product& o) const
{
	// We should be the same kind, so upcast
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Product, but it is a ") + typeid(&o).name() + " instead");

	if (int res = m_origin - v->m_origin) return res;
	if (int res = m_table - v->m_table) return res;
	return m_product - v->m_product;
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

bool GRIB1::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "origin")
		lua_pushnumber(L, origin());
	else if (name == "table")
		lua_pushnumber(L, table());
	else if (name == "product")
		lua_pushnumber(L, product());
	else
		return Product::lua_lookup(L, name);
	return true;
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
const char* GRIB2::lua_type_name() const { return "arki.types.product.grib2"; }

int GRIB2::compare_local(const Product& o) const
{
	// We should be the same kind, so upcast
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB2 Product, but it is a ") + typeid(&o).name() + " instead");

	if (int res = m_centre - v->m_centre) return res;
	if (int res = m_discipline - v->m_discipline) return res;
	if (int res = m_category - v->m_category) return res;
	return m_number - v->m_number;
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
bool GRIB2::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "centre")
		lua_pushnumber(L, centre());
	else if (name == "discipline")
		lua_pushnumber(L, discipline());
	else if (name == "category")
		lua_pushnumber(L, category());
	else if (name == "number")
		lua_pushnumber(L, number());
	else
		return Product::lua_lookup(L, name);
	return true;
}


Product::Style BUFR::style() const { return Product::BUFR; }

void BUFR::encodeWithoutEnvelope(Encoder& enc) const
{
	Product::encodeWithoutEnvelope(enc);
	enc.addUInt(m_type, 1);
	enc.addUInt(m_subtype, 1);
	enc.addUInt(m_localsubtype, 1);
	m_values.encode(enc);
}
std::ostream& BUFR::writeToOstream(std::ostream& o) const
{
	o << formatStyle(style()) << "("
	  << setfill('0')
	  << setw(3) << (int)m_type << ", "
	  << setw(3) << (int)m_subtype << ", "
	  << setw(3) << (int)m_localsubtype
	  << setfill(' ');
	if (m_values.empty())
		o << ")";
	else
		o << ", " << m_values << ")";
	return o;
}
std::string BUFR::exactQuery() const
{
    string res = str::fmtf("BUFR,%u,%u,%u", type(), subtype(), localsubtype());
    if (!m_values.empty())
	    res += ":" + m_values.toString();
    return res;
}
const char* BUFR::lua_type_name() const { return "arki.types.product.bufr"; }

int BUFR::compare_local(const Product& o) const
{
	// We should be the same kind, so upcast
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a BUFR Product, but it is a ") + typeid(&o).name() + " instead");

	if (int res = m_type - v->m_type) return res;
	if (int res = m_subtype - v->m_subtype) return res;
	if (int res = m_localsubtype - v->m_localsubtype) return res;
	return m_values.compare(v->m_values);
}
bool BUFR::operator==(const Type& o) const
{
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v) return false;
	return m_type == v->m_type
	    && m_subtype == v->m_subtype
	    && m_localsubtype == v->m_localsubtype
	    && m_values == v->m_values;
}

Item<BUFR> BUFR::create(unsigned char type, unsigned char subtype, unsigned char localsubtype)
{
	BUFR* res = new BUFR;
	res->m_type = type;
	res->m_subtype = subtype;
	res->m_localsubtype = localsubtype;
	return cache_bufr.intern(res);
}

Item<BUFR> BUFR::create(unsigned char type, unsigned char subtype, unsigned char localsubtype, const ValueBag& values)
{
	BUFR* res = new BUFR;
	res->m_type = type;
	res->m_subtype = subtype;
	res->m_localsubtype = localsubtype;
	res->m_values = values;
	return cache_bufr.intern(res);
}

Item<BUFR> BUFR::addValues(const ValueBag& newvalues) const
{
	BUFR* res = new BUFR;
	res->m_type = m_type;
	res->m_subtype = m_subtype;
	res->m_localsubtype = m_localsubtype;
	res->m_values = m_values;
	res->m_values.update(newvalues);
	return cache_bufr.intern(res);
}

std::vector<int> BUFR::toIntVector() const
{
	return vector<int>();
}

bool BUFR::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "type")
		lua_pushnumber(L, type());
	else if (name == "subtype")
		lua_pushnumber(L, subtype());
	else if (name == "localsubtype")
		lua_pushnumber(L, localsubtype());
	else if (name == "val")
		m_values.lua_push(L);
	else
		return Product::lua_lookup(L, name);
	return true;
}

static int arkilua_addvalues(lua_State* L)
{
	Item<BUFR> b = Type::lua_check<Product>(L, 1).upcast<BUFR>();
	luaL_checktype(L, 2, LUA_TTABLE);

	ValueBag values;
	values.load_lua_table(L, 2);

	b->addValues(values)->lua_push(L);
        return 1;
}

void BUFR::lua_register_methods(lua_State* L) const
{
	Product::lua_register_methods(L);

	static const struct luaL_reg lib [] = {
		{ "addValues", arkilua_addvalues },
		{ NULL, NULL }
	};
	luaL_register(L, NULL, lib);
}
static void debug_interns()
{
	fprintf(stderr, "product GRIB1: sz %zd reused %zd\n", cache_grib1.size(), cache_grib1.reused());
	fprintf(stderr, "product GRIB2: sz %zd reused %zd\n", cache_grib2.size(), cache_grib2.reused());
	fprintf(stderr, "product BUFR: sz %zd reused %zd\n", cache_bufr.size(), cache_bufr.reused());
}

}

static MetadataType productType = MetadataType::create<Product>(product::debug_interns);

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
