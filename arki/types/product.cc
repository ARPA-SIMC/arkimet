/*
 * types/product - Product metadata item
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/utils.h>
#include <sstream>
#include <iomanip>
#include <config.h>

#ifdef HAVE_LUA
#include <arki/utils-lua.h>
#endif

#define CODE types::TYPE_PRODUCT
#define TAG "product"
#define SERSIZELEN 1

using namespace std;
using namespace arki::utils;

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

std::string Product::encodeWithoutEnvelope() const
{
	using namespace utils;
	return encodeUInt(style(), 1);
}

Item<Product> Product::decode(const unsigned char* buf, size_t len)
{
	using namespace utils;
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
		lua_pushnumber(L, v1->origin);
		lua_pushnumber(L, v1->table);
		lua_pushnumber(L, v1->product);
		return 3;
	}
	else if (name == "grib2" && v.style() == Product::GRIB2)
	{
		const product::GRIB2* v1 = v.upcast<product::GRIB2>();
		lua_pushnumber(L, v1->centre);
		lua_pushnumber(L, v1->discipline);
		lua_pushnumber(L, v1->category);
		lua_pushnumber(L, v1->number);
		return 4;
	}
	else if (name == "bufr" && v.style() == Product::BUFR)
	{
		const product::BUFR* v1 = v.upcast<product::BUFR>();
		lua_pushnumber(L, v1->type);
		lua_pushnumber(L, v1->subtype);
		lua_pushnumber(L, v1->localsubtype);
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

Product::Style GRIB1::style() const { return Product::GRIB1; }
std::string GRIB1::encodeWithoutEnvelope() const
{
	using namespace utils;
	return Product::encodeWithoutEnvelope() + encodeUInt(origin, 1) + encodeUInt(table, 1) + encodeUInt(product, 1);
}
std::ostream& GRIB1::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << setfill('0')
	         << setw(3) << (int)origin << ", " << setw(3) << (int)table << ", " << setw(3) << (int)product
			 << setfill(' ')
			 << ")";
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
	if (int res = origin - o.origin) return res;
	if (int res = table - o.table) return res;
	return product - o.product;
}

bool GRIB1::operator==(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;
	return origin == v->origin && table == v->table && product == v->product;
}

Item<GRIB1> GRIB1::create(unsigned char origin, unsigned char table, unsigned char product)
{
	GRIB1* res = new GRIB1;
	res->origin = origin;
	res->table = table;
	res->product = product;
	return res;
}

std::vector<int> GRIB1::toIntVector() const
{
	vector<int> res;
	res.push_back(origin);
	res.push_back(table);
	res.push_back(product);
	return res;
}


Product::Style GRIB2::style() const { return Product::GRIB2; }
std::string GRIB2::encodeWithoutEnvelope() const
{
	using namespace utils;
	return Product::encodeWithoutEnvelope() + encodeUInt(centre, 2) + encodeUInt(discipline, 1) + encodeUInt(category, 1) + encodeUInt(number, 1); 
}
std::ostream& GRIB2::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << setfill('0')
			 << setw(5) << (int)centre << ", " << setw(3) << (int)discipline << ", " << setw(3) << (int)category << ", " << (int)number
			 << setfill(' ')
			 << ")";
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
	if (int res = centre - o.centre) return res;
	if (int res = discipline - o.discipline) return res;
	if (int res = category - o.category) return res;
	return number - o.number;
}

bool GRIB2::operator==(const Type& o) const
{
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v) return false;
	return centre == v->centre
	    && discipline == v->discipline
		&& category == v->category
		&& number == v->number;
}

Item<GRIB2> GRIB2::create(unsigned short centre, unsigned char discipline, unsigned char category, unsigned char number)
{
	GRIB2* res = new GRIB2;
	res->centre = centre;
	res->discipline = discipline;
	res->category = category;
	res->number = number;
	return res;
}

std::vector<int> GRIB2::toIntVector() const
{
	vector<int> res;
	res.push_back(centre);
	res.push_back(discipline);
	res.push_back(category);
	res.push_back(number);
	return res;
}


Product::Style BUFR::style() const { return Product::BUFR; }
std::string BUFR::encodeWithoutEnvelope() const
{
	using namespace utils;
	return Product::encodeWithoutEnvelope() + encodeUInt(type, 1) + encodeUInt(subtype, 1) + encodeUInt(localsubtype, 1);
}
std::ostream& BUFR::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "("
			 << setfill('0')
			 << setw(3) << (int)type << ", " << setw(3) << (int)subtype << ", " << setw(3) << (int)localsubtype
			 << setfill(' ')
			 << ")";
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
	if (int res = type - o.type) return res;
	if (int res = subtype - o.subtype) return res;
	return localsubtype - o.localsubtype;
}
bool BUFR::operator==(const Type& o) const
{
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v) return false;
	return type == v->type
	    && subtype == v->subtype
		&& localsubtype == v->localsubtype;
}

Item<BUFR> BUFR::create(unsigned char type, unsigned char subtype, unsigned char localsubtype)
{
	BUFR* res = new BUFR;
	res->type = type;
	res->subtype = subtype;
	res->localsubtype = localsubtype;
	return res;
}

std::vector<int> BUFR::toIntVector() const
{
	vector<int> res;
	res.push_back(type);
	res.push_back(subtype);
	res.push_back(localsubtype);
	return res;
}

}

static MetadataType productType(
	CODE, SERSIZELEN, TAG,
	(MetadataType::item_decoder)(&Product::decode),
	(MetadataType::string_decoder)(&Product::decodeString));

}
}
// vim:set ts=4 sw=4:
