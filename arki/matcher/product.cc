/*
 * matcher/product - Product matcher
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

#include <arki/matcher/product.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace matcher {

std::string MatchProduct::name() const { return "product"; }

MatchProductGRIB1::MatchProductGRIB1(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	origin = args.getInt(0, -1);
	table = args.getInt(1, -1);
	product = args.getInt(2, -1);
}

bool MatchProductGRIB1::matchItem(const Item<>& o) const
{
	const types::product::GRIB1* v = dynamic_cast<const types::product::GRIB1*>(o.ptr());
	if (!v) return false;
	if (origin != -1 && (unsigned)origin != v->origin()) return false;
	if (table != -1 && (unsigned)table != v->table()) return false;
	if (product != -1 && (unsigned)product != v->product()) return false;
	return true;
}

std::string MatchProductGRIB1::toString() const
{
	CommaJoiner res;
	res.add("GRIB1");
	if (origin != -1) res.add(origin); else res.addUndef();
	if (table != -1) res.add(table); else res.addUndef();
	if (product != -1) res.add(product); else res.addUndef();
	return res.join();
}

MatchProductGRIB2::MatchProductGRIB2(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	centre = args.getInt(0, -1);
	discipline = args.getInt(1, -1);
	category = args.getInt(2, -1);
	number = args.getInt(3, -1);
}

bool MatchProductGRIB2::matchItem(const Item<>& o) const
{
	const types::product::GRIB2* v = dynamic_cast<const types::product::GRIB2*>(o.ptr());
	if (!v) return false;
	if (centre != -1 && (unsigned)centre != v->centre()) return false;
	if (discipline != -1 && (unsigned)discipline != v->discipline()) return false;
	if (category != -1 && (unsigned)category != v->category()) return false;
	if (number != -1 && (unsigned)number != v->number()) return false;
	return true;
}

std::string MatchProductGRIB2::toString() const
{
	CommaJoiner res;
	res.add("GRIB2");
	if (centre != -1) res.add(centre); else res.addUndef();
	if (discipline != -1) res.add(discipline); else res.addUndef();
	if (category != -1) res.add(category); else res.addUndef();
	if (number != -1) res.add(number); else res.addUndef();
	return res.join();
}

MatchProductBUFR::MatchProductBUFR(const std::string& pattern)
	: name(pattern)
{
}

bool MatchProductBUFR::matchItem(const Item<>& o) const
{
	const types::product::BUFR* v = dynamic_cast<const types::product::BUFR*>(o.ptr());
	if (!v) return false;
	return name.empty() or name == v->name();
}

std::string MatchProductBUFR::toString() const
{
	return "BUFR," + name;
}


MatchProduct* MatchProduct::parse(const std::string& pattern)
{
	size_t beg = 0;
	size_t pos = pattern.find(',', beg);
	string name;
	string rest;
	if (pos == string::npos)
		name = str::trim(pattern.substr(beg));
	else {
		name = str::trim(pattern.substr(beg, pos-beg));
		rest = pattern.substr(pos+1);
	}
	switch (types::Product::parseStyle(name))
	{
		case types::Product::GRIB1: return new MatchProductGRIB1(rest);
		case types::Product::GRIB2: return new MatchProductGRIB2(rest);
		case types::Product::BUFR: return new MatchProductBUFR(rest);
		default:
			throw wibble::exception::Consistency("parsing type of product to match", "unsupported product style: " + name);
	}
}

MatcherType product("product", types::TYPE_PRODUCT, (MatcherType::subexpr_parser)MatchProduct::parse);

}
}

// vim:set ts=4 sw=4:
