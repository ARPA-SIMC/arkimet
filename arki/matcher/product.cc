/*
 * matcher/product - Product matcher
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/matcher/product.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>
#include <limits>

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
{
	OptionalCommaList args(pattern, true);
	type = args.getInt(0, -1);
	subtype = args.getInt(1, -1);
	localsubtype = args.getInt(2, -1);
	values = ValueBag::parse(args.tail);
}

bool MatchProductBUFR::matchItem(const Item<>& o) const
{
	const types::product::BUFR* v = dynamic_cast<const types::product::BUFR*>(o.ptr());
	if (!v) return false;
	if (type != -1 && (unsigned)type != v->type()) return false;
	if (subtype != -1 && (unsigned)subtype != v->subtype()) return false;
	if (localsubtype != -1 && (unsigned)localsubtype != v->localsubtype()) return false;
	return v->values().contains(values);
}

std::string MatchProductBUFR::toString() const
{
	stringstream res;
	res << "BUFR";
	if (type != -1 || subtype != -1 || localsubtype != -1)
	{
		res << ",";
		if (type != -1) res << type;
		if (subtype != -1 || localsubtype != -1)
		{
			res << ",";
			if (subtype != -1) res << subtype;
			if (localsubtype != -1) res << "," << localsubtype;
		}
	}
	if (!values.empty())
		res << ":" << values.toString();
	return res.str();
}

static const double DOUBLENAN = std::numeric_limits<double>::quiet_NaN();

MatchProductODIMH5::MatchProductODIMH5(const std::string& pattern)
{
	OptionalCommaList args(pattern, true);
	obj		= args.getString(0, "");
	prod		= args.getString(1, "");
	/*REMOVED:prodpar1	= args.getDouble(2, DOUBLENAN); */
	/*REMOVED:prodpar2	= args.getDouble(3, DOUBLENAN); */
}

bool MatchProductODIMH5::matchItem(const Item<>& o) const
{
	const types::product::ODIMH5* v = dynamic_cast<const types::product::ODIMH5*>(o.ptr());
	if (!v) return false;
	if (obj.size() && 		obj != v->obj()) 	return false;
	if (prod.size() && 		prod != v->prod()) 	return false;
	/*REMOVED:if (!isnan(prodpar1) && 	prodpar1 != v->prodpar1()) 	return false; */
	/*REMOVED:if (!isnan(prodpar2) && 	prodpar2 != v->prodpar2()) 	return false; */
	return true;
}

std::string MatchProductODIMH5::toString() const
{
	CommaJoiner res;
	res.add("ODIMH5");
	if (obj.size()) 	res.add(obj); 		else res.addUndef();
	if (prod.size()) 	res.add(prod); 		else res.addUndef();
	/*REMOVED:if (!isnan(prodpar1)) 	res.add(prodpar1); 	else res.addUndef();*/
	/*REMOVED:if (!isnan(prodpar2)) 	res.add(prodpar2); 	else res.addUndef();*/
	return res.join();
}

MatchProduct* MatchProduct::parse(const std::string& pattern)
{
	size_t beg = 0;
	size_t pos = pattern.find_first_of(":,", beg);
	string name;
	string rest;
	if (pos == string::npos)
		name = str::trim(pattern.substr(beg));
	else {
		name = str::trim(pattern.substr(beg, pos-beg));
		rest = pattern.substr(pos+(pattern[pos] == ',' ? 1 : 0));
	}
	switch (types::Product::parseStyle(name))
	{
		case types::Product::GRIB1: return new MatchProductGRIB1(rest);
		case types::Product::GRIB2: return new MatchProductGRIB2(rest);
		case types::Product::BUFR: return new MatchProductBUFR(rest);
		case types::Product::ODIMH5: return new MatchProductODIMH5(rest);
		default:
			throw wibble::exception::Consistency("parsing type of product to match", "unsupported product style: " + name);
	}
}

void MatchProduct::init()
{
    Matcher::register_matcher("product", types::TYPE_PRODUCT, (MatcherType::subexpr_parser)MatchProduct::parse);
}

}
}

// vim:set ts=4 sw=4:
