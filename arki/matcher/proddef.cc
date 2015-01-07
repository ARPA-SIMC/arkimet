/*
 * matcher/proddef - Product definition matcher
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

#include <arki/matcher/proddef.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>

using namespace std;
using namespace wibble;
using namespace arki::types;

namespace arki {
namespace matcher {

std::string MatchProddef::name() const { return "proddef"; }

MatchProddefGRIB::MatchProddefGRIB(const std::string& pattern)
{
	expr = ValueBag::parse(pattern);
}

bool MatchProddefGRIB::matchItem(const Type& o) const
{
    const types::proddef::GRIB* v = dynamic_cast<const types::proddef::GRIB*>(&o);
    if (!v) return false;
    return v->values().contains(expr);
}

std::string MatchProddefGRIB::toString() const
{
	return "GRIB:" + expr.toString();
}

MatchProddef* MatchProddef::parse(const std::string& pattern)
{
	size_t beg = 0;
	size_t pos = pattern.find(':', beg);
	string name;
	string rest;
	if (pos == string::npos)
		name = str::trim(pattern.substr(beg));
	else {
		name = str::trim(pattern.substr(beg, pos-beg));
		rest = pattern.substr(pos+1);
	}
	switch (types::Proddef::parseStyle(name))
	{
		case types::Proddef::GRIB: return new MatchProddefGRIB(rest);
		default:
			throw wibble::exception::Consistency("parsing type of proddef to match", "unsupported proddef style: " + name);
	}
}

void MatchProddef::init()
{
    Matcher::register_matcher("proddef", types::TYPE_PRODDEF, (MatcherType::subexpr_parser)MatchProddef::parse);
}

}
}

// vim:set ts=4 sw=4:
