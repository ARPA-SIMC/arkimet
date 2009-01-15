/*
 * matcher/ensemble - Ensemble matcher
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

#include <arki/matcher/ensemble.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace matcher {

std::string MatchEnsemble::name() const { return "ensemble"; }

MatchEnsembleGRIB::MatchEnsembleGRIB(const std::string& pattern)
{
	expr = ValueBag::parse(pattern);
}

bool MatchEnsembleGRIB::matchItem(const Item<>& o) const
{
	const types::ensemble::GRIB* v = dynamic_cast<const types::ensemble::GRIB*>(o.ptr());
	if (!v) return false;
	return v->values.contains(expr);
}

std::string MatchEnsembleGRIB::toString() const
{
	return "GRIB:" + expr.toString();
}

MatchEnsemble* MatchEnsemble::parse(const std::string& pattern)
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
	switch (types::Ensemble::parseStyle(name))
	{
		case types::Ensemble::GRIB: return new MatchEnsembleGRIB(rest);
		default:
			throw wibble::exception::Consistency("parsing type of ensemble to match", "unsupported ensemble style: " + name);
	}
}

MatcherType ensemble("ensemble", types::TYPE_ENSEMBLE, (MatcherType::subexpr_parser)MatchEnsemble::parse);

}
}

// vim:set ts=4 sw=4:
