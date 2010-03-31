/*
 * matcher/origin - Origin matcher
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

#include <arki/matcher/origin.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace matcher {

std::string MatchOrigin::name() const { return "origin"; }

MatchOriginGRIB1::MatchOriginGRIB1(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	centre = args.getInt(0, -1);
	subcentre = args.getInt(1, -1);
	process = args.getInt(2, -1);
}

bool MatchOriginGRIB1::matchItem(const Item<>& o) const
{
	const types::origin::GRIB1* v = dynamic_cast<const types::origin::GRIB1*>(o.ptr());
	if (!v) return false;
	if (centre != -1 && centre != v->centre()) return false;
	if (subcentre != -1 && subcentre != v->subcentre()) return false;
	if (process != -1 && process != v->process()) return false;
	return true;
}

std::string MatchOriginGRIB1::toString() const
{
	CommaJoiner res;
	res.add("GRIB1");
	if (centre != -1) res.add(centre); else res.addUndef();
	if (subcentre != -1) res.add(subcentre); else res.addUndef();
	if (process != -1) res.add(process); else res.addUndef();
	return res.join();
}

MatchOriginGRIB2::MatchOriginGRIB2(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	centre = args.getInt(0, -1);
	subcentre = args.getInt(1, -1);
	processtype = args.getInt(2, -1);
	bgprocessid = args.getInt(3, -1);
	processid = args.getInt(4, -1);
}

bool MatchOriginGRIB2::matchItem(const Item<>& o) const
{
	const types::origin::GRIB2* v = dynamic_cast<const types::origin::GRIB2*>(o.ptr());
	if (!v) return false;
	if (centre      != -1 && centre      != v->centre()) return false;
	if (subcentre   != -1 && subcentre   != v->subcentre()) return false;
	if (processtype != -1 && processtype != v->processtype()) return false;
	if (bgprocessid != -1 && bgprocessid != v->bgprocessid()) return false;
	if (processid   != -1 && processid   != v->processid()) return false;
	return true;
}

std::string MatchOriginGRIB2::toString() const
{
	CommaJoiner res;
	res.add("GRIB2");
	if (centre      != -1) res.add(centre); else res.addUndef();
	if (subcentre   != -1) res.add(subcentre); else res.addUndef();
	if (processtype != -1) res.add(processtype); else res.addUndef();
	if (bgprocessid != -1) res.add(bgprocessid); else res.addUndef();
	if (processid   != -1) res.add(processid); else res.addUndef();
	return res.join();
}

MatchOriginBUFR::MatchOriginBUFR(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	centre = args.getInt(0, -1);
	subcentre = args.getInt(1, -1);
}

bool MatchOriginBUFR::matchItem(const Item<>& o) const
{
	const types::origin::BUFR* v = dynamic_cast<const types::origin::BUFR*>(o.ptr());
	if (!v) return false;
	if (centre != -1 && centre != v->centre()) return false;
	if (subcentre != -1 && subcentre != v->subcentre()) return false;
	return true;
}

std::string MatchOriginBUFR::toString() const
{
	CommaJoiner res;
	res.add("BUFR");
	if (centre != -1) res.add(centre); else res.addUndef();
	if (subcentre != -1) res.add(subcentre); else res.addUndef();
	return res.join();
}

MatchOrigin* MatchOrigin::parse(const std::string& pattern)
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
	switch (types::Origin::parseStyle(name))
	{
		case types::Origin::GRIB1: return new MatchOriginGRIB1(rest);
		case types::Origin::GRIB2: return new MatchOriginGRIB2(rest);
		case types::Origin::BUFR: return new MatchOriginBUFR(rest);
		default:
			throw wibble::exception::Consistency("parsing type of origin to match", "unsupported origin style: " + name);
	}
}

MatcherType origin("origin", types::TYPE_ORIGIN, (MatcherType::subexpr_parser)MatchOrigin::parse);

}
}

// vim:set ts=4 sw=4:
