/*
 * matcher/area - Area matcher
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

#include <arki/matcher/area.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>
#include <arki/utils/geosdef.h>
#include <wibble/regexp.h>
#include <strings.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace matcher {

std::string MatchArea::name() const { return "area"; }

MatchAreaGRIB::MatchAreaGRIB(const std::string& pattern)
{
	expr = ValueBag::parse(pattern);
}

bool MatchAreaGRIB::matchItem(const Item<>& o) const
{
	const types::area::GRIB* v = dynamic_cast<const types::area::GRIB*>(o.ptr());
	if (!v) return false;
	return v->values.contains(expr);
}

std::string MatchAreaGRIB::toString() const
{
	return "GRIB:" + expr.toString();
}

MatchArea* MatchArea::parse(const std::string& pattern)
{
	string p = str::trim(pattern);
	if (strncasecmp(p.c_str(), "grib:", 5) == 0)
	{
		return new MatchAreaGRIB(str::trim(p.substr(5)));
#ifdef HAVE_GEOS
	} else if (strncasecmp(p.c_str(), "bbox ", 5) == 0) {
		return MatchAreaBBox::parse(str::trim(p.substr(5)));
#endif
	} else 
		throw wibble::exception::Consistency("parsing type of area to match", "unsupported area match: " + str::trim(p.substr(0, 5)));
}

#ifdef HAVE_GEOS
static auto_ptr<ARKI_GEOS_GEOMETRY> parseBBox(const ARKI_GEOS_GEOMETRYFACTORY& gf, const std::string sb)
{
	using namespace ARKI_GEOS_NS;

	geos::io::WKTReader reader(&gf);
	return auto_ptr<ARKI_GEOS_GEOMETRY>(reader.read(sb));
}

MatchAreaBBox::MatchAreaBBox(const std::string& verb, const std::string& geom)
	: gf(0), geom(0), verb(verb)
{
	gf = new ARKI_GEOS_GEOMETRYFACTORY();
	std::auto_ptr<ARKI_GEOS_GEOMETRY> g = parseBBox(*gf, geom);
	this->geom = g.release();
	geom_str = geom;
}

MatchAreaBBox::~MatchAreaBBox()
{
	if (gf) delete gf;
	if (geom) delete geom;
}

MatchAreaBBox* MatchAreaBBox::parse(const std::string& pattern)
{
	size_t beg = 0;
	size_t pos = pattern.find(' ', beg);
	string verb;
	string rest;
	if (pos == string::npos)
		verb = str::tolower(str::trim(pattern.substr(beg)));
	else {
		verb = str::tolower(str::trim(pattern.substr(beg, pos-beg)));
		rest = str::trim(pattern.substr(pos+1));
	}
	
	if (verb == "equals")
	{
		return new MatchAreaBBoxEquals(rest);
	} else if (verb == "covers") {
		return new MatchAreaBBoxCovers(rest);
	} else if (verb == "intersects") {
		return new MatchAreaBBoxIntersects(rest);
	} else if (verb == "coveredby") {
		return new MatchAreaBBoxCoveredBy(rest);
	} else {
		throw wibble::exception::Consistency("parsing type of bbox match", "unsupported match type: " + verb);
	}
}

bool MatchAreaBBox::matchItem(const Item<>& o) const
{
	if (geom == 0) return false;

	const types::Area* v = dynamic_cast<const types::Area*>(o.ptr());
	if (!v) return false;
	const ARKI_GEOS_GEOMETRY* bbox = v->bbox();
	if (bbox == 0) return false;

	return matchGeom(bbox);
}

std::string MatchAreaBBox::toString() const
{
	stringstream out;
	out << "bbox " << verb << " " << geom_str;
	return out.str();
}



MatchAreaBBoxEquals::MatchAreaBBoxEquals(const std::string& geom) : MatchAreaBBox("equals", geom) {}

bool MatchAreaBBoxEquals::matchGeom(const ARKI_GEOS_GEOMETRY* val) const
{
	return val->equals(geom);
}

MatchAreaBBoxCovers::MatchAreaBBoxCovers(const std::string& geom) : MatchAreaBBox("covers", geom) {}

bool MatchAreaBBoxCovers::matchGeom(const ARKI_GEOS_GEOMETRY* val) const
{
	return val->covers(geom);
}

MatchAreaBBoxIntersects::MatchAreaBBoxIntersects(const std::string& geom) : MatchAreaBBox("intersects", geom) {}

bool MatchAreaBBoxIntersects::matchGeom(const ARKI_GEOS_GEOMETRY* val) const
{
	return val->intersects(geom);
}

MatchAreaBBoxCoveredBy::MatchAreaBBoxCoveredBy(const std::string& geom) : MatchAreaBBox("coveredby", geom) {}

bool MatchAreaBBoxCoveredBy::matchGeom(const ARKI_GEOS_GEOMETRY* val) const
{
	return val->coveredBy(geom);
}
#endif

MatcherType area("area", types::TYPE_AREA, (MatcherType::subexpr_parser)MatchArea::parse);

}
}

// vim:set ts=4 sw=4:
