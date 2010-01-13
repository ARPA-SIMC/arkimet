/*
 * matcher/area - Area matcher
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
MatchAreaBBox::MatchAreaBBox()
	: gf(0), geom(0)
{
	gf = new ARKI_GEOS_GEOMETRYFACTORY();
}

MatchAreaBBox::~MatchAreaBBox()
{
	if (gf) delete gf;
	if (geom) delete geom;
}

static auto_ptr<ARKI_GEOS_GEOMETRY> parseBBox(const ARKI_GEOS_GEOMETRYFACTORY& gf, const std::string sb)
{
	using namespace ARKI_GEOS_NS;

	geos::io::WKTReader reader(&gf);
	return auto_ptr<ARKI_GEOS_GEOMETRY>(reader.read(sb));

	/*
#define RE_FLOAT "[0-9]+(\\.[0-9]+)?"
#define RE_COUPLE RE_FLOAT " +" RE_FLOAT
	ERegexp re_point("^" RE_COUPLE "$");
	ERegexp re_box("^" RE_FLOAT " *- *" RE_FLOAT " +" RE_FLOAT " *- *" RE_FLOAT "$");
	ERegexp re_poly("^" RE_COUPLE "( *, *" RE_COUPLE " *)+$");
#undef RE_FLOAT
#undef RE_COUPLE
	if (re_point.match(sb))
	{
		std::stringstream in(sb);
		double lat, lon;
		in >> lat;
		in >> lon;
		return auto_ptr<ARKI_GEOS_GEOMETRY>(gf.createPoint(Coordinate(lon, lat)));
	} else if (re_box.match(sb)) {
		//std::stringstream in(sb);
		//double latmin, latmax, lonmin, lonmax;
		//in >> latmin;
		//in >> '-';
		//in >> latmax;
		//in >> lonmin;
		//in >> '-';
		//in >> lonmax;
		//CoordinateArraySequence cas;
		//cas.add(Coordinate(lonmin, latmin));
		//cas.add(Coordinate(lonmax, latmin));
		//cas.add(Coordinate(lonmax, latmax));
		//cas.add(Coordinate(lonmin, latmax));
		//cas.add(Coordinate(lonmin, latmin));
		//auto_ptr<LinearRing> lr(gf.createLinearRing(cas));
		//return auto_ptr<ARKI_GEOS_GEOMETRY>(gf.createPolygon(*lr, vector<Geometry*>()));
		return auto_ptr<ARKI_GEOS_GEOMETRY>(0);
	} else if (re_poly.match(sb)) {
		//CoordinateArraySequence cas;
		//for (size_t i = 0; i < points.size(); ++i)
		//	cas.add(Coordinate(points[i].second, points[i].first));
		//auto_ptr<LinearRing> lr(gf.createLinearRing(cas));
		//return gf.createPolygon(*lr, vector<Geometry*>());
		return auto_ptr<ARKI_GEOS_GEOMETRY>(0);
	} else {
		throw wibble::exception::Consistency("parsing bounding box", "unrecognised bounding box: " + sb);
	}
	*/
}

MatchAreaBBoxExact::MatchAreaBBoxExact(const std::string& geom)
{
	std::auto_ptr<ARKI_GEOS_GEOMETRY> g = parseBBox(*gf, geom);
	this->geom = g.release();
	geom_str = geom;
}

bool MatchAreaBBoxExact::matchItem(const Item<>& o) const
{
	if (geom == 0) 
		return false;

	const types::Area* v = dynamic_cast<const types::Area*>(o.ptr());
	if (!v) return false;
	const ARKI_GEOS_GEOMETRY* bbox = v->bbox();
	if (bbox == 0) return false;

	return bbox->equals(geom);
}

std::string MatchAreaBBoxExact::toString() const
{
	stringstream out;
	out << "bbox is " << geom_str;
	return out.str();
}

MatchAreaBBoxContains::MatchAreaBBoxContains(const std::string& geom)
{
	std::auto_ptr<ARKI_GEOS_GEOMETRY> g = parseBBox(*gf, geom);
	this->geom = g.release();
	geom_str = geom;
}

bool MatchAreaBBoxContains::matchItem(const Item<>& o) const
{
	if (geom == 0) return false;

	const types::Area* v = dynamic_cast<const types::Area*>(o.ptr());
	if (!v) return false;
	const ARKI_GEOS_GEOMETRY* bbox = v->bbox();
	if (bbox == 0) return false;

	return bbox->covers(geom);
}

std::string MatchAreaBBoxContains::toString() const
{
	stringstream out;
	out << "bbox contains " << geom_str;
	return out.str();
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
	
	if (verb == "is")
	{
		return new MatchAreaBBoxExact(rest);
	} else if (verb == "contains") {
		return new MatchAreaBBoxContains(rest);
	} else {
		throw wibble::exception::Consistency("parsing type of bbox match", "unsupported match type: " + verb);
	}
}
#endif

MatcherType area("area", types::TYPE_AREA, (MatcherType::subexpr_parser)MatchArea::parse);

}
}

// vim:set ts=4 sw=4:
