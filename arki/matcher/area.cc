#include "config.h"
#include "area.h"
#include "arki/utils/geos.h"
#include "arki/utils/regexp.h"
#include <strings.h>
#include <algorithm>

#ifdef HAVE_VM2
#include <arki/utils/vm2.h>
#endif

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace matcher {

std::string MatchArea::name() const { return "area"; }

MatchAreaGRIB::MatchAreaGRIB(const std::string& pattern)
{
	expr = ValueBag::parse(pattern);
}

bool MatchAreaGRIB::matchItem(const Type& o) const
{
    const types::area::GRIB* v = dynamic_cast<const types::area::GRIB*>(&o);
    if (!v) return false;
    return v->values().contains(expr);
}

std::string MatchAreaGRIB::toString() const
{
	return "GRIB:" + expr.toString();
}

MatchAreaODIMH5::MatchAreaODIMH5(const std::string& pattern)
{
	expr = ValueBag::parse(pattern);
}

bool MatchAreaODIMH5::matchItem(const Type& o) const
{
    const types::area::ODIMH5* v = dynamic_cast<const types::area::ODIMH5*>(&o);
    if (!v) return false;
    return v->values().contains(expr);
}

std::string MatchAreaODIMH5::toString() const
{
	return "ODIMH5:" + expr.toString();
}

MatchAreaVM2::MatchAreaVM2(const std::string& pattern)
{
    OptionalCommaList args(pattern, true);
	station_id = args.getInt(0, -1);
    expr = ValueBag::parse(args.tail);
#ifdef HAVE_VM2
    if (!expr.empty())
        idlist = utils::vm2::find_stations(expr);
#endif
}

bool MatchAreaVM2::matchItem(const Type& o) const
{
    const types::area::VM2* v = dynamic_cast<const types::area::VM2*>(&o);
    if (!v) return false;
    if (station_id != -1 && (unsigned) station_id != v->station_id()) return false;
    if (!expr.empty() && 
        std::find(idlist.begin(), idlist.end(), v->station_id()) == idlist.end())
            return false;
    return true;
}

std::string MatchAreaVM2::toString() const
{
	stringstream res;
	res << "VM2";
	if (station_id != -1)
	{
		res << "," << station_id;
	}
	if (!expr.empty())
		res << ":" << expr.toString();
	return res.str();
}


Implementation* MatchArea::parse(const std::string& pattern)
{
    string p = str::strip(pattern);
    if (strncasecmp(p.c_str(), "grib:", 5) == 0)
    {
        return new MatchAreaGRIB(str::strip(p.substr(5)));
    }
    else if (strncasecmp(p.c_str(), "odimh5:", 7) == 0)
    {
        return new MatchAreaODIMH5(str::strip(p.substr(7)));
    }
#ifdef HAVE_VM2
    else if (strncasecmp(p.c_str(), "vm2", 3) == 0)
    {
        if (strncasecmp(p.c_str(), "vm2,", 4) == 0)
            return new MatchAreaVM2(str::strip(p.substr(4)));
        else
            return new MatchAreaVM2(str::strip(p.substr(3)));
    }
#endif
#ifdef HAVE_GEOS
    else if (strncasecmp(p.c_str(), "bbox ", 5) == 0) 
    {
        return MatchAreaBBox::parse(str::strip(p.substr(5)));
    }
#endif
    else
        throw std::invalid_argument("cannot parse type of area to match: unsupported area match: " + str::strip(p.substr(0, 5)));
}

#ifdef HAVE_GEOS

MatchAreaBBox::MatchAreaBBox(const std::string& verb, const std::string& geom)
    : geom(0), verb(verb)
{
    auto gf = arki::utils::geos::GeometryFactory::getDefaultInstance();
    arki::utils::geos::WKTReader reader(gf);
#if GEOS_VERSION_MAJOR >= 3 && GEOS_VERSION_MINOR >= 8
    this->geom = reader.read(geom).release();
#else
    this->geom = reader.read(geom);
#endif
    geom_str = geom;
}

MatchAreaBBox::~MatchAreaBBox()
{
	if (geom) delete geom;
}

Implementation* MatchAreaBBox::parse(const std::string& pattern)
{
    size_t beg = 0;
    size_t pos = pattern.find(' ', beg);
    string verb;
    string rest;
    if (pos == string::npos)
        verb = str::lower(str::strip(pattern.substr(beg)));
    else {
        verb = str::lower(str::strip(pattern.substr(beg, pos-beg)));
        rest = str::strip(pattern.substr(pos+1));
    }

    if (verb == "equals")
    {
        return new MatchAreaBBoxEquals(rest);
    } else if (verb == "intersects") {
        return new MatchAreaBBoxIntersects(rest);
#if GEOS_VERSION_MAJOR >= 3
    } else if (verb == "covers") {
        return new MatchAreaBBoxCovers(rest);
    } else if (verb == "coveredby") {
        return new MatchAreaBBoxCoveredBy(rest);
#endif
    } else {
        throw std::invalid_argument("cannot parse type of bbox match: unsupported match type: " + verb);
    }
}

bool MatchAreaBBox::matchItem(const Type& o) const
{
	if (geom == 0) return false;

    const types::Area* v = dynamic_cast<const types::Area*>(&o);
    if (!v) return false;
    const arki::utils::geos::Geometry* bbox = v->bbox();
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

bool MatchAreaBBoxEquals::matchGeom(const arki::utils::geos::Geometry* val) const
{
	return val->equals(geom);
}

MatchAreaBBoxIntersects::MatchAreaBBoxIntersects(const std::string& geom) : MatchAreaBBox("intersects", geom) {}

bool MatchAreaBBoxIntersects::matchGeom(const arki::utils::geos::Geometry* val) const
{
	return val->intersects(geom);
}

#if GEOS_VERSION_MAJOR >= 3
MatchAreaBBoxCovers::MatchAreaBBoxCovers(const std::string& geom) : MatchAreaBBox("covers", geom) {}

bool MatchAreaBBoxCovers::matchGeom(const arki::utils::geos::Geometry* val) const
{
	return val->covers(geom);
}

MatchAreaBBoxCoveredBy::MatchAreaBBoxCoveredBy(const std::string& geom) : MatchAreaBBox("coveredby", geom) {}

bool MatchAreaBBoxCoveredBy::matchGeom(const arki::utils::geos::Geometry* val) const
{
	return val->coveredBy(geom);
}
#endif

#endif

void MatchArea::init()
{
    MatcherType::register_matcher("area", TYPE_AREA, (MatcherType::subexpr_parser)MatchArea::parse);
}

}
}
