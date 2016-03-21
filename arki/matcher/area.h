#ifndef ARKI_MATCHER_AREA
#define ARKI_MATCHER_AREA

/*
 * matcher/area - Area matcher
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/matcher.h>
#include <arki/types/area.h>
#include <arki/utils/geosfwd.h>

namespace arki {
namespace matcher {

/**
 * Match Areas
 */
struct MatchArea : public Implementation
{
    std::string name() const override;

    static std::unique_ptr<MatchArea> parse(const std::string& pattern);
    static void init();
};

struct MatchAreaGRIB : public MatchArea
{
	ValueBag expr;

    MatchAreaGRIB(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchAreaODIMH5 : public MatchArea
{
	ValueBag expr;

    MatchAreaODIMH5(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchAreaVM2 : public MatchArea
{
    // This is -1 when should be ignored
    int station_id;
    ValueBag expr;
    std::vector<int> idlist;

    MatchAreaVM2(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

#ifdef HAVE_GEOS
/**
 * Match BBoxs
 */
struct MatchAreaBBox : public MatchArea
{
	ARKI_GEOS_GEOMETRYFACTORY* gf;
	ARKI_GEOS_GEOMETRY* geom;
	std::string verb;
	std::string geom_str;

	MatchAreaBBox(const std::string& verb, const std::string& geom);
	~MatchAreaBBox();

    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
    virtual bool matchGeom(const ARKI_GEOS_GEOMETRY* val) const = 0;

    static std::unique_ptr<MatchAreaBBox> parse(const std::string& pattern);
};

struct MatchAreaBBoxEquals : public MatchAreaBBox
{
    MatchAreaBBoxEquals(const std::string& geom);
    virtual bool matchGeom(const ARKI_GEOS_GEOMETRY* val) const override;
};

struct MatchAreaBBoxIntersects : public MatchAreaBBox
{
    MatchAreaBBoxIntersects(const std::string& geom);
    virtual bool matchGeom(const ARKI_GEOS_GEOMETRY* val) const override;
};

#ifdef ARKI_NEW_GEOS
struct MatchAreaBBoxCovers : public MatchAreaBBox
{
    MatchAreaBBoxCovers(const std::string& geom);
    virtual bool matchGeom(const ARKI_GEOS_GEOMETRY* val) const override;
};

struct MatchAreaBBoxCoveredBy : public MatchAreaBBox
{
    MatchAreaBBoxCoveredBy(const std::string& geom);
    virtual bool matchGeom(const ARKI_GEOS_GEOMETRY* val) const override;
};
#endif

#endif

}
}

// vim:set ts=4 sw=4:
#endif
