#ifndef ARKI_MATCHER_BBOX
#define ARKI_MATCHER_BBOX

/*
 * matcher/area - BBox matcher
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

#include <arki/matcher.h>
#include <arki/types/area.h>
#include <arki/utils/geosfwd.h>

namespace arki {

namespace types {
struct BBox;
}

namespace matcher {

/**
 * Match BBoxs
 */
struct MatchBBox : public Implementation
{
	//MatchType type() const { return MATCH_BBOX; }
	std::string name() const;

	static MatchBBox* parse(const std::string& pattern);
};

struct MatchBBoxExact : public MatchBBox
{
	Item<types::BBox> sample;

	MatchBBoxExact(const std::string& pattern);
	bool matchItem(const Item<>& o) const;
	std::string toString() const;
};

#ifdef HAVE_GEOS
struct MatchBBoxContains : public MatchBBox
{
	Item<types::BBox> sample;
	ARKI_GEOS_GEOMETRYFACTORY* gf;
	ARKI_GEOS_GEOMETRY* geom;

	MatchBBoxContains(const std::string& pattern);
	~MatchBBoxContains();
	bool matchItem(const Item<>& o) const;
	std::string toString() const;
};
#endif

}
}

// vim:set ts=4 sw=4:
#endif
