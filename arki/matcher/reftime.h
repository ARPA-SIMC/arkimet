#ifndef ARKI_MATCHER_REFTIME
#define ARKI_MATCHER_REFTIME

/*
 * matcher/reftime - Reftime matcher
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/reftime.h>
#include <vector>

namespace arki {
namespace matcher {

namespace reftime {
class DTMatch;
}

/**
 * Match reference times.
 *
 * Ranges match when they intersect the interval given in the expression.
 */
struct MatchReftime : public Implementation
{
	std::vector<reftime::DTMatch*> tests;

	MatchReftime(const std::string& pattern);
	~MatchReftime();

	//MatchType type() const { return MATCH_REFTIME; }
	std::string name() const;

	bool matchItem(const Item<>& o) const;
	std::string toString() const;
	std::string sql(const std::string& column) const;

	/**
	 * Get the date extremes matched by this matcher.
	 *
	 * The interval includes the two extremes.
	 *
	 * There can be further restrictions than this interval (for example,
	 * restrictions on the time of the day).
	 *
	 * @returns The start and end date and time for this matcher.
	 *   If begin or end are all zeros, it means that the interval is
	 *   open-ended.
	 */
	void dateRange(UItem<types::Time>& begin, UItem<types::Time>& end) const;

    static MatchReftime* parse(const std::string& pattern);
    static void init();
};

}
}

// vim:set ts=4 sw=4:
#endif
