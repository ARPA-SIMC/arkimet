#ifndef ARKI_MATCHER_SOURCE
#define ARKI_MATCHER_SOURCE

/*
 * matcher/source - Source matcher
 *
 * Copyright (C) 2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/source.h>

namespace arki {
namespace matcher {

/**
 * Match Runs
 */
struct MatchSource : public Implementation
{
    std::string name() const;

    static MatchSource* parse(const std::string& pattern);
    static void init();
};

#if 0
struct MatchRunMinute : public MatchRun
{
	// This is -1 when it should be ignored in the match
	int minute;

	MatchRunMinute(const std::string& pattern);
	bool matchItem(const Item<>& o) const;
	std::string toString() const;
};
#endif

}
}

// vim:set ts=4 sw=4:
#endif