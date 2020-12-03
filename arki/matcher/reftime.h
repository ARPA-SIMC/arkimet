#ifndef ARKI_MATCHER_REFTIME
#define ARKI_MATCHER_REFTIME

/*
 * matcher/reftime - Reftime matcher
 *
 * Copyright (C) 2007,2008,2009  ARPAE-SIMC <simc-urp@arpae.it>
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
#include <arki/matcher/utils.h>
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
struct MatchReftime : public matcher::Implementation
{
    /// Sequences of reftime matches, ANDed together
    std::vector<reftime::DTMatch*> tests;

    MatchReftime();
    MatchReftime(const std::string& pattern);
    ~MatchReftime();

    //MatchType type() const { return MATCH_REFTIME; }
    std::string name() const override;

    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    bool match_interval(const core::Interval& o) const;
    std::string toString() const override;
    std::string sql(const std::string& column) const;

    /**
     * Restrict a datetime range, returning the new range endpoints in begin
     * and end. Begin is considered included in the range, end excluded.
     *
     * A nullptr unique_ptr means an open end.
     *
     * Returns true if the result is a valid interval, false if this match does
     * not match the given interval at all.
     *
     * There can be further restrictions than this interval (for example,
     * restrictions on the time of the day).
     */
    bool intersect_interval(core::Interval& interval) const;

    static Implementation* parse(const std::string& pattern);
    static void init();
};

}
}

#endif
