#ifndef ARKI_MATCHER_LEVEL
#define ARKI_MATCHER_LEVEL

/*
 * matcher/level - Level matcher
 *
 * Copyright (C) 2007,2008  ARPAE-SIMC <simc-urp@arpae.it>
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
#include <arki/types/level.h>

namespace arki {
namespace matcher {

/**
 * Match Levels
 */
struct MatchLevel : public Implementation
{
    std::string name() const;

    static std::unique_ptr<MatchLevel> parse(const std::string& pattern);
    static void init();
};

struct MatchLevelGRIB1 : public MatchLevel
{
	// This is -1 when it should be ignored in the match
	int type;
	int l1;
	int l2;

    MatchLevelGRIB1(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchLevelGRIB2S : public MatchLevel
{
    uint8_t type;
    bool has_type;
    uint8_t scale;
    bool has_scale;
    uint32_t value;
    bool has_value;

    MatchLevelGRIB2S(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchLevelGRIB2D : public MatchLevel
{
    uint8_t type1;
    bool has_type1;
    uint8_t scale1;
    bool has_scale1;
    uint32_t value1;
    bool has_value1;
    uint8_t type2;
    bool has_type2;
    uint8_t scale2;
    bool has_scale2;
    uint32_t value2;
    bool has_value2;

    MatchLevelGRIB2D(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchLevelODIMH5 : public MatchLevel
{
	std::vector<double> vals;
	double vals_offset;

	double range_min;
	double range_max;

    MatchLevelODIMH5(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};


}
}

// vim:set ts=4 sw=4:
#endif
