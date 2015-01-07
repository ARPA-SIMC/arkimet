#ifndef ARKI_MATCHER_TIMERANGE
#define ARKI_MATCHER_TIMERANGE

/*
 * matcher/timerange - Timerange matcher
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
#include <arki/types/timerange.h>

namespace arki {
namespace matcher {

/**
 * Match Timeranges
 *
 * Syntax: GRIB,1,0,0 or GRIB,1 or GRIB,1,, instant
 *         GRIB,2,0,3h forecast next 3 hours
 *         GRIB,3 any average
 */
struct MatchTimerange : public Implementation
{
    std::string name() const override;

    static MatchTimerange* parse(const std::string& pattern);
    static void init();
};

struct MatchTimerangeGRIB1 : public MatchTimerange
{
    types::timerange::GRIB1Unit unit;
    bool has_ptype, has_p1, has_p2;
    int ptype, p1, p2;

    MatchTimerangeGRIB1(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchTimerangeGRIB2 : public MatchTimerange
{
	int type;
	int unit;
	int p1;
	int p2;

    MatchTimerangeGRIB2(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchTimerangeBUFR : public MatchTimerange
{
	bool has_forecast;
	bool is_seconds;
	unsigned int value;

    MatchTimerangeBUFR(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

/**
 * 'timedef' timerange matcher implementation.
 *
 * Syntax:
 *
 *    timerange:timedef,+72h,1,6h
 *                        |  | +---- Duration of statistical process
 *                        |  +------ Type of statistical process (from DB-All.e)
 *                        +--------- Forecast step
 */
struct MatchTimerangeTimedef : public MatchTimerange
{
    bool has_step;
    int step;
    bool step_is_seconds;

    bool has_proc_type;
    int proc_type;

    bool has_proc_duration;
    int proc_duration;
    bool proc_duration_is_seconds;

    MatchTimerangeTimedef(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

}
}

// vim:set ts=4 sw=4:
#endif
