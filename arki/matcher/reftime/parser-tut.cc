/*
 * Copyright (C) 2007--2011  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include "config.h"

#include <arki/matcher/tests.h>
#include <arki/matcher/reftime/parser.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::matcher::reftime;

struct arki_matcher_reftime_parser_shar {
};
TESTGRP(arki_matcher_reftime_parser);

// Try matching reference times
template<> template<>
void to::test<1>()
{
	Parser p;

	try {
		p.parse("rusco");
		ensure(false);
	} catch (...) {
		;
	}

	try {
		p.parse("1 2 3");
		ensure(false);
	} catch (...) {
		;
	}

	p.parse("=03");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->timebase(), 3*3600);
	ensure_equals(p.res[0]->toString(), ">=03:00:00,<=03:59:59");

	p.parse("==9999");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->timebase(), 0);
	ensure_equals(p.res[0]->toString(), ">=9999-01-01 00:00:00,<=9999-12-31 23:59:59");

	p.parse("every 3 hours");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), "==00:00:00%3h");

	p.parse(">2007-01-02 03:04:05%3h");
	ensure_equals(p.res.size(), 2u);
	ensure_equals(p.res[0]->toString(), ">2007-01-02 03:04:05");
	ensure_equals(p.res[0]->timebase(), 3*3600+4*60+5);
	ensure_equals(p.res[1]->toString(), "%3h");

	p.parse("<2007-01-02 03:04:05%3h");
	ensure_equals(p.res.size(), 2u);
	ensure_equals(p.res[0]->toString(), "<2007-01-02 03:04:05");
	ensure_equals(p.res[0]->timebase(), 3*3600+4*60+5);
	ensure_equals(p.res[1]->toString(), "%3h");

	p.parse("==12:00:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), "==12:00:00");

	p.parse(">=12:00:00Z");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=12:00:00");
}

// Check that relative times are what we want
template<> template<>
void to::test<2>()
{
	Parser p;
	// Set the date to: Tue Mar 25 00:00:00 UTC 2008
	p.tnow = 1206403200;
	p.parse(">= yesterday");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-24 00:00:00");

	p.parse("from yesterday");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-24 00:00:00");

	p.parse("<= tomorrow");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), "<=2008-03-26 23:59:59");

	p.parse("until tomorrow");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), "<=2008-03-26 23:59:59");

	p.parse(">= yesterday 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-24 12:00:00");
}

// Expressions used before the refactoring into bison
template<> template<>
void to::test<3>()
{
	Parser p;
	p.parse(">=2008");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-01-01 00:00:00");

	p.parse("=2008-03-01 12:00%30m");
	ensure_equals(p.res.size(), 2u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-01 12:00:00,<=2008-03-01 12:00:59");
	ensure_equals(p.res[1]->toString(), "%30m");

	p.parse(">=2008,>=12:30%1h,<18:30");
	ensure_equals(p.res.size(), 4u);
	ensure_equals(p.res[0]->toString(), ">=2008-01-01 00:00:00");
	ensure_equals(p.res[1]->toString(), ">=12:30:00");
	ensure_equals(p.res[2]->toString(), "%1h");
	ensure_equals(p.res[3]->toString(), "<18:30:00");
}

// Replace the date with today, yesterday and tomorrow
template<> template<>
void to::test<4>()
{
	Parser p;
	// Set the date to: Tue Mar 25 00:00:00 UTC 2008
	p.tnow = 1206403200;

	p.parse("=today");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-25 00:00:00,<=2008-03-25 23:59:59");

	p.parse("=yesterday");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-24 00:00:00,<=2008-03-24 23:59:59");

	p.parse("=tomorrow");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-26 00:00:00,<=2008-03-26 23:59:59");

	// Combine with the time

	p.parse(">=yesterday 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-24 12:00:00");

	p.parse("<=tomorrow 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), "<=2008-03-26 12:00:59");
}

// Add a time offset before date and time
template<> template<>
void to::test<5>()
{
	Parser p;
	// Set the date to: Tue Mar 25 00:00:00 UTC 2008
	p.tnow = 1206403200;

	p.parse(">=3 days after tomorrow 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-29 12:00:00");

	p.parse(">=3 months after tomorrow 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-06-26 12:00:00");

	p.parse(">=3 years after tomorrow 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2011-03-26 12:00:00");

	p.parse(">=3 weeks before tomorrow 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-05 12:00:00");

	p.parse(">=3 seconds before tomorrow 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-26 11:59:57");

	// If you add a month, a month is added, not 30 days
	p.parse(">=1 month after 2007-02-01 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2007-03-01 12:00:00");
}

// Use now instead of date and time
template<> template<>
void to::test<6>()
{
	Parser p;
	// Set the date to: Tue Mar 25 01:02:03 UTC 2008 GMT
	p.tnow = 1206403200 + 3600+120+3;

	// The resulting datetime should be GMT
	p.parse(">=now");
	ensure_equals(p.res[0]->toString(), ">=2008-03-25 01:02:03");
}

// Combine time intervals
template<> template<>
void to::test<7>()
{
	Parser p;
	// Set the date to: Tue Mar 25 00:00:00 UTC 2008
	p.tnow = 1206403200;

	p.parse(">=3 days 5 hours before tomorrow 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-23 07:00:00");

	p.parse(">=3 days 5 hours and 3 minutes before tomorrow 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-23 06:57:00");

	p.parse(">=2 months a week 3 days 5 hours and 3 minutes before tomorrow 12:00");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-01-16 06:57:00");

	// Intervals can also be given with + and -
	p.parse(">=today - 3 days");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-22 00:00:00");

	p.parse(">=today + 48 hours");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-27 00:00:00");

	p.parse(">=today + 1 month - 2 weeks");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-04-11 00:00:00");
}

// 'xxx ago' should always do the right thing with regards to incomplete dates
template<> template<>
void to::test<8>()
{
	Parser p;
	// Set the date to: Tue Mar 25 00:00:00 UTC 2008
	p.tnow = 1206403200;

	p.parse(">=3 years ago");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2005-01-01 00:00:00");

	p.parse(">=2 days ago");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-23 00:00:00");

	p.parse(">=3 hours ago");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-24 21:00:00");

	p.parse(">=3 seconds ago");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-24 23:59:57");
}

// from and until can be used instead of >= and <=
template<> template<>
void to::test<9>()
{
	Parser p;
	// Set the date to: Tue Mar 25 00:00:00 UTC 2008
	p.tnow = 1206403200;

	p.parse("from yesterday");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-24 00:00:00");

	p.parse("until 3 years after today");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), "<=2011-03-25 23:59:59");
}

// 'at' users can use midday, midnight and noon
template<> template<>
void to::test<10>()
{
	Parser p;
	// Set the date to: Tue Mar 25 00:00:00 UTC 2008
	p.tnow = 1206403200;

	p.parse("from 2006-03-20 midday");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2006-03-20 12:00:00");

	p.parse("from yesterday midnight, until tomorrow midday");
	ensure_equals(p.res.size(), 2u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-24 00:00:00");
	ensure_equals(p.res[1]->toString(), "<=2008-03-26 12:00:00");
}

// % can be replaced with 'every'
template<> template<>
void to::test<11>()
{
	Parser p;
	// Set the date to: Tue Mar 25 00:00:00 UTC 2008
	p.tnow = 1206403200;

	p.parse("% 30 minutes");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), "==00:00:00%30m");

	p.parse("every 30 minutes");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), "==00:00:00%30m");

	p.parse("from yesterday midnight every 3 hours");
	ensure_equals(p.res.size(), 2u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-24 00:00:00");
	ensure_equals(p.res[1]->toString(), "%3h");
}

// To be elegant, 'a' or 'an' can be used instead of 1
template<> template<>
void to::test<12>()
{
	Parser p;
	// Set the date to: Tue Mar 25 00:00:00 UTC 2008
	p.tnow = 1206403200;

	p.parse("from a week ago every 3 hours");
	ensure_equals(p.res.size(), 2u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-18 00:00:00");
	ensure_equals(p.res[1]->toString(), "%3h");

	p.parse(">=an hour ago");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-24 23:00:00");
}

// Time intervals within a day still work
template<> template<>
void to::test<13>()
{
	Parser p;
	// Set the date to: Tue Mar 25 00:00:00 UTC 2008
	p.tnow = 1206403200;

	p.parse("=1 month ago, from 03:00, until 18:00, every 1 hour");
	ensure_equals(p.res.size(), 4u);
	ensure_equals(p.res[0]->toString(), ">=2008-02-01 00:00:00,<=2008-02-29 23:59:59");
	ensure_equals(p.res[1]->toString(), ">=03:00:00");
	ensure_equals(p.res[2]->toString(), "<=18:00:59");
	ensure_equals(p.res[3]->toString(), "==00:00:00%1h");
}

// Check Easter-related dates
template<> template<>
void to::test<14>()
{
	Parser p;
	p.parse(">=easter 2008");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2008-03-23 00:00:00");

	// The San Luca procession in Bologna
	p.parse(">=5 weeks after easter 2007 - 1 day");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2007-05-12 00:00:00");

	// The San Luca procession in Bologna, easter egg version
	// It is said that when there is the San Luca procession in Bologna it
	// usually rains.
	p.parse(">=processione san luca 2007");
	ensure_equals(p.res.size(), 1u);
	ensure_equals(p.res[0]->toString(), ">=2007-05-12 00:00:00");
}

}

// vim:set ts=4 sw=4:
