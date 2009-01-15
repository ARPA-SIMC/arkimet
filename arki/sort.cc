/*
 * arki/sort - Sorting routines for metadata
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

#include <arki/sort.h>
#include <arki/metadata.h>
#include <arki/types/reftime.h>
#include <arki/metadata.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <wibble/grcal/grcal.h>

#include <cstring>

using namespace std;
using namespace wibble;

namespace arki {
namespace sort {

TimeIntervalSorter::TimeIntervalSorter(MetadataConsumer& nextConsumer, const std::string& desc)
	: nextConsumer(nextConsumer)
{
	size_t pos = desc.find(':');
	if (pos == string::npos)
		throw wibble::exception::Consistency("parsing sorting specification", "'"+desc+"' does not contain ':'");

	period = parsePeriod(desc.substr(0, pos));

	string order = str::trim(desc.substr(pos+1));
	Splitter sp("[[:space:]]*([[:space:]]|,)[[:space:]]*", REG_EXTENDED);
	for (Splitter::const_iterator i = sp.begin(order);
			i != sp.end(); ++i)
		if ((*i)[0] == '-')
			addOrder(types::parseCodeName(i->substr(1)), true);
		else
			addOrder(types::parseCodeName(*i), false);
}

TimeIntervalSorter::Period TimeIntervalSorter::parsePeriod(const std::string& p)
{
	string lp = str::tolower(p);
	if (lp == "minute") return MINUTE;
	if (lp == "hour") return HOUR;
	if (lp == "day") return DAY;
	if (lp == "month") return MONTH;
	if (lp == "year") return YEAR;
	throw wibble::exception::Consistency("parsing period", "invalid period: " + p + ".  Valid periods are minute, hour, day, month and year");
}

void TimeIntervalSorter::setEndOfPeriod(const UItem<types::Reftime>& rt)
{
	UItem<types::Time> t;
	switch (rt->style())
	{
		case types::Reftime::POSITION: t = rt.upcast<types::reftime::Position>()->time; break;
		case types::Reftime::PERIOD: t = rt.upcast<types::reftime::Period>()->end; break;
		default:
			throw wibble::exception::Consistency("setting end of period", "reference time has invalid style: " + types::Reftime::formatStyle(rt->style()));
	}
	int vals[6];
	memcpy(vals, t->vals, 6 * sizeof(int));
	switch (period)
	{
		case YEAR: vals[1] = -1;
		case MONTH: vals[2] = -1;
		case DAY: vals[3] = -1;
		case HOUR: vals[4] = -1;
		case MINUTE: vals[5] = -1; break;
		default:
			throw wibble::exception::Consistency("setting end of period", "period type has invalid value: " + (int)period);
	}
	wibble::grcal::date::upperbound(vals);
	endofperiod = types::reftime::Position::create(types::Time::create(vals));
}

void TimeIntervalSorter::flush()
{
	if (buffer.empty()) return;
	std::sort(buffer.begin(), buffer.end(), sorter);
	for (vector<Metadata>::iterator i = buffer.begin();
			i != buffer.end(); ++i)
		nextConsumer(*i);
	buffer.clear();
}

bool TimeIntervalSorter::operator()(Metadata& m)
{
	if (m.get(types::TYPE_REFTIME) > endofperiod)
	{
		flush();
		buffer.push_back(m);
		setEndOfPeriod(m.get(types::TYPE_REFTIME).upcast<types::Reftime>());
	}
	else
		buffer.push_back(m);
	return true;
}

}
}

// vim:set ts=4 sw=4:
