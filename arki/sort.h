#ifndef ARKI_SORT_H
#define ARKI_SORT_H

/*
 * arki/sort - Sorting routines for metadata
 *
 * Copyright (C) 2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types.h>
#include <arki/types/reftime.h>
#include <arki/metadata.h>
#include <string>
#include <vector>

namespace arki {
namespace sort {

/**
 * Compare metadata according to a custom sort order
 */
class Order
{
protected:
	struct SortOrder
	{
		types::Code code;
		bool reverse;

		SortOrder(types::Code code, bool reverse) : code(code), reverse(reverse) {}
	};

	std::vector<SortOrder> order;

public:
	void addOrder(types::Code code, bool reverse=false)
	{
		order.push_back(SortOrder(code, reverse));
	}

	bool operator()(const Metadata& a, const Metadata& b)
	{
		for (size_t i = 0; i < order.size(); ++i)
		{
			int cmp = a.get(order[i].code).compare(b.get(order[i].code));
			if (order[i].reverse) cmp = -cmp;
			if (cmp < 0) return true;
			if (cmp > 0) return false;
		}
		return false;
	}
};

class TimeIntervalSorter : public MetadataConsumer
{
public:
	enum Period {
		MINUTE,
		HOUR,
		DAY,
		MONTH,
		YEAR
	};

	Period parsePeriod(const std::string& p);

protected:
	MetadataConsumer& nextConsumer;
	UItem<types::Reftime> endofperiod;
	Order sorter;
	std::vector<Metadata> buffer;
	Period period;

	void setEndOfPeriod(const UItem<types::Reftime>& rt);

public:
	TimeIntervalSorter(MetadataConsumer& nextConsumer, Period period)
		: nextConsumer(nextConsumer), period(period)
	{
	}
	TimeIntervalSorter(MetadataConsumer& nextConsumer, const std::string& desc);
	~TimeIntervalSorter()
	{
		flush();
	}

	void flush();

	void addOrder(types::Code code, bool reverse=false)
	{
		sorter.addOrder(code, reverse);
	}

	virtual bool operator()(Metadata& m);
};

}
}

// vim:set ts=4 sw=4:
#endif
