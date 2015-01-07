#ifndef ARKI_SORT_H
#define ARKI_SORT_H

/*
 * arki/sort - Sorting routines for metadata
 *
 * Copyright (C) 2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/refcounted.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/types/reftime.h>

#include <string>
#include <vector>
#include <memory>

namespace arki {
class Metadata;

namespace sort {

/**
 * Interface for metadata sort functors
 *
 * Syntax: (interval:)?([+-]?metadata)(,[+-]?metadata)*
 *
 * An empty expression sorts by reftime.
 *
 * An expression without "interval:" sorts by the first metadata, then by the
 * second, and so on. A '-' before a metadata means sort descending.
 *
 * If an interval is specified, data is grouped in the given time intervals,
 * then every group is sorted independently from the others.
 */
struct Compare : public refcounted::Base
{
	/// Allowed types of sort intervals
	enum Interval {
		NONE,
		MINUTE,
		HOUR,
		DAY,
		MONTH,
		YEAR
	};

	virtual ~Compare() {}

	/// Comparison function for metadata
	virtual int compare(const Metadata& a, const Metadata& b) const = 0;

	/// Return the sort interval
	virtual Interval interval() const { return NONE; }

	/// Compute the string representation of this sorter
	virtual std::string toString() const = 0;

	/// Parse a string representation into a sorter
	static refcounted::Pointer<Compare> parse(const std::string& expr);
};

/// Adaptor to use compare in STL sort functions
struct STLCompare
{
	const Compare& cmp;

	STLCompare(const Compare& cmp) : cmp(cmp) {}

	bool operator()(const Metadata& a, const Metadata& b) const
	{
		return cmp.compare(a, b) < 0;
	}
};

class Stream : public metadata::Consumer
{
protected:
	const Compare& sorter;
	metadata::Consumer& nextConsumer;
	bool hasInterval;
    types::Time endofperiod;
    std::vector<Metadata> buffer;

    void setEndOfPeriod(const types::Reftime& rt);

public:
	Stream(const Compare& sorter, metadata::Consumer& nextConsumer)
		: sorter(sorter), nextConsumer(nextConsumer)
	{
		hasInterval = sorter.interval() != Compare::NONE;
	}
	~Stream() { flush(); }

	virtual bool operator()(Metadata& m);

	void flush();
};

}
}

// vim:set ts=4 sw=4:
#endif
