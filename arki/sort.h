#ifndef ARKI_SORT_H
#define ARKI_SORT_H

/// Sorting routines for metadata

#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/metadata/collection.h>
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
struct Compare
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
    static std::unique_ptr<Compare> parse(const std::string& expr);
};

/// Adaptor to use compare in STL sort functions
struct STLCompare
{
	const Compare& cmp;

	STLCompare(const Compare& cmp) : cmp(cmp) {}

    bool operator()(const std::shared_ptr<Metadata>& a, const std::shared_ptr<Metadata>& b) const
    {
        return cmp.compare(*a, *b) < 0;
    }

    bool operator()(const Metadata& a, const Metadata& b) const
    {
        return cmp.compare(a, b) < 0;
    }

    bool operator()(const Metadata* a, const Metadata* b) const
    {
        return cmp.compare(*a, *b) < 0;
    }
};

class Stream
{
protected:
    const Compare& sorter;
    metadata_dest_func next_dest;
    bool hasInterval;
    std::unique_ptr<core::Time> endofperiod;
    metadata::Collection buffer;

    void setEndOfPeriod(const types::Reftime& rt);

public:
    Stream(const Compare& sorter, metadata_dest_func next_dest)
        : sorter(sorter), next_dest(next_dest)
    {
        hasInterval = sorter.interval() != Compare::NONE;
    }

    bool add(std::shared_ptr<Metadata> md);

    /**
     * Send all currently buffered data to next_dest.
     *
     * Returns true if next_dest always returned true, false if sending was
     * interrupted because of next_dest returning false.
     */
    bool flush();

private:
    Stream(const Stream&);
    Stream& operator=(const Stream&);
};

}
}
#endif
