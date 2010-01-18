#ifndef ARKI_DATASET_GRIDSPACE_H
#define ARKI_DATASET_GRIDSPACE_H

/*
 * dataset/gridspace - Filter another dataset over a dense data grid
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset.h>
#include <arki/types.h>
#include <arki/types/time.h>
#include <arki/matcher.h>
#include <arki/utils/metadata.h>
#include <string>
#include <vector>
#include <set>
#include <map>

namespace arki {

class ConfigFile;
class Metadata;
class MetadataConsumer;
class Matcher;

namespace dataset {

namespace gridspace {

struct UnresolvedMatcher : public std::string
{
	std::set< Item<> > candidates;
	Matcher matcher;

	UnresolvedMatcher(types::Code code, const std::string& expr)
		: std::string(expr), matcher(Matcher::parse(types::tag(code) + ":" + expr)) {}
};

/**
 * Define a linear metadata space.
 *
 * For every metadata type in the soup, we have a number of elements we want.
 *
 * Every metadata code in the soup defines a dimension of a matrix. The number
 * of items requested for that metadata code defines the number of elements of
 * the matrix across that dimension.
 *
 * Since the metadata codes are sorted, and the items are sorted, and the soup
 * (that is, the topology of the matrix) will not change, we can define an
 * unchanging bijective function between the matrix elements and a segment in
 * N. This means that we can associate to each matrix element an integer number
 * between 0 and the number of items in the matrix.
 *
 * What is required at the end of the game is to have one and only one metadata
 * item per matrix element.
 */
struct MDGrid
{
	std::map<types::Code, std::vector< Item<> > > soup;
	std::map<types::Code, std::vector<UnresolvedMatcher> > oneMatchers;
	std::map<types::Code, std::vector<UnresolvedMatcher> > allMatchers;
	std::map<types::Code, std::vector<std::string> > extraMatchers;
	utils::metadata::Collector mds;
	std::vector<size_t> dim_sizes;
	size_t maxidx;
	bool all_local;

	// Find the linearised matrix index for md. Returns -1 if md does not
	// match a point in the matrix
	int index(const Metadata& md) const;

	// Expand an index in its corresponding set of metadata
	std::vector< Item<> > expand(size_t index) const;

	// Create the minimal arkimet query that matches at least the whole
	// data space
	std::string make_query() const;

	/**
	 * Resolve matchers into metadata.
	 *
	 * For every unresolved matcher, check it against the dataset summary
	 * to find a single matching metadata item.
	 *
	 * Returns true if all matchers have been resolved to only one item,
	 * false if there are some ambiguous matchers.
	 *
	 * This method calls consolidate(), therefore altering the matrix
	 * space and invalidating indices.
	 */
	bool resolveMatchers(ReadonlyDataset& rd);

	// Clear the grid space, starting afresh
	void clear();

	// Add an item to the grid space
	void add(const Item<>& item);

	// Add a match expression (to be resolved in one item) to the grid
	// space
	void addOne(types::Code code, const std::string& expr);

	// Add a match expression (to be resolved in all existing matching
	// items) to the grid space
	void addAll(types::Code code, const std::string& expr);

	// Add a discrete time sequence (@see types::reftime::Position::generate())
	void addTimeInterval(const Item<types::Time>& begin, const Item<types::Time>& end, int step);

	/**
	 * Read metadata items and matchers from a file descriptor
	 *
	 * The input file is parsed one line at a time. Empty lines are
	 * ignored.
	 *
	 * A line in the form:
	 *   type: value
	 * Enters a metadata item to the appropriate grid dimension.
	 *
	 * A line in the form:
	 *   match type: expr
	 * Enters a metadata item that matches the given arkimet query.
	 */
	void read(std::istream& input, const std::string& fname);

	/**
	 * Consolidate the soup space: remove duplicates, sort the vectors
	 *
	 * This can be called more than once if the space changes, but it
	 * invalidates the previous linearisation of the matrix space, which
	 * means that all previously generated indices are to be considered
	 * invalid.
	 */
	void consolidate();
};


}

/**
 * Filter another dataset over a dense data grid.
 *
 * The resulting filtered dataset is something similar to the result set of
 * ECMWF Mars or ARPA/SIM xgrib.
 */
class Gridspace : public ReadonlyDataset
{
protected:
	ReadonlyDataset& nextds;
	gridspace::MDGrid mdgrid;

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	Gridspace(ReadonlyDataset& nextds);
	virtual ~Gridspace();

	// Clear the grid space, starting afresh
	void clear() { mdgrid.clear(); }

	// Add an item to the grid space
	void add(const Item<>& item) { mdgrid.add(item); }

	// Add a match expression (to be resolved in one item) to the grid
	// space
	void addOne(types::Code code, const std::string& expr)
	{
		mdgrid.addOne(code, expr);
	}

	// Add a match expression (to be resolved in all existing matching
	// items) to the grid space
	void addAll(types::Code code, const std::string& expr)
	{
		mdgrid.addAll(code, expr);
	}

	// Add a discrete time sequence (@see types::reftime::Position::generate())
	void addTimeInterval(const Item<types::Time>& begin, const Item<types::Time>& end, int step)
	{
		mdgrid.addTimeInterval(begin, end, step);
	}


	/**
	 * Read grid space information from the given input stream
	 *
	 * @see MDGrid::read()
	 */
	void read(std::istream& input, const std::string& fname)
	{
		mdgrid.read(input, fname);
	}

	/**
	 * Validate the grid space against the dataset contents
	 *
	 * This needs to be called before querying the dataset
	 */
	void validate();

	/**
	 * Query the dataset using the given matcher, and sending the results to
	 * the metadata consumer.
	 */
	virtual void queryData(const dataset::DataQuery& q, MetadataConsumer& consumer);
	virtual void querySummary(const Matcher& matcher, Summary& summary);
	virtual void queryBytes(const dataset::ByteQuery& q, std::ostream& out);
};

}
}

// vim:set ts=4 sw=4:
#endif
