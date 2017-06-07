#ifndef ARKI_METADATAGRID_H
#define ARKI_METADATAGRID_H

/*
 * metadatagrid - Index values by a combination of metadata
 *
 * Copyright (C) 2010--2015  ARPAE-SIMC <simc-urp@arpae.it>
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

#include <arki/itemset.h>
#include <arki/types/typevector.h>
#include <string>
#include <vector>
#include <set>
#include <map>

namespace arki {
class Metadata;

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
struct MetadataGrid
{
    /**
     * n-dimensional metadata space, each types::Code is a dimension, the item
     * vector represents the coordinates along that dimension
     */
    std::map<types::Code, types::TypeVector> dims;
    /// Length of each axis of the n-dimensional metadata space
    std::vector<size_t> dim_sizes;

	MetadataGrid();

	// Find the linearised matrix index for md. Returns -1 if md does not
	// match a point in the matrix
	int index(const ItemSet& md) const;

    // Expand an index in its corresponding set of metadata
    types::TypeVector expand(size_t index) const;

	// Create the minimal arkimet query that matches at least the whole
	// data space
	std::string make_query() const;

	// Clear the metadata grid, starting afresh
	void clear();

    // Add an item to the grid space
    void add(const types::Type& item);

	// Add all items from an itemset
	void add(const ItemSet& is);

	/**
	 * Consolidate the grid: remove duplicates, sort the vectors
	 *
	 * This can be called more than once if the space changes, but it
	 * invalidates the previous linearisation of the matrix space, which
	 * means that all previously generated indices are to be considered
	 * invalid.
	 */
	void consolidate();
};

}

// vim:set ts=4 sw=4:
#endif
