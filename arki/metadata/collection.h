#ifndef ARKI_METADATA_COLLECTION_H
#define ARKI_METADATA_COLLECTION_H

/*
 * metadata/collection - In-memory collection of metadata
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/dataset.h>
#include <vector>
#include <string>
#include <iosfwd>

namespace arki {
struct Summary;

namespace sort {
struct Compare;
}

namespace metadata {

/**
 * Consumer that collects all metadata into a vector
 */
struct Collection : public std::vector<Metadata>, public Consumer, public ReadonlyDataset
{
	bool operator()(Metadata& md)
	{
		push_back(md);
		back().dropCachedData();
		return true;
	}

	virtual void queryData(const dataset::DataQuery& q, Consumer& consumer);
	virtual void querySummary(const Matcher& matcher, Summary& summary);

	/**
	 * Write all the metadata to a file, atomically, using AtomicWriter
	 */
	void writeAtomically(const std::string& fname) const;

	/**
	 * Append all metadata to the given file
	 */
	void appendTo(const std::string& fname) const;

	/**
	 * Write all metadata to the given output stream
	 */
	void writeTo(std::ostream& out, const std::string& fname) const;

	/**
	 * Send all metadata to a consumer
	 */
	bool sendTo(Consumer& out)
	{
		for (std::vector<Metadata>::iterator i = begin();
				i != end(); ++i)
			if (!out(*i))
				return false;
		return true;
	}

	/**
	 * Ensure that all metadata point to data in the same file and that
	 * they completely cover the file.
	 *
	 * @returns the data file name
	 */
	std::string ensureContiguousData(const std::string& source = std::string("metadata")) const;

	/**
	 * If all the metadata here entirely cover a single data file, replace
	 * it with a compressed version
	 */
	void compressDataFile(size_t groupsize = 512, const std::string& source = std::string("metadata"));

	/// Sort with the given order
	void sort(const sort::Compare& cmp);
	void sort(const std::string& cmp);
	void sort(); // Sort by reftime
};

}
}

// vim:set ts=4 sw=4:
#endif
