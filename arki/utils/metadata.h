#ifndef ARKI_UTILS_METADATA_H
#define ARKI_UTILS_METADATA_H

/*
 * utils/metadata - Useful functions for working with metadata
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset.h>
#include <vector>
#include <string>
#include <iosfwd>

namespace arki {
struct Summary;

namespace utils {
namespace metadata {

/**
 * Consumer that collects all metadata into a vector
 */
struct Collector : public std::vector<Metadata>, public MetadataConsumer, public ReadonlyDataset
{
	bool operator()(Metadata& md)
	{
		push_back(md);
		return true;
	}

	virtual void queryData(const dataset::DataQuery& q, MetadataConsumer& consumer);
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
	 * Send all metadata to a consumer
	 */
	bool sendTo(MetadataConsumer& out)
	{
		for (std::vector<Metadata>::iterator i = begin();
				i != end(); ++i)
			if (!out(*i))
				return false;
		return true;
	}
};

/**
 * Write metadata to a file, atomically.
 *
 * The file will be created with a temporary name, and then renamed to its
 * final name.
 *
 * Note: the temporary file name will NOT be created securely.
 */
struct AtomicWriter : public MetadataConsumer
{
	std::string fname;
	std::string tmpfname;
	std::ofstream* outmd;

	AtomicWriter(const std::string& fname);
	~AtomicWriter();

	bool operator()(Metadata& md);
	bool operator()(const Metadata& md);

	void close();
};

struct Summarise : public MetadataConsumer
{
	Summary& s;
	Summarise(Summary& s) : s(s) {}

	bool operator()(Metadata& md);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
