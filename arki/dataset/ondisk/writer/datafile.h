#ifndef ARKI_DATASET_ONDISK_WRITER_DATAFILE_H
#define ARKI_DATASET_ONDISK_WRITER_DATAFILE_H

/*
 * dataset/ondisk/writer/datafile - Handle a data file plus its associated files
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

#include <string>

namespace arki {
class Metadata;
class MetadataConsumer;
class Summary;

namespace dataset {
namespace ondisk {
namespace writer {
class Directory;

/**
 * Manage a data file in the Ondisk dataset, including all accessory files,
 * like metadata file and flagfiles
 */
struct Datafile
{
	struct NormalAccess;
	struct SummaryAccess;

	Directory* parent;
	std::string name;
	std::string ext;
	// Full path to the data file
	std::string pathname;
	// Path to the data file relative to the dataset root
	std::string relname;
	NormalAccess* na;
	SummaryAccess* sa;

	bool summaryIsInvalid;

	Datafile(Directory* parent, const std::string& name, const std::string& ext);
	~Datafile();

	/**
	 * Deallocate all cached structures
	 */
	void flushCached();

	/**
	 * Return the offset at which a future metadata will be inserted
	 */
	off_t nextOffset();

	/**
	 * Append the metadata and its data to the datafile and associated metadata
	 * file
	 */
	void append(Metadata& md);

	/**
	 * Mark the metadata at oldofs as deleted.
	 *
	 * Note that the old data will be left in the file.  In order to get rid of
	 * the gaps, you have to pack the datafile.
	 */
	void remove(size_t oldofs);

	// Get the summary for this datafile
	const Summary& summary();
};

}
}
}
}

// vim:set ts=4 sw=4:
#endif
