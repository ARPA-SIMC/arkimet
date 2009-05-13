#ifndef ARKI_DATASET_ONDISK_MAINT_DATAFILE_H
#define ARKI_DATASET_ONDISK_MAINT_DATAFILE_H

/*
 * dataset/ondisk/maint/datafile - Handle a data file plus its associated files
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
namespace maint {
class Directory;

/**
 * Manage a data file in the Ondisk dataset, including all accessory files,
 * like metadata file and flagfiles
 */
struct Datafile
{
	Directory* parent;

	// Full path to the data file
	std::string pathname;
	// Path to the data file relative to the dataset root
	std::string relname;

	Datafile(Directory* parent, const std::string& basename);
	~Datafile();

	/// Return the datafile extension
	std::string extension() const;

	/**
	 * Repack the data file removing the data that has been deleted.
	 *
	 * The .metadata file is used as the reference to see what items in the
	 * data file are still valid.
	 *
	 * repack() fails if the rebuild flagfile is present, signaling that the
	 * .metadata file should not be trusted.
	 *
	 * Return the number of data items that have been removed.
	 */
	size_t repack();

	/**
	 * Rebuild the metadata by rescanning the data file.
	 *
	 * This causes the summary to be regenerated as well, and the changes are
	 * propagated to parent summaries.
	 *
	 * The index for all the data in this file is also regenerated.
	 *
	 * In case, during index regeneration, one of the data items turns out to
	 * be duplicated, its corresponding metadata will be marked as deleted, and
	 * the element will be removed from the archive during the next repack.
	 * The metadata of the duplicated element will be sent to the \a salvage
	 * consumer.
	 */
	void rebuild(MetadataConsumer& salvage, bool reindex);

	/**
	 * Reindex the file contents.
	 *
	 * In case, during reindexing, one of the data items turns out to be
	 * duplicated, its corresponding metadata will be marked as deleted, and
	 * the element will be removed from the archive during the next repack.
	 * The metadata of the duplicated element will be sent to the \a salvage
	 * consumer.
	 */
	void reindex(MetadataConsumer& salvage);

	/**
	 * Rebuild the summary for this file
	 */
	void rebuildSummary(bool reindex);
};

}
}
}
}

// vim:set ts=4 sw=4:
#endif
