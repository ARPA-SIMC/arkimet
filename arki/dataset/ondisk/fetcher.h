#ifndef ARKI_DATASET_ONDISK_DSFETCHER_H
#define ARKI_DATASET_ONDISK_DSFETCHER_H

/*
 * dataset/ondisk/fetcher - Optimised data fetcher
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
#include <fstream>
#include <arki/metadata.h>

namespace arki {
class MetadataConsumer;

namespace dataset {
namespace ondisk {

/**
 * Fetch many metadata items from a dataset
 */
class Fetcher
{
	// Root directory
	std::string m_root;
	// File currently been read (or empty)
	std::string m_cur;
	// Currently open stream
	std::ifstream m_in;
	// True if this file has problems and needs to be skipped
	bool skipFile;
	// Reused Metadata object
	Metadata m_md;

public:
	Fetcher(const std::string& root);
	~Fetcher();

	/**
	 * Fetch the metadata element at the given file (pathname relative to root
	 * directory) and offset
	 */
	void fetch(const std::string& file, int offset, MetadataConsumer& out);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
