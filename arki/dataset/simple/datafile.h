#ifndef ARKI_DATASET_SIMLE_DATAFILE_H
#define ARKI_DATASET_SIMLE_DATAFILE_H

/*
 * dataset/simple/datafile - Handle a data file plus its associated files
 *
 * Copyright (C) 2007--2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/summary.h>
#include <arki/metadata/collection.h>
#include <arki/data.h>

namespace arki {
class Metadata;

namespace dataset {
namespace simple {

namespace datafile {

/// Accumulate metadata and summaries while writing
struct MdBuf
{
    std::string pathname;
    std::string basename;
    bool flushed;
    metadata::Collection mds;
    Summary sum;

    MdBuf(const std::string& pathname);
    ~MdBuf();

    void add(const Metadata& md);
    void flush();
};

}

/**
 * Manage a data file in the Ondisk dataset, including all accessory files,
 * like metadata file and flagfiles
 */
struct Datafile
{
    // Full path to the data file
    data::Writer writer;
    datafile::MdBuf mdbuf;

    Datafile(const std::string& pathname, const std::string& format);
    ~Datafile();

	void flush();

	/**
	 * Append the data to the datafile.
	 *
	 * Metadata updates will only be saved when the Datafile is flushed or
	 * destroyed
	 */
	void append(Metadata& md);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
