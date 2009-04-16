#ifndef ARKI_DATASET_ONDISK_WRITER_DATAFILE_H
#define ARKI_DATASET_ONDISK_WRITER_DATAFILE_H

/*
 * dataset/ondisk2/writer/datafile - Handle a data file plus its associated files
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
#include <arki/transaction.h>

namespace arki {
class Metadata;

namespace dataset {
namespace ondisk2 {
namespace writer {

/**
 * Manage a data file in the Ondisk dataset, including all accessory files,
 * like metadata file and flagfiles
 */
struct Datafile
{
	// Full path to the data file
	std::string pathname;
	int fd;

	Datafile(const std::string& pathname);
	~Datafile();

	/**
	 * Append the data to the datafile and return a pending with the cancelable
	 * operation.  The pending is only valid as long as this Datafile is valid.
	 */
	Pending append(Metadata& md, off_t* ofs);
};

}
}
}
}

// vim:set ts=4 sw=4:
#endif
