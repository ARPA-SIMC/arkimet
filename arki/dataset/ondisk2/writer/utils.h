#ifndef ARKI_DATASET_ONDISK_WRITER_UTILS_H
#define ARKI_DATASET_ONDISK_WRITER_UTILS_H

/*
 * dataset/ondisk2/writer/utils - ondisk2 maintenance utilities
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

#include <string>
#include <vector>

namespace arki {
namespace dataset {
namespace ondisk2 {
namespace writer {

/**
 * Scan a dataset for data files, with a pull-style interface.
 *
 * The sequence of pathnames extracted by DirScanner is always sorted.
 */
class DirScanner
{
protected:
	std::string m_root;

	std::vector<std::string> names;

	void scan(const std::string& root, int level = 0);

public:
	DirScanner(const std::string& root);

	/**
	 * Return the pathname (relative to root) of the current file, or the
	 * empty string if the iteration is finished.
	 */
	std::string cur() const;

	/**
	 * Advance to the next file
	 */
	void next();
};


}
}
}
}

// vim:set ts=4 sw=4:
#endif
