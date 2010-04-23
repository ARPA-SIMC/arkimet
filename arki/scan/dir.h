#ifndef ARKI_SCAN_DIR_H
#define ARKI_SCAN_DIR_H

/*
 * scan/dir - Find data files inside directories
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

#include <string>
#include <vector>

namespace arki {
namespace scan {

/**
 * Scan a dataset for data files, with a pull-style interface.
 *
 * The sequence of pathnames extracted by DirScanner is always sorted.
 */
std::vector<std::string> dir(const std::string& root, bool files_in_root=false);

}
}

// vim:set ts=4 sw=4:
#endif
