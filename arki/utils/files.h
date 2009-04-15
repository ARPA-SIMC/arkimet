#ifndef ARKI_DATASET_ONDISK_COMMON_H
#define ARKI_DATASET_ONDISK_COMMON_H

/*
 * dataset/ondisk/common - Common code for ondisk implementation
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

#define FLAGFILE_REBUILD ".needs-rebuild"
#define FLAGFILE_PACK ".needs-pack"
#define FLAGFILE_INDEX "index-out-of-sync"

namespace arki {
namespace utils {
namespace files {

// Flagfile handling

/// Create an empty file, succeeding if it already exists
void createRebuildFlagfile(const std::string& pathname);

/// Create an empty file, failing if it already exists
void createNewRebuildFlagfile(const std::string& pathname);

/// Remove a file, succeeding if it does not exists
void removeRebuildFlagfile(const std::string& pathname);

/// Check if a file exists
bool hasRebuildFlagfile(const std::string& pathname);


/// Create an empty file, succeeding if it already exists
void createPackFlagfile(const std::string& pathname);

/// Create an empty file, failing if it already exists
void createNewPackFlagfile(const std::string& pathname);

/// Remove a file, succeeding if it does not exists
void removePackFlagfile(const std::string& pathname);

/// Check if a file exists
bool hasPackFlagfile(const std::string& pathname);


/// Create an empty file, succeeding if it already exists
void createIndexFlagfile(const std::string& dir);

/// Create an empty file, failing if it already exists
void createNewIndexFlagfile(const std::string& dir);

/// Remove a file, succeeding if it does not exists
void removeIndexFlagfile(const std::string& dir);

/// Check if a file exists
bool hasIndexFlagfile(const std::string& dir);


// Timestamps
time_t timestamp(const std::string& file);

}
}
}

// vim:set ts=4 sw=4:
#endif
