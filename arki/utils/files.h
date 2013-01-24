#ifndef ARKI_UTILS_FILES_H
#define ARKI_UTILS_FILES_H

/*
 * utils/files - arkimet-specific file functions
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <sys/types.h>

#define FLAGFILE_REBUILD ".needs-rebuild"
#define FLAGFILE_PACK ".needs-pack"
#define FLAGFILE_INDEX "index-out-of-sync"
#define FLAGFILE_DONTPACK "needs-check-do-not-pack"

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


/// Create an empty file, succeeding if it already exists
void createDontpackFlagfile(const std::string& dir);

/// Create an empty file, failing if it already exists
void createNewDontpackFlagfile(const std::string& dir);

/// Remove a file, succeeding if it does not exists
void removeDontpackFlagfile(const std::string& dir);

/// Check if a file exists
bool hasDontpackFlagfile(const std::string& dir);


/// File mtime (or 0 if the file does not exist)
time_t timestamp(const std::string& file);

/// File size (or 0 if the file does not exist)
size_t size(const std::string& file);

/// File inode number (or 0 if the file does not exist)
ino_t inode(const std::string& file);

/**
 * Same as wibble::sys::fs::readFile, but if \a file is "-" then reads all from
 * stdin
 */
std::string read_file(const std::string &file);

/**
 * Atomically writes a string to a file, replacing all existing contents
 */
void write_file(const std::string &file, const std::string& contents);

/**
 * Compute the absolute path of an executable.
 *
 * If \a name is specified as a partial path, it ensures it is made absolute.
 * If \a name is not specified as a path, it looks for the executable in $PATH
 * and return its absolute pathname.
 */
std::string find_executable(const std::string& name);

/**
 * Normalise a pathname and resolve it in the file system.
 *
 * @param pathname
 *   The path name to resolve. It can be absolute or relative.
 * @retval basedir
 *   The base directory to use with str::joinpath to make the file name absolute.
 *   It is set to the empty string if \a pathname is an absolute path
 * @retval relname
 *   The normalised version of \a pathname
 */
void resolve_path(const std::string& pathname, std::string& basedir, std::string& relname);

}
}
}

// vim:set ts=4 sw=4:
#endif
