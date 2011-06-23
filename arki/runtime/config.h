#ifndef ARKI_RUNTIME_CONFIG_H
#define ARKI_RUNTIME_CONFIG_H

/*
 * runtime/config - Common configuration-related code used in most arkimet executables
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

#include <wibble/commandline/parser.h>
#include <arki/configfile.h>
#include <string>
#include <vector>

namespace arki {
namespace runtime {

/**
 * Hold runtime configuration read from compile-time defaults and environment
 * variables
 */
struct Config
{
    /**
     * Regular string vector with extra convenience methods for file lookup
     */
    struct Dirlist : public std::vector<std::string>
    {
        /**
         * Look for the file in all directories.
         *
         * @return the pathname if found, raises an exception if not found
         */
        std::string find_file(const std::string& fname, bool executable=false) const;

        /**
         * Look for the file in all directories.
         *
         * @return the pathname if found, or the empty string if not found
         */
        std::string find_file_noerror(const std::string& fname, bool executable=false) const;

        /**
         * List the files if the first directory found
         *
         * The file list is sorted by directory order and then by file name.
         *
         * @param ext
         *   The extension (including the dot) that files must have to be considered
         * @param first_only
         *   If true, limit the list to the first directory found
         */
        std::vector<std::string> list_files(const std::string& ext, bool first_only=true) const;
    };
    /// Directories where postprocessor executables are found
    Dirlist dir_postproc;

    /// Directories where report scripts are found
    Dirlist dir_report;

    /// Directories where query macro scripts are found
    Dirlist dir_qmacro;

    /// Directories where grib1 scan scripts are found
    Dirlist dir_scan_grib1;

    /// Directories where grib2 scan scripts are found
    Dirlist dir_scan_grib2;

    /// Directories where bufr scan scripts are found
    Dirlist dir_scan_bufr;

    /// Directories where targetfile scripts are found
    Dirlist dir_targetfile;

    /// Temporary file directory
    std::string dir_temp;

    Config();

    /// Get the runtime configuration
    static Config& get();
};

/**
 * Parse the config file with the given name into the ConfigFile object 'cfg'.
 *
 * Note: use ReadonlyDataset::readConfig to read dataset configuration
 */
void parseConfigFile(ConfigFile& cfg, const std::string& fileName);

/**
 * Parse the config files from the remaining commandline arguments
 *
 * Return true if at least one config file was found in \a opts
 */
bool parseConfigFiles(ConfigFile& cfg, wibble::commandline::Parser& opts);

/**
 * Parse the config files indicated by the given commandline option.
 *
 * Return true if at least one config file was found in \a files
 */
bool parseConfigFiles(ConfigFile& cfg, const wibble::commandline::VectorOption<wibble::commandline::String>& files);

/**
 * Parse a comma separated restrict list into a set of strings
 */
std::set<std::string> parseRestrict(const std::string& str);

struct Restrict
{
	std::set<std::string> wanted;

	Restrict(const std::string& str) : wanted(parseRestrict(str)) {}

	bool is_allowed(const std::string& str);
	bool is_allowed(const std::set<std::string>& names);
	bool is_allowed(const ConfigFile& cfg);
	void remove_unallowed(ConfigFile& cfg);
};

/**
 * Read the Matcher alias database.
 *
 * The file named in the given StringOption (if any) is tried first.
 * Otherwise the file given in the environment variable ARKI_ALIASES is tried.
 * Else, $(sysconfdir)/arkimet/match-alias.conf is tried.
 * Else, nothing is loaded.
 *
 * The alias database is kept statically for all the lifetime of the program,
 * and is automatically used by readQuery.
 */
void readMatcherAliasDatabase(wibble::commandline::StringOption* file = 0);

/**
 * Read the content of an rc directory, returning all the files found, sorted
 * alphabetically by name.
 */
std::vector<std::string> rcFiles(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dir = 0);

/**
 * Read the content of an rc directory, by concatenating all non-hidden,
 * non-backup files in it, sorted alphabetically by name
 */
std::string readRcDir(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dir = 0);

struct FileInfo
{
	std::string pathname;
	size_t length;

	FileInfo(const std::string& pathname, const size_t& length):
		pathname(pathname), length(length) {}
};

struct SourceCode : public std::vector<FileInfo>
{
	std::string code;
};

/**
 * Read the content of an rc directory, by concatenating all non-hidden,
 * non-backup files in it, sorted alphabetically by name.  It also returns
 * information about what parts of the string belong to what file, which can be
 * used to map a string location to its original position.
 */
SourceCode readSourceFromRcDir(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dir = 0);

}
}

// vim:set ts=4 sw=4:
#endif
