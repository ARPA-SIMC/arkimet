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

#include "config.h"

#include <arki/utils/files.h>
#include <arki/utils.h>
#include <wibble/string.h>
#include <wibble/sys/process.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>


using namespace std;
using namespace wibble;

namespace arki {
namespace utils {
namespace files {

void createRebuildFlagfile(const std::string& pathname)
{
	utils::createFlagfile(pathname + FLAGFILE_REBUILD);
}
void createNewRebuildFlagfile(const std::string& pathname)
{
	utils::createNewFlagfile(pathname + FLAGFILE_REBUILD);
}
void removeRebuildFlagfile(const std::string& pathname)
{
    sys::fs::deleteIfExists(pathname + FLAGFILE_REBUILD);
}
bool hasRebuildFlagfile(const std::string& pathname)
{
    return sys::fs::exists(pathname + FLAGFILE_REBUILD);
}


void createPackFlagfile(const std::string& pathname)
{
	utils::createFlagfile(pathname + FLAGFILE_PACK);
}
void createNewPackFlagfile(const std::string& pathname)
{
	utils::createNewFlagfile(pathname + FLAGFILE_PACK);
}
void removePackFlagfile(const std::string& pathname)
{
    sys::fs::deleteIfExists(pathname + FLAGFILE_PACK);
}
bool hasPackFlagfile(const std::string& pathname)
{
    return sys::fs::exists(pathname + FLAGFILE_PACK);
}


void createIndexFlagfile(const std::string& dir)
{
	utils::createFlagfile(str::joinpath(dir, FLAGFILE_INDEX));
}
void createNewIndexFlagfile(const std::string& dir)
{
	utils::createNewFlagfile(str::joinpath(dir, FLAGFILE_INDEX));
}
void removeIndexFlagfile(const std::string& dir)
{
    sys::fs::deleteIfExists(str::joinpath(dir, FLAGFILE_INDEX));
}
bool hasIndexFlagfile(const std::string& dir)
{
    return sys::fs::exists(str::joinpath(dir, FLAGFILE_INDEX));
}


void createDontpackFlagfile(const std::string& dir)
{
	utils::createFlagfile(str::joinpath(dir, FLAGFILE_DONTPACK));
}
void createNewDontpackFlagfile(const std::string& dir)
{
	utils::createNewFlagfile(str::joinpath(dir, FLAGFILE_DONTPACK));
}
void removeDontpackFlagfile(const std::string& dir)
{
    sys::fs::deleteIfExists(str::joinpath(dir, FLAGFILE_DONTPACK));
}
bool hasDontpackFlagfile(const std::string& dir)
{
    return sys::fs::exists(str::joinpath(dir, FLAGFILE_DONTPACK));
}

std::string read_file(const std::string &file)
{
    if (file == "-")
        return sys::fs::readFile(cin, "(stdin)");
    else
        return sys::fs::readFile(file);
}

void resolve_path(const std::string& pathname, std::string& basedir, std::string& relname)
{
    if (pathname[0] == '/')
        basedir.clear();
    else
        basedir = sys::process::getcwd();
    relname = str::normpath(pathname);
}

string normaliseFormat(const std::string& format)
{
    string f = str::tolower(format);
    if (f == "metadata") return "arkimet";
    if (f == "grib1") return "grib";
    if (f == "grib2") return "grib";
#ifdef HAVE_ODIMH5
    if (f == "h5")     return "odimh5";
    if (f == "hdf5")   return "odimh5";
    if (f == "odim")   return "odimh5";
    if (f == "odimh5") return "odimh5";
#endif
    return f;
}

std::string format_from_ext(const std::string& fname, const char* default_format)
{
    // Extract the extension
    size_t epos = fname.rfind('.');
    if (epos != string::npos)
        return normaliseFormat(fname.substr(epos + 1));
    else if (default_format)
        return default_format;
    else
        throw wibble::exception::Consistency(
                "auto-detecting format from file name " + fname,
                "file extension not recognised");
}

PreserveFileTimes::PreserveFileTimes(const std::string& fname)
    : fname(fname), fd(-1)
{
    fd = open(fname.c_str(), O_RDWR);
    if (fd == -1)
        throw wibble::exception::File(fname, "cannot open file");

    struct stat st;
    if (fstat(fd, &st) == -1)
        throw wibble::exception::File(fname, "cannot stat file");

    times[0] = st.st_atim;
    times[1] = st.st_mtim;
}

PreserveFileTimes::~PreserveFileTimes()
{
    if (fd == -1) return;
    if (futimens(fd, times) == -1)
        throw wibble::exception::File(fname, "cannot set file times");
    if (close(fd) == -1)
        throw wibble::exception::File(fname, "cannot close file");
}

}
}
}
// vim:set ts=4 sw=4:
