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


time_t timestamp(const std::string& file)
{
    std::auto_ptr<struct stat64> st = sys::fs::stat(file);
    return st.get() == NULL ? 0 : st->st_mtime;
}

size_t size(const std::string& file)
{
    std::auto_ptr<struct stat64> st = sys::fs::stat(file);
    return st.get() == NULL ? 0 : (size_t)st->st_size;
}

ino_t inode(const std::string& file)
{
    std::auto_ptr<struct stat64> st = sys::fs::stat(file);
    return st.get() == NULL ? 0 : st->st_ino;
}

std::string read_file(const std::string &file)
{
    if (file == "-")
    {
        string res;
        int c;
        while ((c = getc(stdin)) != EOF)
            res.append(1, c);
        return res;
    }
    else
        return sys::fs::readFile(file);
}

void write_file(const std::string &file, const std::string& contents)
{
    char fbuf[file.size() + 7];
    memcpy(fbuf, file.data(), file.size());
    memcpy(fbuf + file.size(), "XXXXXX", 7);
    int fd = mkstemp(fbuf);
    if (fd < 0)
        throw wibble::exception::System("creating temp file " + string(fbuf));
    ssize_t res = write(fd, contents.data(), contents.size());
    if (res != (ssize_t)contents.size())
        throw wibble::exception::System(str::fmtf("writing %d bytes to %s", contents.size(), fbuf));
    if (close(fd) < 0)
        throw wibble::exception::System("closing file " + string(fbuf));
    if (rename(fbuf, file.c_str()) < 0)
        throw wibble::exception::System("renaming file " + string(fbuf) + " to " + file);
}

std::string find_executable(const std::string& name)
{
    // argv[0] has an explicit path: ensure it becomes absolute
    if (name.find('/') != string::npos)
        return sys::fs::abspath(name);

    // argv[0] has no explicit path, look for it in $PATH
    const char* path = getenv("PATH");
    if (path == NULL)
        return name;
    str::Split splitter(":", path);
    for (str::Split::const_iterator i = splitter.begin(); i != splitter.end(); ++i)
    {
        string candidate = str::joinpath(*i, name);
        if (sys::fs::access(candidate, X_OK))
            return sys::fs::abspath(candidate);
    }

    return name;
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

}
}
}
// vim:set ts=4 sw=4:
