/*
 * utils/files - arkimet-specific file functions
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/utils/files.h>
#include <arki/utils.h>
#include <wibble/string.h>

#include <sys/stat.h>
#include <unistd.h>
#include <cstdio>
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

std::string readFile(const std::string &file)
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

}
}
}
// vim:set ts=4 sw=4:
