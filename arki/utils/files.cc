/*
 * utils/files - arkimet-specific file functions
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

#include <arki/utils/files.h>
#include <arki/utils.h>
#include <wibble/string.h>

#include <sys/stat.h>
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
	utils::removeFlagfile(pathname + FLAGFILE_REBUILD);
}
bool hasRebuildFlagfile(const std::string& pathname)
{
	return utils::hasFlagfile(pathname + FLAGFILE_REBUILD);
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
	utils::removeFlagfile(pathname + FLAGFILE_PACK);
}
bool hasPackFlagfile(const std::string& pathname)
{
	return utils::hasFlagfile(pathname + FLAGFILE_PACK);
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
	utils::removeFlagfile(str::joinpath(dir, FLAGFILE_INDEX));
}
bool hasIndexFlagfile(const std::string& dir)
{
	return utils::hasFlagfile(str::joinpath(dir, FLAGFILE_INDEX));
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
	utils::removeFlagfile(str::joinpath(dir, FLAGFILE_DONTPACK));
}
bool hasDontpackFlagfile(const std::string& dir)
{
	return utils::hasFlagfile(str::joinpath(dir, FLAGFILE_DONTPACK));
}


time_t timestamp(const std::string& file)
{
	std::auto_ptr<struct stat> st = wibble::sys::fs::stat(file);
	return st.get() == NULL ? 0 : st->st_mtime;
}

off_t size(const std::string& file)
{
	std::auto_ptr<struct stat> st = wibble::sys::fs::stat(file);
	return st.get() == NULL ? 0 : st->st_size;
}

ino_t inode(const std::string& file)
{
	std::auto_ptr<struct stat> st = wibble::sys::fs::stat(file);
	return st.get() == NULL ? 0 : st->st_ino;
}

void renameIfExists(const std::string& src, const std::string& dst)
{
	int res = ::rename(src.c_str(), dst.c_str());
	if (res < 0 && errno != ENOENT)
		throw wibble::exception::System("moving " + src + " to " + dst);
}

bool exists(const std::string& file)
{
	return sys::fs::access(file, F_OK);
}

}
}
}
// vim:set ts=4 sw=4:
