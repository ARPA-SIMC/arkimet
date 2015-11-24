/*
 * dataset/index/summarycache - Cache precomputed summaries
 *
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "summarycache.h"
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/types/reftime.h>
#include <arki/utils/fd.h>
#include <arki/utils/string.h>
#include <wibble/sys/fs.h>
#include <cerrno>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace wibble;
using namespace arki::types;

namespace arki {
namespace dataset {
namespace index {

SummaryCache::SummaryCache(const std::string& root)
    : m_scache_root(root)
{
}

SummaryCache::~SummaryCache()
{
}

void SummaryCache::openRO()
{
    string parent = str::basename(m_scache_root);
    if (sys::fs::access(parent, W_OK))
        sys::fs::mkdirIfMissing(m_scache_root, 0777);
}

void SummaryCache::openRW()
{
    sys::fs::mkdirIfMissing(m_scache_root, 0777);
}

bool SummaryCache::read(Summary& s)
{
    string sum_file = str::joinpath(m_scache_root, "all.summary");
    int fd = open(sum_file.c_str(), O_RDONLY);
    if (fd < 0)
    {
        if (errno == ENOENT)
            return false;
        else
            throw wibble::exception::System("opening file " + sum_file);
    }
    utils::fd::HandleWatch hw(sum_file, fd);
    s.read(fd, sum_file);
    return true;
}

bool SummaryCache::read(Summary& s, int year, int month)
{
    string sum_file = str::joinpath(m_scache_root, str::fmtf("%04d-%02d.summary", year, month));
    int fd = open(sum_file.c_str(), O_RDONLY);
    if (fd < 0)
    {
        if (errno == ENOENT)
            return false;
        else
            throw wibble::exception::System("opening file " + sum_file);
    }
    utils::fd::HandleWatch hw(sum_file, fd);
    s.read(fd, sum_file);
    return true;
}

bool SummaryCache::write(Summary& s)
{
    string sum_file = str::joinpath(m_scache_root, "all.summary");

    // Write back to the cache directory, if allowed
    if (!sys::fs::access(m_scache_root, W_OK))
        return false;

    s.writeAtomically(sum_file);
    return true;
}

bool SummaryCache::write(Summary& s, int year, int month)
{
    string sum_file = str::joinpath(m_scache_root, str::fmtf("%04d-%02d.summary", year, month));

    // Write back to the cache directory, if allowed
    if (!sys::fs::access(m_scache_root, W_OK))
        return false;

    s.writeAtomically(sum_file);
    return true;
}

void SummaryCache::invalidate()
{
    // Delete all files *.summary in the cache directory
    sys::fs::Directory dir(m_scache_root);
    for (sys::fs::Directory::const_iterator i = dir.begin();
            i != dir.end(); ++i)
        if (str::endsWith(*i, ".summary"))
        {
            string pathname = str::joinpath(m_scache_root, *i);
            if (unlink(pathname.c_str()) < 0)
                throw wibble::exception::System("deleting file " + pathname);
        }
}

void SummaryCache::invalidate(int year, int month)
{
    sys::fs::deleteIfExists(str::joinpath(m_scache_root, str::fmtf("%04d-%02d.summary", year, month)));
    sys::fs::deleteIfExists(str::joinpath(m_scache_root, "all.summary"));
}

void SummaryCache::invalidate(const Metadata& md)
{
    if (const reftime::Position* rt = md.get<reftime::Position>())
        invalidate(rt->time.vals[0], rt->time.vals[1]);
}

void SummaryCache::invalidate(const Time& tmin, const Time& tmax)
{
    bool deleted = false;
    for (Time t = tmin; t <= tmax; t = t.start_of_next_month())
    {
        if (sys::fs::deleteIfExists(str::joinpath(m_scache_root, str::fmtf("%04d-%02d.summary", t.vals[0], t.vals[1]))))
            deleted = true;
    }
    if (deleted)
        sys::fs::deleteIfExists(str::joinpath(m_scache_root, "all.summary"));
}


bool SummaryCache::check(const std::string& dsname, std::ostream& log) const
{
    bool res = true;

    // Visit all .summary files in the cache directory
    sys::fs::Directory dir(m_scache_root);
    for (sys::fs::Directory::const_iterator i = dir.begin();
            i != dir.end(); ++i)
    {
        if (!str::endsWith(*i, ".summary")) continue;

        string pathname = str::joinpath(m_scache_root, *i);
        if (!sys::fs::access(pathname, W_OK))
        {
            log << dsname << ": " << pathname << " is not writable." << endl;
            res = false;
        }
    }
    return res;
}

}
}
}
