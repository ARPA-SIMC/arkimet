#include "summarycache.h"
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/types/reftime.h>
#include <arki/utils/fd.h>
#include <arki/utils/string.h>
#include <arki/utils/sys.h>
#include <arki/wibble/exception.h>
#include <cerrno>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

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

std::string SummaryCache::summary_pathname(int year, int month) const
{
    char buf[32];
    snprintf(buf, 32, "%04d-%02d.summary", year, month);
    return str::joinpath(m_scache_root, buf);
}

void SummaryCache::openRO()
{
    string parent = str::basename(m_scache_root);
    if (sys::access(parent, W_OK))
        sys::mkdir_ifmissing(m_scache_root, 0777);
}

void SummaryCache::openRW()
{
    sys::mkdir_ifmissing(m_scache_root, 0777);
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
    string sum_file = summary_pathname(year, month);
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
    if (!sys::access(m_scache_root, W_OK))
        return false;

    s.writeAtomically(sum_file);
    return true;
}

bool SummaryCache::write(Summary& s, int year, int month)
{
    string sum_file = summary_pathname(year, month);

    // Write back to the cache directory, if allowed
    if (!sys::access(m_scache_root, W_OK))
        return false;

    s.writeAtomically(sum_file);
    return true;
}

void SummaryCache::invalidate()
{
    // Delete all files *.summary in the cache directory
    sys::Path dir(m_scache_root);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
        if (str::endswith(i->d_name, ".summary"))
            sys::unlink(str::joinpath(m_scache_root, i->d_name));
}

void SummaryCache::invalidate(int year, int month)
{
    sys::unlink_ifexists(summary_pathname(year, month));
    sys::unlink_ifexists(str::joinpath(m_scache_root, "all.summary"));
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
        if (sys::unlink_ifexists(summary_pathname(t.vals[0], t.vals[1])));
            deleted = true;
    }
    if (deleted)
        sys::unlink_ifexists(str::joinpath(m_scache_root, "all.summary"));
}


bool SummaryCache::check(const std::string& dsname, std::ostream& log) const
{
    bool res = true;

    // Visit all .summary files in the cache directory
    sys::Path dir(m_scache_root);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!str::endswith(i->d_name, ".summary")) continue;

        string pathname = str::joinpath(m_scache_root, i->d_name);
        if (!sys::access(pathname, W_OK))
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
