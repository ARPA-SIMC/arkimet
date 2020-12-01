#include "summarycache.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/dataset.h"
#include "arki/dataset/reporter.h"
#include "arki/types/reftime.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <cerrno>
#include <fcntl.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

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
    sys::File fd(sum_file);
    if (!fd.open_ifexists(O_RDONLY))
        return false;
    s.read(fd);
    return true;
}

bool SummaryCache::read(Summary& s, int year, int month)
{
    string sum_file = summary_pathname(year, month);
    sys::File fd(sum_file);
    if (!fd.open_ifexists(O_RDONLY))
        return false;
    s.read(fd);
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
    {
        auto t = rt->get_Position();
        invalidate(t.ye, t.mo);
    }
}

void SummaryCache::invalidate(const Time& tmin, const Time& tmax)
{
    bool deleted = false;
    for (Time t = tmin; t <= tmax; t = t.start_of_next_month())
    {
        if (sys::unlink_ifexists(summary_pathname(t.ye, t.mo)))
            deleted = true;
    }
    if (deleted)
        sys::unlink_ifexists(str::joinpath(m_scache_root, "all.summary"));
}


bool SummaryCache::check(const dataset::Base& ds, Reporter& reporter) const
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
            reporter.operation_manual_intervention(ds.name(), "check", pathname + " is not writable");
            res = false;
        }
    }
    return res;
}

}
}
}
