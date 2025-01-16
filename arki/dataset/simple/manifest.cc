#include "manifest.h"
#include "arki/exceptions.h"
#include "arki/core/lock.h"
#include "arki/query.h"
#include "arki/dataset/simple.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/sort.h"
#include "arki/types/source/blob.h"
#include "arki/summary.h"
#include "arki/matcher.h"
#include "arki/utils/sqlite.h"
#include "arki/utils/files.h"
#include "arki/nag.h"
#include "arki/iotrace.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <algorithm>
#include <unistd.h>
#include <fcntl.h>
#include <ctime>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::utils::sqlite;

namespace arki::dataset::simple::manifest {

bool exists(const std::filesystem::path& root)
{
    return sys::access(root / "MANIFEST", F_OK) or sys::access(root / "index.sqlite", F_OK);
}

std::vector<SegmentInfo> read_plain(const std::filesystem::path& path)
{
    core::File infd(path, O_RDONLY);
    iotrace::trace_file(path, 0, 0, "read MANIFEST");

    std::vector<SegmentInfo> res;

    auto reader = core::LineReader::from_fd(infd);
    std::string line;
    for (size_t lineno = 1; reader->getline(line); ++lineno)
    {
        // Skip empty lines
        if (line.empty()) continue;

        size_t beg = 0;
        size_t end = line.find(';');
        if (end == std::string::npos)
        {
            std::stringstream ss;
            ss << "cannot parse " << path << ":" << lineno << ": line has only 1 field";
            throw std::runtime_error(ss.str());
        }

        std::string file = line.substr(beg, end-beg);

        beg = end + 1;
        end = line.find(';', beg);
        if (end == std::string::npos)
        {
            std::stringstream ss;
            ss << "cannot parse " << path << ":" << lineno << ": line has only 2 fields";
            throw std::runtime_error(ss.str());
        }

        time_t mtime = strtoul(line.substr(beg, end-beg).c_str(), 0, 10);

        beg = end + 1;
        end = line.find(';', beg);
        if (end == std::string::npos)
        {
            std::stringstream ss;
            ss << "cannot parse " << path << ":" << lineno << ": line has only 3 fields";
            throw std::runtime_error(ss.str());
        }

        // Times are saved with extremes included
        core::Time interval_end = core::Time::create_sql(line.substr(end+1));
        interval_end.se += 1;
        interval_end.normalise();
        res.emplace_back(SegmentInfo{file, mtime, core::Interval(core::Time::create_sql(line.substr(beg, end-beg)), interval_end)});
    }

    return res;
}

std::vector<SegmentInfo> read_sqlite(const std::filesystem::path& path)
{
    nag::warning("%s: found legacy SQLite manifest", path.c_str());
    utils::sqlite::SQLiteDB m_db;
    m_db.open(path);
    m_db.exec("PRAGMA legacy_file_format = 0");

    std::vector<SegmentInfo> res;

    auto query = "SELECT file, mtime, start_time, end_time FROM files ORDER BY start_time";
    Query q("sel_archive", m_db);
    q.compile(query);
    while (q.step())
    {
        Interval interval;
        std::filesystem::path relpath = q.fetchString(0);
        auto mtime = q.fetch<time_t>(1);
        interval.begin.set_sql(q.fetchString(2));
        interval.end.set_sql(q.fetchString(3));
        res.emplace_back(SegmentInfo{relpath, mtime, interval});
    }

    return res;
}

Reader::Reader(const std::filesystem::path& root)
    : m_root(root)
{
}

bool Reader::reread()
{
    auto pathname = m_root / "MANIFEST";
    ino_t inode = sys::inode(pathname, 0);

    if (inode == 0)
    {
        // Try to fallback on legacy sqlite
        auto sqlite_pathname = m_root / "index.sqlite";
        if (std::filesystem::exists(sqlite_pathname))
        {
            segmentinfo_cache = read_sqlite(m_root / "index.sqlite");
            last_inode = 0;
            legacy_sql = true;
            // index.sqlite existed but MANIFEST did not exist
            // If this is run by Writer, it needs to flag the status as dirty
            // to schedule writing the new MANIFEST file
            return false;
        }
    }

    if (inode == last_inode) return inode != 0;

    last_inode = inode;
    if (inode == 0)
    {
        segmentinfo_cache.clear();
        return false;
    }

    segmentinfo_cache = read_plain(pathname);
    return true;
}

const SegmentInfo* Reader::segment(const std::filesystem::path& relpath) const
{
    SegmentInfo sample{relpath, 0, core::Interval()};
    auto lb = lower_bound(segmentinfo_cache.begin(), segmentinfo_cache.end(), sample, [](const auto& a, const auto& b) { return a.relpath < b.relpath; });
    if (lb != segmentinfo_cache.end() && lb->relpath == relpath)
        return &*lb;
    else
        return nullptr;
}

std::vector<SegmentInfo> Reader::file_list(const Matcher& matcher) const
{
    core::Interval interval;
    if (!matcher.intersect_interval(interval))
        return std::vector<SegmentInfo>();

    if (interval.is_unbounded())
    {
        // No restrictions on reftime: get all files
        return segmentinfo_cache;
    } else {
        // Get files with matching reftime
        std::vector<SegmentInfo> res;
        for (const auto& i: segmentinfo_cache)
            if (interval.intersects(i.time))
                res.push_back(i);
        return res;
    }
}

const std::vector<SegmentInfo>& Reader::file_list() const
{
    return segmentinfo_cache;
}

core::Interval Reader::get_stored_time_interval() const
{
    core::Interval res;
    for (const auto& i: segmentinfo_cache)
        if (res.is_unbounded())
            res = i.time;
        else
            res.extend(i.time);
    return res;
}


Writer::Writer(const std::filesystem::path& root, bool eatmydata)
    : Reader(root), eatmydata(eatmydata)
{
}

void Writer::reread()
{
    if (not Reader::reread())
        dirty = true;
}

void Writer::set(const std::filesystem::path& relpath, time_t mtime, const core::Interval& time)
{
    // Add to index
    SegmentInfo info{relpath, mtime, time};

    // Insertion sort; at the end, everything is already sorted and we
    // avoid inserting lots of duplicate items
    auto lb = std::lower_bound(segmentinfo_cache.begin(), segmentinfo_cache.end(), info, [](const auto& a, const auto& b) { return a.relpath < b.relpath; });
    if (lb == segmentinfo_cache.end())
        segmentinfo_cache.push_back(info);
    else if (lb->relpath != info.relpath)
        segmentinfo_cache.insert(lb, info);
    else
        *lb = info;

    dirty = true;
}

void Writer::remove(const std::filesystem::path& relpath)
{
    auto i = std::find_if(segmentinfo_cache.begin(), segmentinfo_cache.end(), [&](const auto& el) { return el.relpath == relpath; });
    if (i != segmentinfo_cache.end())
    {
        segmentinfo_cache.erase(i);
        dirty = true;
    }
}

void Writer::write(const SegmentInfo& info, core::NamedFileDescriptor& out) const
{
    // Time is stored with the right extreme included
    core::Time end = info.time.end;
    end.se -= 1;
    end.normalise();
    std::stringstream ss;
    ss << info.relpath.native() << ";" << info.mtime << ";" << info.time.begin.to_sql() << ";" << end.to_sql() << std::endl;
    out.write_all_or_throw(ss.str());
}

void Writer::flush()
{
    if (!dirty)
        return;

    auto pathname = m_root / "MANIFEST.tmp";

    core::File out(pathname, O_WRONLY | O_CREAT | O_TRUNC);
    for (const auto& i: segmentinfo_cache)
        write(i, out);
    if (!eatmydata)
        out.fdatasync();
    out.close();

    auto target = m_root / "MANIFEST";
    if (::rename(pathname.c_str(), target.c_str()) < 0)
        throw_system_error("cannot rename " + pathname.native() + " to " + target.native());

    if (legacy_sql)
        std::filesystem::remove(m_root / "index.sqlite");

    dirty = false;
}

void Writer::rename(const std::filesystem::path& relpath, const std::filesystem::path& new_relpath)
{
    // This can be made more efficient, but it's really only used for testing
    for (auto& i: segmentinfo_cache)
        if (i.relpath == relpath)
        {
            i.relpath = new_relpath;
            dirty = true;
        }
    std::sort(segmentinfo_cache.begin(), segmentinfo_cache.end(), [](const auto& a, const auto& b) { return a.relpath < b.relpath; });
}

}
