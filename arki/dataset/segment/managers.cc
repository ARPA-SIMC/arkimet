#include "managers.h"
#include "arki/segment/concat.h"
#include "arki/segment/lines.h"
#include "arki/segment/dir.h"
#include "arki/segment/tar.h"
#include "arki/exceptions.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

BaseManager::BaseManager(const std::string& root, bool mockdata) : SegmentManager(root), mockdata(mockdata) {}


AutoManager::AutoManager(const std::string& root, bool mockdata)
    : BaseManager(root, mockdata) {}

std::shared_ptr<segment::Writer> AutoManager::create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname)
{
    auto res(segment::Writer::for_pathname(format, root, relname, absname, mockdata));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        if (mockdata)
            res.reset(new segment::concat::HoleWriter(root, relname, absname));
        else
            res.reset(new segment::concat::Writer(root, relname, absname));
    } else if (format == "bufr") {
        if (mockdata)
            res.reset(new segment::concat::HoleWriter(root, relname, absname));
        else
            res.reset(new segment::concat::Writer(root, relname, absname));
    } else if (format == "odimh5" || format == "h5" || format == "odim") {
        if (mockdata)
            res.reset(new segment::dir::HoleWriter(format, root, relname, absname));
        else
            res.reset(new segment::dir::Writer(format, root, relname, absname));
    } else if (format == "vm2") {
        if (mockdata)
            throw_consistency_error("mockdata single-file line-based segments not implemented");
        else
            res.reset(new segment::lines::Writer(root, relname, absname));
    } else {
        throw_consistency_error(
                "getting writer for " + format + " file " + relname,
                "format not supported");
    }
    return res;
}

std::shared_ptr<segment::Checker> AutoManager::create_checker_for_format(const std::string& format, const std::string& relname, const std::string& absname)
{
    auto res(segment::Checker::for_pathname(format, root, relname, absname, mockdata));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        if (mockdata)
            res.reset(new segment::concat::HoleChecker(root, relname, absname));
        else
            res.reset(new segment::concat::Checker(root, relname, absname));
    } else if (format == "bufr") {
        if (mockdata)
            res.reset(new segment::concat::HoleChecker(root, relname, absname));
        else
            res.reset(new segment::concat::Checker(root, relname, absname));
    } else if (format == "odimh5" || format == "h5" || format == "odim") {
        if (mockdata)
            res.reset(new segment::dir::HoleChecker(format, root, relname, absname));
        else
            res.reset(new segment::dir::Checker(format, root, relname, absname));
    } else if (format == "vm2") {
        if (mockdata)
            throw_consistency_error("mockdata single-file line-based segments not implemented");
        else
            res.reset(new segment::lines::Checker(root, relname, absname));
    } else {
        throw_consistency_error(
                "getting writer for " + format + " file " + relname,
                "format not supported");
    }
    return res;
}

void AutoManager::scan_dir(std::function<void(const std::string& relname)> dest)
{
    // Trim trailing '/'
    string m_root = root;
    while (m_root.size() > 1 and m_root[m_root.size()-1] == '/')
        m_root.resize(m_root.size() - 1);

    files::PathWalk walker(m_root);
    walker.consumer = [&](const std::string& relpath, sys::Path::iterator& entry, struct stat& st) {
        // Skip '.', '..' and hidden files
        if (entry->d_name[0] == '.')
            return false;

        string name = entry->d_name;

        // Skip compressed data index files
        if (str::endswith(name, ".gz.idx"))
            return false;

        bool is_dir = S_ISDIR(st.st_mode);
        if (is_dir)
        {
            sys::Path sub(entry.open_path());
            struct stat seq_st;
            if (sub.fstatat_ifexists(".sequence", seq_st))
            {
                // Directory segment
                string format = utils::get_format(name);
                if (segment::dir::can_store(format))
                    dest(str::joinpath(relpath, name));
                return false;
            }
            else
                // Normal subdirectory, recurse into it
                return true;
        } else {
            // Concat segment
            if (str::endswith(name, ".gz"))
                name = name.substr(0, name.size() - 3);

            // Check whether the file format (from the extension) could be
            // stored in this kind of segment
            string format = utils::get_format(name);
            if (segment::fd::can_store(format))
                dest(str::joinpath(relpath, name));
            return false;
        }
    };

    walker.walk();
}

ForceDirManager::ForceDirManager(const std::string& root) : BaseManager(root) {}

std::shared_ptr<segment::Writer> ForceDirManager::create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname)
{
    auto res(segment::Writer::for_pathname(format, root, relname, absname, mockdata));
    if (res) return res;
    return std::shared_ptr<segment::Writer>(new segment::dir::Writer(format, root, relname, absname));
}

std::shared_ptr<segment::Checker> ForceDirManager::create_checker_for_format(const std::string& format, const std::string& relname, const std::string& absname)
{
    return std::shared_ptr<segment::Checker>(new segment::dir::Checker(format, root, relname, absname));
}

void ForceDirManager::scan_dir(std::function<void(const std::string& relname)> dest)
{
    // Trim trailing '/'
    string m_root = root;
    while (m_root.size() > 1 and m_root[m_root.size()-1] == '/')
        m_root.resize(m_root.size() - 1);

    files::PathWalk walker(m_root);
    walker.consumer = [&](const std::string& relpath, sys::Path::iterator& entry, struct stat& st) {
        // Skip '.', '..' and hidden files
        if (entry->d_name[0] == '.')
            return false;

        string name = entry->d_name;

        // Skip compressed data index files
        if (str::endswith(name, ".gz.idx"))
            return false;

        if (!S_ISDIR(st.st_mode))
            return false;

        sys::Path sub(entry.open_path());
        struct stat seq_st;
        if (!sub.fstatat_ifexists(".sequence", seq_st))
            // Normal subdirectory, recurse into it
            return true;

        // Check whether the file format (from the extension) could be
        // stored in this kind of segment
        string format = utils::get_format(name);
        if (segment::dir::can_store(format))
            dest(str::joinpath(relpath, name));
        return false;
    };

    walker.walk();
}


HoleDirManager::HoleDirManager(const std::string& root) : ForceDirManager(root) {}

std::shared_ptr<segment::Writer> HoleDirManager::create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname)
{
    return std::shared_ptr<segment::Writer>(new segment::dir::HoleWriter(format, root, relname, absname));
}

}
}
