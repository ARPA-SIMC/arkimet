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

std::shared_ptr<segment::Writer> BaseManager::create_writer_for_existing_segment(const std::string& format, const std::string& relname, const std::string& absname, bool nullptr_on_error)
{
    std::unique_ptr<struct stat> st = sys::stat(absname);
    std::shared_ptr<segment::Writer> res;
    if (!st.get())
        st = sys::stat(absname + ".gz");
    if (!st.get())
    {
        st = sys::stat(absname + ".tar");
        if (st.get())
            throw_consistency_error(
                    "getting writer for " + format + " file " + relname,
                    "cannot write to .tar segments");
    }
    if (!st.get())
        return res;

    if (S_ISDIR(st->st_mode))
    {
        if (segment::dir::can_store(format))
        {
            if (mockdata)
                res.reset(new segment::dir::HoleWriter(format, root, relname, absname));
            else
                res.reset(new segment::dir::Writer(format, root, relname, absname));
        } else {
            if (nullptr_on_error)
                return res;
            throw_consistency_error(
                    "getting writer for " + format + " file " + relname,
                    "format not supported");
        }
    } else {
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
        } else if (format == "vm2") {
            if (mockdata)
                throw_consistency_error("mockdata single-file line-based segments not implemented");
            else
                res.reset(new segment::lines::Writer(root, relname, absname));
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            throw_consistency_error("segment is a file, but odimh5 data can only be stored into directory segments");
        } else {
            if (nullptr_on_error)
                return res;
            throw_consistency_error(
                    "getting segment for " + format + " file " + relname,
                    "format not supported");
        }
    }
    return res;
}

std::shared_ptr<segment::Checker> BaseManager::create_checker_for_existing_segment(const std::string& format, const std::string& relname, const std::string& absname, bool nullptr_on_error)
{
    std::unique_ptr<struct stat> st = sys::stat(absname);
    std::shared_ptr<segment::Checker> res;
    if (!st.get())
        st = sys::stat(absname + ".gz");
    if (!st.get())
    {
        st = sys::stat(absname + ".tar");
        if (st.get())
        {
            res.reset(new segment::tar::Checker(root, relname, absname));
            return res;
        }
    }
    if (!st.get())
        return res;

    if (S_ISDIR(st->st_mode))
    {
        if (segment::dir::can_store(format))
        {
            if (mockdata)
                res.reset(new segment::dir::HoleChecker(format, root, relname, absname));
            else
                res.reset(new segment::dir::Checker(format, root, relname, absname));
        } else {
            if (nullptr_on_error)
                return res;
            throw_consistency_error(
                    "getting segment for " + format + " file " + relname,
                    "format not supported");
        }
    } else {
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
        } else if (format == "vm2") {
            if (mockdata)
                throw_consistency_error("mockdata single-file line-based segments not implemented");
            else
                res.reset(new segment::lines::Checker(root, relname, absname));
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            // If it's a file and we need a directory, still get a checker
            // so it can deal with it
            if (mockdata)
                res.reset(new segment::dir::HoleChecker(format, root, relname, absname));
            else
                res.reset(new segment::dir::Checker(format, root, relname, absname));
        } else {
            if (nullptr_on_error)
                return res;
            throw_consistency_error(
                    "getting segment for " + format + " file " + relname,
                    "format not supported");
        }
    }
    return res;
}


AutoManager::AutoManager(const std::string& root, bool mockdata)
    : BaseManager(root, mockdata) {}

std::shared_ptr<segment::Writer> AutoManager::create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname)
{
    auto res(create_writer_for_existing_segment(format, relname, absname));
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
    auto res(create_checker_for_existing_segment(format, relname, absname));
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
    auto res(create_writer_for_existing_segment(format, relname, absname));
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
