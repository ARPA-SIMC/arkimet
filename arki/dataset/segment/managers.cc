#include "managers.h"
#include "arki/segment/fd.h"
#include "arki/segment/dir.h"
#include "arki/exceptions.h"
#include "arki/scan.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

BaseManager::BaseManager(const std::string& root, bool mockdata) : SegmentManager(root), mockdata(mockdata) {}

void BaseManager::scan_dir(std::function<void(const std::string& relpath)> dest)
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
        string abspath = str::joinpath(m_root, relpath, name);
        if (Segment::is_segment(abspath))
        {
            string basename = Segment::basename(name);
            dest(str::joinpath(relpath, basename));
            return false;
        }

        return true;
    };

    walker.walk();
}


AutoManager::AutoManager(const std::string& root, bool mockdata)
    : BaseManager(root, mockdata) {}

std::shared_ptr<segment::Writer> AutoManager::create_writer_for_format(const std::string& format, const std::string& relpath, const std::string& abspath)
{
    auto res(Segment::detect_writer(format, root, relpath, abspath, mockdata));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        if (mockdata)
            res.reset(new segment::concat::HoleWriter(format, root, relpath, abspath));
        else
            res.reset(new segment::concat::Writer(format, root, relpath, abspath));
    } else if (format == "bufr") {
        if (mockdata)
            res.reset(new segment::concat::HoleWriter(format, root, relpath, abspath));
        else
            res.reset(new segment::concat::Writer(format, root, relpath, abspath));
    } else if (format == "odimh5" || format == "h5" || format == "odim") {
        if (mockdata)
            res.reset(new segment::dir::HoleWriter(format, root, relpath, abspath));
        else
            res.reset(new segment::dir::Writer(format, root, relpath, abspath));
    } else if (format == "vm2") {
        if (mockdata)
            throw_consistency_error("mockdata single-file line-based segments not implemented");
        else
            res.reset(new segment::lines::Writer(format, root, relpath, abspath));
    } else {
        throw_consistency_error(
                "getting writer for " + format + " file " + relpath,
                "format not supported");
    }
    return res;
}

std::shared_ptr<segment::Checker> AutoManager::create_checker_for_format(const std::string& format, const std::string& relpath, const std::string& abspath)
{
    auto res(Segment::detect_checker(format, root, relpath, abspath, mockdata));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        if (mockdata)
            res.reset(new segment::concat::HoleChecker(format, root, relpath, abspath));
        else
            res.reset(new segment::concat::Checker(format, root, relpath, abspath));
    } else if (format == "bufr") {
        if (mockdata)
            res.reset(new segment::concat::HoleChecker(format, root, relpath, abspath));
        else
            res.reset(new segment::concat::Checker(format, root, relpath, abspath));
    } else if (format == "odimh5" || format == "h5" || format == "odim") {
        if (mockdata)
            res.reset(new segment::dir::HoleChecker(format, root, relpath, abspath));
        else
            res.reset(new segment::dir::Checker(format, root, relpath, abspath));
    } else if (format == "vm2") {
        if (mockdata)
            throw_consistency_error("mockdata single-file line-based segments not implemented");
        else
            res.reset(new segment::lines::Checker(format, root, relpath, abspath));
    } else {
        throw_consistency_error(
                "getting writer for " + format + " file " + relpath,
                "format not supported");
    }
    return res;
}

ForceDirManager::ForceDirManager(const std::string& root) : BaseManager(root) {}

std::shared_ptr<segment::Writer> ForceDirManager::create_writer_for_format(const std::string& format, const std::string& relpath, const std::string& abspath)
{
    auto res(Segment::detect_writer(format, root, relpath, abspath, mockdata));
    if (res) return res;
    return std::shared_ptr<segment::Writer>(new segment::dir::Writer(format, root, relpath, abspath));
}

std::shared_ptr<segment::Checker> ForceDirManager::create_checker_for_format(const std::string& format, const std::string& relpath, const std::string& abspath)
{
    auto res(Segment::detect_checker(format, root, relpath, abspath, mockdata));
    if (res) return res;
    return std::shared_ptr<segment::Checker>(new segment::dir::Checker(format, root, relpath, abspath));
}


HoleDirManager::HoleDirManager(const std::string& root) : ForceDirManager(root) {}

std::shared_ptr<segment::Writer> HoleDirManager::create_writer_for_format(const std::string& format, const std::string& relpath, const std::string& abspath)
{
    return std::shared_ptr<segment::Writer>(new segment::dir::HoleWriter(format, root, relpath, abspath));
}

}
}
