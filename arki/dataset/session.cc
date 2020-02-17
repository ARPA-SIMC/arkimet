#include "session.h"
#include "arki/segment.h"
#include "arki/segment/fd.h"
#include "arki/segment/dir.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

using namespace arki::utils;

namespace arki {
namespace dataset {


Session::~Session()
{
}

std::shared_ptr<segment::Reader> Session::reader_from_pool(const std::string& abspath)
{
    auto res = reader_pool.find(abspath);
    if (res == reader_pool.end())
        return std::shared_ptr<segment::Reader>();
    return res->second.lock();
}


std::shared_ptr<segment::Reader> Session::segment_reader(const std::string& format, const std::string& root, const std::string& relpath, std::shared_ptr<core::Lock> lock)
{
    auto res = reader_from_pool(relpath);
    if (!res)
    {
        res = Segment::detect_reader(format, root, relpath, str::joinpath(root, relpath), lock);
        reader_pool[relpath] = res;
    }
    return res;
}

std::shared_ptr<segment::Writer> Session::segment_writer(const std::string& format, const std::string& root, const std::string& relpath)
{
    // Ensure that the directory containing the segment exists
    std::string abspath = str::joinpath(root, relpath);
    sys::makedirs(str::dirname(abspath));

    auto res(Segment::detect_writer(format, root, relpath, abspath, false));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        res.reset(new segment::concat::Writer(format, root, relpath, abspath));
    } else if (format == "bufr") {
        res.reset(new segment::concat::Writer(format, root, relpath, abspath));
    } else if (format == "odimh5" || format == "h5" || format == "odim") {
        res.reset(new segment::dir::Writer(format, root, relpath, abspath));
    } else if (format == "vm2") {
        res.reset(new segment::lines::Writer(format, root, relpath, abspath));
    } else {
        throw std::runtime_error(
                "cannot writer for " + format + " file " + relpath +
                ": format not supported");
    }
    return res;
}

std::shared_ptr<segment::Checker> Session::segment_checker(const std::string& format, const std::string& root, const std::string& relpath)
{
    std::string abspath = str::joinpath(root, relpath);

    auto res(Segment::detect_checker(format, root, relpath, abspath, false));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        res.reset(new segment::concat::Checker(format, root, relpath, abspath));
    } else if (format == "bufr") {
        res.reset(new segment::concat::Checker(format, root, relpath, abspath));
    } else if (format == "odimh5" || format == "h5" || format == "odim") {
        res.reset(new segment::dir::Checker(format, root, relpath, abspath));
    } else if (format == "vm2") {
        res.reset(new segment::lines::Checker(format, root, relpath, abspath));
    } else {
        throw std::runtime_error(
                "getting writer for " + format + " file " + relpath +
                ": format not supported");
    }
    return res;
}

#if 0

struct BaseManager : public SegmentManager
{
    bool mockdata;

    BaseManager(const std::string& root, bool mockdata=false);
};

/// Segment manager that picks the right readers/writers based on file types
struct AutoManager : public BaseManager
{
    AutoManager(const std::string& root, bool mockdata=false);

    std::shared_ptr<segment::Writer> create_writer_for_format(const std::string& format, const std::string& relpath, const std::string& abspath);

    std::shared_ptr<segment::Checker> create_checker_for_format(const std::string& format, const std::string& relpath, const std::string& abspath);
};

/// Segment manager that always picks directory segments
struct ForceDirManager : public BaseManager
{
    ForceDirManager(const std::string& root);

    std::shared_ptr<segment::Writer> create_writer_for_format(const std::string& format, const std::string& relpath, const std::string& abspath) override;

    std::shared_ptr<segment::Checker> create_checker_for_format(const std::string& format, const std::string& relpath, const std::string& abspath) override;
};

/// Segment manager that always uses hole file segments
struct HoleDirManager : public ForceDirManager
{
    HoleDirManager(const std::string& root);

    std::shared_ptr<segment::Writer> create_writer_for_format(const std::string& format, const std::string& relpath, const std::string& abspath) override;
};


AutoManager::AutoManager(const std::string& root, bool mockdata)
    : BaseManager(root, mockdata) {}

std::shared_ptr<segment::Writer> AutoManager::create_writer_for_format(const std::string& format, const std::string& relpath, const std::string& abspath)
{
}

std::shared_ptr<segment::Checker> AutoManager::create_checker_for_format(const std::string& format, const std::string& relpath, const std::string& abspath)
{
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
#endif

}
}
