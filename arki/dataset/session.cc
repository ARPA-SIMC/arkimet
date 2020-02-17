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

}
}
