#include "session.h"
#include "data.h"
#include "data/fd.h"
#include "data/dir.h"
#include "arki/core/file.h"
#include "arki/scan.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <memory>

using namespace arki::core;
using namespace arki::utils;

namespace arki::segment {

Session::Session()
    : reader_pool()
{
}

Session::~Session()
{
}

std::shared_ptr<segment::data::Reader> Session::segment_reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock)
{
    auto abspath = std::filesystem::weakly_canonical(root / relpath);
    auto res = reader_pool.find(abspath);
    if (res == reader_pool.end() || res->second.expired())
    {
        auto seg = segment::Segment::detect_reader(format, root, relpath, abspath, lock);
        reader_pool[abspath] = seg;
        return seg;
    }
    return res->second.lock();
}

std::shared_ptr<segment::data::Writer> Session::segment_writer(const segment::data::WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    // Ensure that the directory containing the segment exists
    auto abspath = root / relpath;
    std::filesystem::create_directories(abspath.parent_path());

    auto res(segment::Segment::detect_writer(config, format, root, relpath, abspath, false));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        res.reset(new segment::data::concat::Writer(config, format, root, relpath, abspath));
    } else if (format == "bufr") {
        res.reset(new segment::data::concat::Writer(config, format, root, relpath, abspath));
    } else if (format == "odimh5" || format == "h5" || format == "odim" || format == "nc" || format == "jpeg") {
        res.reset(new segment::data::dir::Writer(config, format, root, relpath, abspath));
    } else if (format == "vm2") {
        res.reset(new segment::data::lines::Writer(config, format, root, relpath, abspath));
    } else {
        throw std::runtime_error(
                "cannot create writer for " + format + " file " + relpath.native() +
                ": format not supported");
    }
    return res;
}

std::shared_ptr<segment::data::Checker> Session::segment_checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    std::string abspath = root / relpath;

    auto res(segment::Segment::detect_checker(format, root, relpath, abspath, false));
    if (res) return res;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        res.reset(new segment::data::concat::Checker(format, root, relpath, abspath));
    } else if (format == "bufr") {
        res.reset(new segment::data::concat::Checker(format, root, relpath, abspath));
    } else if (format == "odimh5" || format == "h5" || format == "odim" || format == "nc" || format == "jpeg") {
        res.reset(new segment::data::dir::Checker(format, root, relpath, abspath));
    } else if (format == "vm2") {
        res.reset(new segment::data::lines::Checker(format, root, relpath, abspath));
    } else {
        throw std::runtime_error(
                "getting writer for " + format + " file " + relpath.native() +
                ": format not supported");
    }
    return res;
}


std::shared_ptr<segment::data::Reader> DirSegmentsMixin::segment_reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock)
{
    auto abspath = std::filesystem::weakly_canonical(root / relpath);
    auto res = reader_pool.find(abspath);
    if (res == reader_pool.end() || res->second.expired())
    {
        auto seg = segment::Segment::detect_reader(format, root, relpath, root / relpath, lock);
        reader_pool[abspath] = seg;
        return seg;
    }
    return res->second.lock();
}

std::shared_ptr<segment::data::Writer> DirSegmentsMixin::segment_writer(const segment::data::WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    // Ensure that the directory containing the segment exists
    auto abspath = root / relpath;
    std::filesystem::create_directories(abspath.parent_path());

    auto res(segment::Segment::detect_writer(config, format, root, relpath, abspath, false));
    if (res) return res;

    res.reset(new segment::data::dir::Writer(config, format, root, relpath, abspath));
    return res;
}

std::shared_ptr<segment::data::Checker> DirSegmentsMixin::segment_checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    auto abspath = root / relpath;

    auto res(segment::Segment::detect_checker(format, root, relpath, abspath, false));
    if (res) return res;

    res.reset(new segment::data::dir::Checker(format, root, relpath, abspath));
    return res;
}

}
