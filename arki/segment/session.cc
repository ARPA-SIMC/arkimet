#include "session.h"
#include "data.h"
#include "data/fd.h"
#include "data/dir.h"
#include "arki/core/file.h"
#include "arki/scan.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
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

std::shared_ptr<Segment> Session::segment(DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    return std::make_shared<Segment>(shared_from_this(), format, root, relpath);
}

std::shared_ptr<Segment> Session::segment_from_path(const std::filesystem::path& path)
{
    return segment_from_path_and_format(path, scan::Scanner::format_from_filename(path));
}

std::shared_ptr<Segment> Session::segment_from_path_and_format(const std::filesystem::path& path, DataFormat format)
{
    std::filesystem::path basedir;
    std::filesystem::path relpath;
    utils::files::resolve_path(path, basedir, relpath);
    return segment(format, basedir, relpath);
}


std::shared_ptr<segment::data::Reader> Session::segment_reader(DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock)
{
    auto seg = segment(format, root, relpath);
    auto res = reader_pool.find(seg->abspath);
    if (res == reader_pool.end() || res->second.expired())
    {
        auto reader = seg->detect_data_reader(lock);
        reader_pool[seg->abspath] = reader;
        return reader;
    }
    return res->second.lock();
}

std::shared_ptr<segment::data::Writer> Session::segment_writer(const segment::data::WriterConfig& config, DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    // Ensure that the directory containing the segment exists
    auto seg = segment(format, root, relpath);
    std::filesystem::create_directories(seg->abspath.parent_path());

    auto data = seg->detect_data();
    return data->writer(config, false);
}

std::shared_ptr<segment::data::Checker> Session::segment_checker(DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    auto seg = segment(format, root, relpath);
    auto data = seg->detect_data();
    return data->checker(false);
}


std::shared_ptr<segment::data::Reader> DirSegmentsMixin::segment_reader(DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock)
{
    auto seg = segment(format, root, relpath);
    auto res = reader_pool.find(seg->abspath);
    if (res == reader_pool.end() || res->second.expired())
    {
        auto data = seg->detect_data(Segment::DefaultFileSegment::SEGMENT_DIR);
        auto reader = data->reader(lock);
        reader_pool[seg->abspath] = reader;
        return reader;
    }
    return res->second.lock();
}

std::shared_ptr<segment::data::Writer> DirSegmentsMixin::segment_writer(const segment::data::WriterConfig& config, DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    auto seg = segment(format, root, relpath);
    // Ensure that the directory containing the segment exists
    std::filesystem::create_directories(seg->abspath.parent_path());

    auto data = seg->detect_data(Segment::DefaultFileSegment::SEGMENT_DIR);
    return data->writer(config, false);
}

std::shared_ptr<segment::data::Checker> DirSegmentsMixin::segment_checker(DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    auto seg = segment(format, root, relpath);
    auto data = seg->detect_data(Segment::DefaultFileSegment::SEGMENT_DIR);
    return data->checker(false);
}

}
