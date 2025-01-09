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
    auto segment = std::make_shared<Segment>(format, root, relpath);
    auto res = reader_pool.find(segment->abspath);
    if (res == reader_pool.end() || res->second.expired())
    {
        auto seg = segment->detect_data_reader(lock);
        reader_pool[segment->abspath] = seg;
        return seg;
    }
    return res->second.lock();
}

std::shared_ptr<segment::data::Writer> Session::segment_writer(const segment::data::WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    // Ensure that the directory containing the segment exists
    auto segment = std::make_shared<Segment>(format, root, relpath);
    std::filesystem::create_directories(segment->abspath.parent_path());

    auto data = segment->detect_data();
    return data->writer(config, false);
}

std::shared_ptr<segment::data::Checker> Session::segment_checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    auto segment = std::make_shared<Segment>(format, root, relpath);
    auto data = segment->detect_data();
    return data->checker(false);
}


std::shared_ptr<segment::data::Reader> DirSegmentsMixin::segment_reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock)
{
    auto segment = std::make_shared<Segment>(format, root, relpath);
    auto res = reader_pool.find(segment->abspath);
    if (res == reader_pool.end() || res->second.expired())
    {
        auto data = segment->detect_data(Segment::DefaultFileSegment::SEGMENT_DIR);
        auto reader = data->reader(lock);
        reader_pool[segment->abspath] = reader;
        return reader;
    }
    return res->second.lock();
}

std::shared_ptr<segment::data::Writer> DirSegmentsMixin::segment_writer(const segment::data::WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    auto segment = std::make_shared<Segment>(format, root, relpath);
    // Ensure that the directory containing the segment exists
    std::filesystem::create_directories(segment->abspath.parent_path());

    auto data = segment->detect_data(Segment::DefaultFileSegment::SEGMENT_DIR);
    return data->writer(config, false);
}

std::shared_ptr<segment::data::Checker> DirSegmentsMixin::segment_checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath)
{
    auto segment = std::make_shared<Segment>(format, root, relpath);

    auto data = segment->detect_data(Segment::DefaultFileSegment::SEGMENT_DIR);
    return data->checker(false);
}

}
