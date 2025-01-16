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

Session::Session(const std::filesystem::path& root)
    : reader_pool(), root(root)
{
}

Session::~Session()
{
}

std::shared_ptr<Segment> Session::segment_from_relpath(const std::filesystem::path& relpath) const
{
    return segment_from_relpath_and_format(relpath, scan::Scanner::format_from_filename(relpath));
}

std::shared_ptr<Segment> Session::segment_from_relpath_and_format(const std::filesystem::path& relpath, DataFormat format) const
{
    return std::make_shared<Segment>(shared_from_this(), format, relpath);
}

std::shared_ptr<segment::data::Reader> Session::segment_reader(std::shared_ptr<const Segment> segment, std::shared_ptr<const core::ReadLock> lock) const
{
    auto res = reader_pool.find(segment->abspath());
    if (res == reader_pool.end() || res->second.expired())
    {
        auto reader = segment->detect_data_reader(lock);
        reader_pool[segment->abspath()] = reader;
        return reader;
    }
    return res->second.lock();
}

std::shared_ptr<segment::data::Reader> Session::segment_reader(DataFormat format, const std::filesystem::path& relpath, std::shared_ptr<const core::ReadLock> lock) const
{
    return segment_reader(segment_from_relpath_and_format(relpath, format), lock);
}

std::shared_ptr<segment::data::Writer> Session::segment_writer(const segment::data::WriterConfig& config, DataFormat format, const std::filesystem::path& relpath) const
{
    // Ensure that the directory containing the segment exists
    auto seg = segment_from_relpath_and_format(relpath, format);
    std::filesystem::create_directories(seg->abspath().parent_path());

    auto data = seg->detect_data();
    return data->writer(config, false);
}

std::shared_ptr<segment::data::Checker> Session::segment_checker(DataFormat format, const std::filesystem::path& relpath) const
{
    auto seg = segment_from_relpath_and_format(relpath, format);
    auto data = seg->detect_data();
    return data->checker(false);
}

}
