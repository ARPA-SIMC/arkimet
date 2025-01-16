#include "session.h"
#include "data.h"
#include "data/fd.h"
#include "data/dir.h"
#include "metadata.h"
#include "arki/core/file.h"
#include "arki/scan.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
#include "arki/nag.h"
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
    return segment_from_relpath_and_format(relpath, arki::scan::Scanner::format_from_filename(relpath));
}

std::shared_ptr<Segment> Session::segment_from_relpath_and_format(const std::filesystem::path& relpath, DataFormat format) const
{
    return std::make_shared<Segment>(shared_from_this(), format, relpath);
}


std::shared_ptr<segment::Reader> Session::segment_reader(std::shared_ptr<const Segment> segment, std::shared_ptr<const core::ReadLock> lock) const
{
    auto data = segment->detect_data();

    // stat the metadata file, if it exists
    auto md_abspath = sys::with_suffix(segment->abspath(), ".metadata");
    auto st_md = sys::stat(md_abspath);
    // If it exists and it looks new enough, use it
    if (st_md.get())
    {
        auto ts = data->timestamp();
        if (!ts)
        {
            nag::warning("%s: segment data is not available", segment->abspath().c_str());
            return std::make_shared<segment::EmptyReader>(segment, lock);
        }

        if (st_md->st_mtime >= ts.value())
            return std::make_shared<segment::metadata::Reader>(segment, lock);
        else
            nag::warning("%s: outdated .metadata file: falling back to data scan", segment->abspath().c_str());
    }

    // Else scan the file as usual
    //return std::make_shared<segment::scan::Reader>(reader);
    throw std::runtime_error("scan::Reader not yet implemented");
}


std::shared_ptr<segment::data::Reader> Session::segment_data_reader(std::shared_ptr<const Segment> segment, std::shared_ptr<const core::ReadLock> lock) const
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

std::shared_ptr<segment::data::Reader> Session::segment_data_reader(DataFormat format, const std::filesystem::path& relpath, std::shared_ptr<const core::ReadLock> lock) const
{
    return segment_data_reader(segment_from_relpath_and_format(relpath, format), lock);
}

std::shared_ptr<segment::data::Writer> Session::segment_data_writer(const segment::data::WriterConfig& config, DataFormat format, const std::filesystem::path& relpath) const
{
    // Ensure that the directory containing the segment exists
    auto seg = segment_from_relpath_and_format(relpath, format);
    std::filesystem::create_directories(seg->abspath().parent_path());

    auto data = seg->detect_data();
    return data->writer(config, false);
}

std::shared_ptr<segment::data::Checker> Session::segment_data_checker(DataFormat format, const std::filesystem::path& relpath) const
{
    auto seg = segment_from_relpath_and_format(relpath, format);
    auto data = seg->detect_data();
    return data->checker(false);
}

}
