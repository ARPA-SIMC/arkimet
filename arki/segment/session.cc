#include "session.h"
#include "data.h"
#include "data/fd.h"
#include "data/dir.h"
#include "data/tar.h"
#include "data/zip.h"
#include "data/gz.h"
#include "metadata.h"
#include "scan.h"
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

bool Session::is_data_segment(const std::filesystem::path& relpath) const
{
    auto abspath = root / relpath;
    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (!st.get())
        return false;

    auto extension = abspath.extension();

    if (extension == ".metadata")
        return false;

    if (extension == ".summary")
        return false;

    if (extension == ".gz.idx")
        return false;

    if (extension == ".zip")
    {
        if (S_ISDIR(st->st_mode))
            return false;
        auto format = arki::scan::Scanner::format_from_filename(abspath.stem());
        return segment::data::zip::Data::can_store(format);
    }

    if (extension == ".gz")
    {
        if (S_ISDIR(st->st_mode))
            return false;
        return segment::data::gz::Data::can_store(
            arki::scan::Scanner::format_from_filename(abspath.stem()));
    }

    if (extension == ".tar")
    {
        if (S_ISDIR(st->st_mode))
            return false;
        return segment::data::tar::Data::can_store(
                arki::scan::Scanner::format_from_filename(abspath.stem()));
    }

    auto format = arki::scan::Scanner::detect_format(abspath);
    if (not format)
        return false;

    if (!S_ISDIR(st->st_mode))
        return segment::data::fd::Data::can_store(format.value());

    if (!std::filesystem::exists(abspath / ".sequence"))
        return false;

    return segment::data::dir::Data::can_store(format.value());
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
    return std::make_shared<segment::scan::Reader>(segment, lock);
}

std::shared_ptr<segment::Checker> Session::segment_checker(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock) const
{
    throw std::runtime_error("this session misses a policy to determine how to create checkers");
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

std::shared_ptr<segment::data::Writer> Session::segment_data_writer(std::shared_ptr<const Segment> segment, const segment::data::WriterConfig& config) const
{
    std::filesystem::create_directories(segment->abspath().parent_path());
    auto data = segment->detect_data();
    return data->writer(config, false);
}

std::shared_ptr<segment::data::Checker> Session::segment_data_checker(std::shared_ptr<const Segment> segment) const
{
    auto data = segment->detect_data();
    return data->checker(false);
}

}
