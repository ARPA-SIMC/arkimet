#include "session.h"
#include "arki/core/cfg.h"
#include "arki/core/file.h"
#include "arki/core/lock.h"
#include "arki/nag.h"
#include "arki/scan.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "data.h"
#include "data/dir.h"
#include "data/fd.h"
#include "data/gz.h"
#include "data/tar.h"
#include "data/zip.h"
#include "metadata.h"
#include "scan.h"
#include <memory>

using namespace arki::core;
using namespace arki::utils;

namespace arki::segment {

Session::Session(const std::filesystem::path& root)
    : reader_pool(), root(sys::abspath(root))
{
}

Session::Session(const core::cfg::Section& cfg)
    : reader_pool(), root(cfg.value("path")),
      smallfiles(cfg.value_bool("smallfiles")),
      eatmydata(cfg.value_bool("eatmydata"))
{
}

Session::~Session() {}

bool Session::is_segment(const std::filesystem::path& relpath) const
{
    auto abspath                    = root / relpath;
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

std::shared_ptr<Segment>
Session::segment_from_relpath(const std::filesystem::path& relpath) const
{
    return segment_from_relpath_and_format(
        relpath, arki::scan::Scanner::format_from_filename(relpath));
}

std::shared_ptr<Segment>
Session::segment_from_relpath_and_format(const std::filesystem::path& relpath,
                                         DataFormat format) const
{
    return std::make_shared<Segment>(shared_from_this(), format, relpath);
}

std::shared_ptr<segment::Reader>
Session::segment_reader(std::shared_ptr<const Segment> segment,
                        std::shared_ptr<const core::ReadLock> lock) const
{
    std::shared_ptr<segment::Reader> reader;
    auto from_pool = reader_pool.find(segment->abspath());
    if (from_pool != reader_pool.end() && !from_pool->second.expired())
    {
        reader = from_pool->second.lock();
        if (reader->has_changed())
            reader.reset();
    }

    if (!reader)
    {
        reader                          = create_segment_reader(segment, lock);
        reader_pool[segment->abspath()] = reader;
    }

    return reader;
}

void Session::create_scan(std::shared_ptr<Segment> segment,
                          arki::metadata::Collection& mds) const
{
    auto data = Data::create(segment);
    data->create_segment(mds);
}

void Session::create_metadata(std::shared_ptr<Segment> segment,
                              arki::metadata::Collection& mds) const
{
    auto data = Data::create(segment);
    data->create_segment(mds);
    // TODO: implement data->read_lock() and data->check_lock()
    auto lock    = std::make_shared<core::lock::NullCheckLock>();
    auto checker = std::make_shared<segment::metadata::Checker>(segment, lock);
    auto fixer   = checker->fixer();
    fixer->reindex(mds);
}

void Session::create_iseg(std::shared_ptr<Segment>,
                          arki::metadata::Collection&) const
{
    throw std::runtime_error("normal sessions cannot create iseg segments");
}

} // namespace arki::segment
