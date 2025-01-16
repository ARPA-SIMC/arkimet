#include "segment.h"
#include "segment/metadata.h"
#include "segment/data/missing.h"
#include "segment/data/fd.h"
#include "segment/data/dir.h"
#include "segment/data/tar.h"
#include "segment/data/zip.h"
#include "segment/data/gz.h"
#include "arki/nag.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/query.h"
#include "arki/scan.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <memory>

using namespace std::string_literals;
using namespace arki::utils;

namespace arki {

Segment::Segment(std::shared_ptr<const segment::Session> session, DataFormat format, const std::filesystem::path& relpath)
    : m_session(session), m_format(format), m_relpath(relpath), m_abspath(std::filesystem::weakly_canonical(session->root / relpath))
{
}

Segment::~Segment()
{
}

std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<const core::ReadLock> lock) const
{
    auto data = detect_data();

    // stat the metadata file, if it exists
    auto md_abspath = sys::with_suffix(abspath(), ".metadata");
    auto st_md = sys::stat(md_abspath);
    // If it exists and it looks new enough, use it
    if (st_md.get())
    {
        auto ts = data->timestamp();
        if (!ts)
        {
            nag::warning("%s: segment data is not available", abspath().c_str());
            return std::make_shared<segment::EmptyReader>(shared_from_this(), lock);
        }

        if (st_md->st_mtime >= ts.value())
            return std::make_shared<segment::metadata::Reader>(shared_from_this(), lock);
        else
            nag::warning("%s: outdated .metadata file: falling back to data scan", abspath().c_str());
    }

    // Else scan the file as usual
    //return std::make_shared<segment::scan::Reader>(reader);
    throw std::runtime_error("scan::Reader not yet implemented");
}

std::shared_ptr<segment::Data> Segment::detect_data() const
{
    std::unique_ptr<struct stat> st = sys::stat(abspath());
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            return std::make_shared<segment::data::dir::Data>(shared_from_this());
        else
            return segment::data::fd::Data::detect_data(shared_from_this());
    }

    st = sys::stat(sys::with_suffix(abspath(), ".gz"));
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
        {
            std::stringstream buf;
            buf << "cannot access data for " << format() << " directory " << relpath() << ": cannot handle a directory with a .gz extension";
            throw std::runtime_error(buf.str());
        }

        switch (format())
        {
            case DataFormat::GRIB:
            case DataFormat::BUFR:
                return std::make_shared<segment::data::gzconcat::Data>(shared_from_this());
            case DataFormat::VM2:
                return std::make_shared<segment::data::gzlines::Data>(shared_from_this());
            case DataFormat::ODIMH5:
            case DataFormat::NETCDF:
            case DataFormat::JPEG:
                return std::make_shared<segment::data::gzconcat::Data>(shared_from_this());
            default:
            {
                std::stringstream buf;
                buf << "cannot access data for " << format() << " file " << relpath()
                    << ": format not supported";
                throw std::runtime_error(buf.str());
            }
        }
    }

    st = sys::stat(sys::with_suffix(abspath(), ".tar"));
    if (st.get())
        return std::make_shared<segment::data::tar::Data>(shared_from_this());

    st = sys::stat(sys::with_suffix(abspath(), ".zip"));
    if (st.get())
        return std::make_shared<segment::data::zip::Data>(shared_from_this());

    // Segment not found on disk, create from defaults

    switch (format())
    {
        case DataFormat::GRIB:
        case DataFormat::BUFR:
            switch (session().default_file_segment)
            {
                case segment::DefaultFileSegment::SEGMENT_FILE:
                    return std::make_shared<segment::data::concat::Data>(shared_from_this());
                case segment::DefaultFileSegment::SEGMENT_GZ:
                    return std::make_shared<segment::data::gzconcat::Data>(shared_from_this());
                case segment::DefaultFileSegment::SEGMENT_DIR:
                    return std::make_shared<segment::data::dir::Data>(shared_from_this());
                case segment::DefaultFileSegment::SEGMENT_TAR:
                    return std::make_shared<segment::data::tar::Data>(shared_from_this());
                case segment::DefaultFileSegment::SEGMENT_ZIP:
                    return std::make_shared<segment::data::zip::Data>(shared_from_this());
                default:
                    throw std::runtime_error("Unknown default file segment");
            }
        case DataFormat::VM2:
            switch (session().default_file_segment)
            {
                case segment::DefaultFileSegment::SEGMENT_FILE:
                    return std::make_shared<segment::data::lines::Data>(shared_from_this());
                case segment::DefaultFileSegment::SEGMENT_GZ:
                    return std::make_shared<segment::data::gzlines::Data>(shared_from_this());
                case segment::DefaultFileSegment::SEGMENT_DIR:
                    return std::make_shared<segment::data::dir::Data>(shared_from_this());
                case segment::DefaultFileSegment::SEGMENT_TAR:
                    return std::make_shared<segment::data::tar::Data>(shared_from_this());
                case segment::DefaultFileSegment::SEGMENT_ZIP:
                    return std::make_shared<segment::data::zip::Data>(shared_from_this());
                default:
                    throw std::runtime_error("Unknown default file segment");
            }
        case DataFormat::ODIMH5:
        case DataFormat::NETCDF:
        case DataFormat::JPEG:
            switch (session().default_dir_segment)
            {
                case segment::DefaultDirSegment::SEGMENT_DIR:
                    return std::make_shared<segment::data::dir::Data>(shared_from_this());
                case segment::DefaultDirSegment::SEGMENT_TAR:
                    return std::make_shared<segment::data::tar::Data>(shared_from_this());
                case segment::DefaultDirSegment::SEGMENT_ZIP:
                    return std::make_shared<segment::data::zip::Data>(shared_from_this());
                default:
                    throw std::runtime_error("Unknown default dir segment");
            }
        default:
        {
            std::stringstream buf;
            buf << "cannot access data for " << format() << " file " << relpath()
                << ": format not supported";
            throw std::runtime_error(buf.str());
        }
    }
}

std::shared_ptr<segment::data::Reader> Segment::detect_data_reader(std::shared_ptr<const core::ReadLock> lock) const
{
    return detect_data()->reader(lock);
}

std::shared_ptr<segment::data::Writer> Segment::detect_data_writer(const segment::data::WriterConfig& config) const
{
    return detect_data()->writer(config, session().mock_data);
    /*
    std::shared_ptr<segment::data::Writer> res;

    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
        {
            if (segment::data::dir::Data::can_store(format))
            {
                if (mock_data)
                    res.reset(new segment::data::dir::HoleWriter(config, shared_from_this()));
                else
                    res.reset(new segment::data::dir::Writer(config, shared_from_this()));
            } else {
                throw_consistency_error(
                        "getting writer for "s + format + " file " + relpath.native(),
                        "format not supported");
            }
        } else {
            if (format == "grib" || format == "bufr")
            {
                if (mock_data)
                    res.reset(new segment::data::concat::HoleWriter(config, shared_from_this()));
                else
                    res.reset(new segment::data::concat::Writer(config, shared_from_this()));
            } else if (format == "vm2") {
                if (mock_data)
                    throw_consistency_error("mock_data single-file line-based segments are not implemented");
                else
                    res.reset(new segment::data::lines::Writer(config, shared_from_this()));
            } else if (format == "odimh5") {
                throw_consistency_error("segment is a file, but odimh5 data can only be stored into directory segments");
            } else if (format == "nc") {
                throw_consistency_error("segment is a file, but netcdf data can only be stored into directory segments");
            } else if (format == "jpeg") {
                throw_consistency_error("segment is a file, but jpeg data can only be stored into directory segments");
            } else {
                throw_consistency_error(
                        "getting segment for " + format + " file " + relpath.native(),
                        "format not supported");
            }
        }
        return res;
    }

    st = sys::stat(sys::with_suffix(abspath, ".gz"));
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting writer for "s + format + " file " + relpath.native(),
                    "cannot handle a directory with a .gz extension");
        else
            throw_consistency_error(
                    "getting writer for "s + format + " file " + relpath.native(),
                    "cannot write to .gz segments");
    }

    st = sys::stat(sys::with_suffix(abspath, ".tar"));
    if (st.get())
        throw_consistency_error(
                "getting writer for "s + format + " file " + relpath.native(),
                "cannot write to .tar segments");

    st = sys::stat(sys::with_suffix(abspath, ".zip"));
    if (st.get())
        throw_consistency_error(
                "getting writer for "s + format + " file " + relpath.native(),
                "cannot write to .zip segments");

    return res;
    */
}

std::shared_ptr<segment::data::Checker> Segment::detect_data_checker() const
{
    return detect_data()->checker(session().mock_data);
    /*
    std::shared_ptr<segment::data::Checker> res;

    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
        {
            if (segment::data::dir::Data::can_store(format))
            {
                if (mock_data)
                    res.reset(new segment::data::dir::HoleChecker(shared_from_this()));
                else
                    res.reset(new segment::data::dir::Checker(shared_from_this()));
            } else {
                throw_consistency_error(
                        "getting segment for " + format + " file " + relpath.native(),
                        "format not supported");
            }
        } else {
            if (format == "grib" || format == "bufr")
            {
                if (mock_data)
                    res.reset(new segment::data::concat::HoleChecker(shared_from_this()));
                else
                    res.reset(new segment::data::concat::Checker(shared_from_this()));
            } else if (format == "vm2") {
                if (mock_data)
                    throw_consistency_error("mockdata single-file line-based segments not implemented");
                else
                    res.reset(new segment::data::lines::Checker(shared_from_this()));
            } else if (format == "odimh5" || format == "nc" || format == "jpeg") {
                // If it's a file and we need a directory, still get a checker
                // so it can deal with it
                if (mock_data)
                    res.reset(new segment::data::dir::HoleChecker(shared_from_this()));
                else
                    res.reset(new segment::data::dir::Checker(shared_from_this()));
            } else {
                throw_consistency_error(
                        "getting segment for "s + format + " file " + relpath.native(),
                        "format not supported");
            }
        }
        return res;
    }

    st = sys::stat(sys::with_suffix(abspath, ".gz"));
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a directory with a .gz extension");

        if (format == "grib" || format == "bufr")
        {
            res.reset(new segment::data::gzconcat::Checker(shared_from_this()));
        } else if (format == "vm2") {
            res.reset(new segment::data::gzlines::Checker(shared_from_this()));
        } else if (format == "odimh5") {
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a gzipped odim file as a segment");
        } else if (format == "nc") {
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a gzipped netcdf file as a segment");
        } else if (format == "jpeg") {
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a gzipped jpeg file as a segment");
        } else {
            throw_consistency_error(
                    "getting segment for "s + format + " file " + relpath.native(),
                    "format not supported");
        }
    }

    st = sys::stat(sys::with_suffix(abspath, ".tar"));
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a directory with a .tar extension");
        res.reset(new segment::data::tar::Checker(shared_from_this()));
        return res;
    }

    st = sys::stat(sys::with_suffix(abspath, ".zip"));
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a directory with a .zip extension");
        res.reset(new segment::data::zip::Checker(shared_from_this()));
        return res;
    }

    return res;
    */
}

bool Segment::is_segment(const std::filesystem::path& abspath)
{
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
        auto format = scan::Scanner::format_from_filename(abspath.stem());
        return segment::data::zip::Data::can_store(format);
    }

    if (extension == ".gz")
    {
        if (S_ISDIR(st->st_mode))
            return false;
        return segment::data::gz::Data::can_store(
            scan::Scanner::format_from_filename(abspath.stem()));
    }

    if (extension == ".tar")
    {
        if (S_ISDIR(st->st_mode))
            return false;
        return segment::data::tar::Data::can_store(
                scan::Scanner::format_from_filename(abspath.stem()));
    }

    auto format = scan::Scanner::detect_format(abspath);
    if (not format)
        return false;

    if (!S_ISDIR(st->st_mode))
        return segment::data::fd::Data::can_store(format.value());

    if (!std::filesystem::exists(abspath / ".sequence"))
        return false;

    return segment::data::dir::Data::can_store(format.value());
}

std::filesystem::path Segment::basename(const std::filesystem::path& pathname)
{
    const auto& native = pathname.native();
    if (str::endswith(native, ".gz"))
        return native.substr(0, native.size() - 3);
    if (str::endswith(native, ".tar"))
        return native.substr(0, native.size() - 4);
    if (str::endswith(native, ".zip"))
        return native.substr(0, native.size() - 4);
    return pathname;
}


namespace segment {

Reader::Reader(std::shared_ptr<const Segment> segment, std::shared_ptr<const core::ReadLock> lock)
    : m_segment(segment), lock(lock)
{
}

Reader::~Reader()
{
}

#if 0
void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    query_data(query::Data(matcher), [&](std::shared_ptr<Metadata> md) { summary.add(*md); return true; });
}
#endif

bool EmptyReader::query_data(const query::Data&, metadata_dest_func)
{
    return true;
}

void EmptyReader::query_summary(const Matcher& matcher, Summary& summary)
{
}

}

}
