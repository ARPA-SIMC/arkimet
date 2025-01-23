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
#include "arki/metadata/collection.h"
#include "arki/summary.h"
#include "arki/query.h"
#include "arki/types/source/blob.h"
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

std::filesystem::path Segment::abspath_metadata() const
{
    return sys::with_suffix(m_abspath, ".metadata");
}

std::filesystem::path Segment::abspath_summary() const
{
    return sys::with_suffix(m_abspath, ".summary");
}

std::filesystem::path Segment::abspath_iseg_index() const
{
    return sys::with_suffix(m_abspath, ".index");
}


std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<const core::ReadLock> lock) const
{
    return session().segment_reader(shared_from_this(), lock);
}

std::shared_ptr<segment::Checker> Segment::checker(std::shared_ptr<core::CheckLock> lock) const
{
    return session().segment_checker(shared_from_this(), lock);
}

std::shared_ptr<segment::Data> Segment::data() const
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

std::shared_ptr<segment::data::Reader> Segment::data_reader(std::shared_ptr<const core::ReadLock> lock) const
{
    return m_session->segment_data_reader(shared_from_this(), lock);
}

std::shared_ptr<segment::data::Writer> Segment::data_writer(const segment::data::WriterConfig& config) const
{
    return m_session->segment_data_writer(shared_from_this(), config);
}

std::shared_ptr<segment::data::Checker> Segment::data_checker() const
{
    return session().segment_data_checker(shared_from_this());
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

bool EmptyReader::read_all(metadata_dest_func dest)
{
    return true;
}

bool EmptyReader::query_data(const query::Data&, metadata_dest_func)
{
    return true;
}

void EmptyReader::query_summary(const Matcher& matcher, Summary& summary)
{
}


Checker::Checker(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock)
    : lock(lock), m_segment(segment), m_data(segment->data())
{
}

Checker::~Checker()
{
}

void Checker::update_data()
{
    m_data = m_segment->data();
}

Fixer::Fixer(std::shared_ptr<Checker> checker, std::shared_ptr<core::CheckWriteLock> lock)
    : lock(lock), m_checker(checker)
{
}


Fixer::~Fixer()
{
}

time_t Fixer::get_data_mtime_after_fix(const char* operation_desc)
{
    auto ts = data().timestamp();
    if (!ts)
    {
        std::stringstream buf;
        buf << segment().abspath() << ": segment data missing after " << operation_desc;
        throw std::runtime_error(buf.str());
    }
    return ts.value();
}

size_t Fixer::remove_ifexists(const std::filesystem::path& path)
{
    size_t res = 0;
    if (std::filesystem::exists(path))
    {
        res = sys::size(path);
        sys::unlink(path);
    }
    return res;
}

void Fixer::test_corrupt_data(unsigned data_idx)
{
    arki::metadata::Collection mds = m_checker->scan();
    data().checker()->test_corrupt(mds, data_idx);
}

void Fixer::test_truncate_data(unsigned data_idx)
{
    arki::metadata::Collection mds = m_checker->scan();
    const auto& s = mds[data_idx].sourceBlob();
    data().checker()->test_truncate(s.offset);
}

void Fixer::test_touch_contents(time_t timestamp)
{
    data().checker()->test_touch_contents(timestamp);
}

}

}
