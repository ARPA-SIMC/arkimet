#include "data.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/scan.h"
#include "arki/stream.h"
#include "arki/types/source/blob.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "data/dir.h"
#include "data/fd.h"
#include "data/gz.h"
#include "data/missing.h"
#include "data/tar.h"
#include "data/zip.h"

using namespace std;
using namespace std::string_literals;
using namespace arki::utils;
using arki::metadata::Collection;

namespace arki::segment {

Data::Data(std::shared_ptr<const Segment> segment) : m_segment(segment) {}

Data::~Data() {}

std::shared_ptr<segment::Data>
Data::create(std::shared_ptr<const Segment> segment)
{
    std::unique_ptr<struct stat> st = sys::stat(segment->abspath());
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
        {
            if (segment->session().mock_data)
                return std::make_shared<segment::data::dir::HoleData>(segment);
            else
                return std::make_shared<segment::data::dir::Data>(segment);
        }
        else
        {
            return segment::data::fd::Data::detect_data(
                segment, segment->session().mock_data);
        }
    }

    st = sys::stat(sys::with_suffix(segment->abspath(), ".gz"));
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
        {
            std::stringstream buf;
            buf << "cannot access data for " << segment->format()
                << " directory " << segment->relpath()
                << ": cannot handle a directory with a .gz extension";
            throw std::runtime_error(buf.str());
        }

        switch (segment->format())
        {
            case DataFormat::GRIB:
            case DataFormat::BUFR:
                return std::make_shared<segment::data::gzconcat::Data>(segment);
            case DataFormat::VM2:
                return std::make_shared<segment::data::gzlines::Data>(segment);
            case DataFormat::ODIMH5:
            case DataFormat::NETCDF:
            case DataFormat::JPEG:
                return std::make_shared<segment::data::gzconcat::Data>(segment);
            default: {
                std::stringstream buf;
                buf << "cannot access data for " << segment->format()
                    << " file " << segment->relpath()
                    << ": format not supported";
                throw std::runtime_error(buf.str());
            }
        }
    }

    st = sys::stat(sys::with_suffix(segment->abspath(), ".tar"));
    if (st.get())
        return std::make_shared<segment::data::tar::Data>(segment);

    st = sys::stat(sys::with_suffix(segment->abspath(), ".zip"));
    if (st.get())
        return std::make_shared<segment::data::zip::Data>(segment);

    // Segment not found on disk, create from defaults

    switch (segment->format())
    {
        case DataFormat::GRIB:
        case DataFormat::BUFR:
            switch (segment->session().default_file_segment)
            {
                case segment::DefaultFileSegment::SEGMENT_FILE:
                    return std::make_shared<segment::data::concat::Data>(
                        segment);
                case segment::DefaultFileSegment::SEGMENT_GZ:
                    return std::make_shared<segment::data::gzconcat::Data>(
                        segment);
                case segment::DefaultFileSegment::SEGMENT_DIR:
                    return std::make_shared<segment::data::dir::Data>(segment);
                case segment::DefaultFileSegment::SEGMENT_TAR:
                    return std::make_shared<segment::data::tar::Data>(segment);
                case segment::DefaultFileSegment::SEGMENT_ZIP:
                    return std::make_shared<segment::data::zip::Data>(segment);
                default:
                    throw std::runtime_error("Unknown default file segment");
            }
        case DataFormat::VM2:
            switch (segment->session().default_file_segment)
            {
                case segment::DefaultFileSegment::SEGMENT_FILE:
                    return std::make_shared<segment::data::lines::Data>(
                        segment);
                case segment::DefaultFileSegment::SEGMENT_GZ:
                    return std::make_shared<segment::data::gzlines::Data>(
                        segment);
                case segment::DefaultFileSegment::SEGMENT_DIR:
                    return std::make_shared<segment::data::dir::Data>(segment);
                case segment::DefaultFileSegment::SEGMENT_TAR:
                    return std::make_shared<segment::data::tar::Data>(segment);
                case segment::DefaultFileSegment::SEGMENT_ZIP:
                    return std::make_shared<segment::data::zip::Data>(segment);
                default:
                    throw std::runtime_error("Unknown default file segment");
            }
        case DataFormat::ODIMH5:
        case DataFormat::NETCDF:
        case DataFormat::JPEG:
            switch (segment->session().default_dir_segment)
            {
                case segment::DefaultDirSegment::SEGMENT_DIR:
                    return std::make_shared<segment::data::dir::Data>(segment);
                case segment::DefaultDirSegment::SEGMENT_TAR:
                    return std::make_shared<segment::data::tar::Data>(segment);
                case segment::DefaultDirSegment::SEGMENT_ZIP:
                    return std::make_shared<segment::data::zip::Data>(segment);
                default:
                    throw std::runtime_error("Unknown default dir segment");
            }
        default: {
            std::stringstream buf;
            buf << "cannot access data for " << segment->format() << " file "
                << segment->relpath() << ": format not supported";
            throw std::runtime_error(buf.str());
        }
    }
}

namespace data {

Reader::Reader(std::shared_ptr<const core::ReadLock> lock) : lock(lock) {}

Reader::~Reader() {}

stream::SendResult Reader::stream(const types::source::Blob& src,
                                  StreamOutput& out)
{
    vector<uint8_t> buf = read(src);
    switch (src.format)
    {
        case DataFormat::VM2: return out.send_line(buf.data(), buf.size());
        default:              return out.send_buffer(buf.data(), buf.size());
    }
}

Writer::PendingMetadata::PendingMetadata(
    const segment::WriterConfig& config, Metadata& md,
    std::unique_ptr<types::source::Blob> new_source)
    : config(config), md(md), new_source(new_source.release())
{
}

Writer::PendingMetadata::PendingMetadata(PendingMetadata&& o)
    : config(o.config), md(o.md), new_source(o.new_source)
{
    o.new_source = nullptr;
}

Writer::PendingMetadata::~PendingMetadata() { delete new_source; }

void Writer::PendingMetadata::set_source()
{
    std::unique_ptr<types::source::Blob> source(new_source);
    new_source = 0;
    md.set_source(std::move(source));
    if (config.drop_cached_data_on_commit)
        md.drop_cached_data();
}

std::shared_ptr<const segment::Data> Checker::tar(Collection& mds)
{
    auto new_data = segment::data::tar::Data::create(segment(), mds);
    remove();
    return new_data;
}

std::shared_ptr<const segment::Data> Checker::zip(Collection& mds)
{
    auto new_data = segment::data::zip::Data::create(segment(), mds);
    remove();
    return new_data;
}

std::shared_ptr<const segment::Data> Checker::compress(Collection& mds,
                                                       unsigned groupsize)
{
    std::shared_ptr<const Data> res;
    switch (segment().format())
    {
        case DataFormat::VM2:
            res = segment::data::gzlines::Data::create(segment(), mds,
                                                       RepackConfig(groupsize));
            break;
        default:
            res = segment::data::gzconcat::Data::create(
                segment(), mds, RepackConfig(groupsize));
    }
    remove();
    return res;
}

} // namespace data
} // namespace arki::segment
