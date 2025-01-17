#include "data.h"
#include "data/missing.h"
#include "data/fd.h"
#include "data/dir.h"
#include "data/tar.h"
#include "data/zip.h"
#include "data/gz.h"
#include "arki/exceptions.h"
#include "arki/stream.h"
#include "arki/metadata/collection.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/scan.h"

using namespace std;
using namespace std::string_literals;
using namespace arki::utils;
using arki::metadata::Collection;

namespace arki::segment {

Data::Data(std::shared_ptr<const Segment> segment) : m_segment(segment)
{
}

Data::~Data()
{
}


namespace data {

Reader::Reader(std::shared_ptr<const core::ReadLock> lock)
    : lock(lock)
{
}

Reader::~Reader()
{
}

stream::SendResult Reader::stream(const types::source::Blob& src, StreamOutput& out)
{
    vector<uint8_t> buf = read(src);
    switch (src.format)
    {
        case DataFormat::VM2:
            return out.send_line(buf.data(), buf.size());
        default:
            return out.send_buffer(buf.data(), buf.size());
    }
}


Writer::PendingMetadata::PendingMetadata(const WriterConfig& config, Metadata& md, std::unique_ptr<types::source::Blob> new_source)
    : config(config), md(md), new_source(new_source.release())
{
}

Writer::PendingMetadata::PendingMetadata(PendingMetadata&& o)
    : config(o.config), md(o.md), new_source(o.new_source)
{
    o.new_source = nullptr;
}

Writer::PendingMetadata::~PendingMetadata()
{
    delete new_source;
}

void Writer::PendingMetadata::set_source()
{
    std::unique_ptr<types::source::Blob> source(new_source);
    new_source = 0;
    md.set_source(std::move(source));
    if (config.drop_cached_data_on_commit)
        md.drop_cached_data();
}


RepackConfig::RepackConfig() {}
RepackConfig::RepackConfig(unsigned gz_group_size, unsigned test_flags)
    : gz_group_size(gz_group_size), test_flags(test_flags)
{
}


std::shared_ptr<segment::data::Checker> Checker::tar(Collection& mds)
{
    auto checker = segment::data::tar::Data::create(segment(), mds);
    remove();
    return checker;
}

std::shared_ptr<segment::data::Checker> Checker::zip(Collection& mds)
{
    auto checker = segment::data::zip::Data::create(segment(), mds);
    remove();
    return checker;
}

std::shared_ptr<segment::data::Checker> Checker::compress(Collection& mds, unsigned groupsize)
{
    std::shared_ptr<segment::data::Checker> res;
    switch (segment().format())
    {
        case DataFormat::VM2:
            res = segment::data::gzlines::Data::create(segment(), mds, RepackConfig(groupsize));
            break;
        default:
            res = segment::data::gzconcat::Data::create(segment(), mds, RepackConfig(groupsize));
    }
    remove();
    return res;
}

void Checker::test_truncate_by_data(const Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    test_truncate(s.offset);
}

}
}
