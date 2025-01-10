#include "gz.h"
#include "common.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/validator.h"
#include "arki/scan.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/iotrace.h"
#include <fcntl.h>
#include <vector>
#include <algorithm>
#include <sys/uio.h>

using namespace std;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki::segment::data {
namespace gz {

namespace {

struct Creator : public AppendCreator
{
    std::vector<uint8_t> padding;
    File out;
    compress::GzipWriter gzout;
    size_t written = 0;
    std::filesystem::path dest_abspath_idx;

    Creator(const Segment& segment, metadata::Collection& mds, const std::filesystem::path& dest_abspath)
        : AppendCreator(segment, mds), out(dest_abspath), gzout(out, 0)
    {
    }
    Creator(const Segment& segment, metadata::Collection& mds, const std::filesystem::path& dest_abspath, const std::filesystem::path& dest_abspath_idx, unsigned groupsize)
        : AppendCreator(segment, mds), out(dest_abspath), gzout(out, groupsize), dest_abspath_idx(dest_abspath_idx)
    {
    }

    size_t append(const metadata::Data& data) override
    {
        size_t wrpos = written;
        gzout.add(data.read());
        if (!padding.empty())
            gzout.add(padding);
        gzout.close_entry();
        written += data.size() + padding.size();
        return wrpos;
    }

    void create()
    {
        out.open(O_WRONLY | O_CREAT | O_TRUNC, 0666);
        AppendCreator::create();
        gzout.flush();
        out.fdatasync();
        out.close();
        if (!(dest_abspath_idx.empty() || gzout.idx.only_one_group()))
        {
            sys::File outidx(dest_abspath_idx, O_WRONLY | O_CREAT | O_TRUNC, 0666);
            gzout.idx.write(outidx);
            outidx.fdatasync();
            outidx.close();
        }
    }
};

template<typename Data>
struct CheckBackend : public AppendCheckBackend
{
    const std::filesystem::path& gzabspath;
    std::vector<uint8_t> all_data;

    CheckBackend(const std::filesystem::path& gzabspath, const Segment& segment, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, segment, mds), gzabspath(gzabspath)
    {
    }

    size_t actual_end(off_t offset, size_t size) const override
    {
        return offset + size + Data::padding;
    }

    void validate(Metadata& md, const types::source::Blob& source) override
    {
        validator->validate_buf(all_data.data() + source.offset, source.size);
    }

    size_t offset_end() const override { return all_data.size(); }

    State check()
    {
        std::unique_ptr<struct stat> st = sys::stat(gzabspath);
        if (st.get() == nullptr)
            return State(SEGMENT_DELETED);

        // Decompress all the segment in memory
        all_data = compress::gunzip(gzabspath);

        return AppendCheckBackend::check();
    }
};

}

time_t Data::timestamp() const
{
    return max(sys::timestamp(sys::with_suffix(segment().abspath, ".gz")), sys::timestamp(sys::with_suffix(segment().abspath, ".gz.idx"), 0));
}

bool Data::can_store(DataFormat format)
{
    switch (format)
    {
        case DataFormat::GRIB:
        case DataFormat::BUFR:
        case DataFormat::VM2:
            return true;
        default:
            return false;
    }
}

template<typename Data>
Reader<Data>::Reader(const std::shared_ptr<const Data> data, std::shared_ptr<core::Lock> lock)
    : BaseReader<Data>(data, lock), fd(sys::with_suffix(this->segment().abspath, ".gz"), O_RDONLY), reader(fd)
{
    // Read index
    std::filesystem::path idxfname = sys::with_suffix(fd.path(), ".idx");
    if (std::filesystem::exists(idxfname))
        reader.idx.read(idxfname);
}

template<typename Data>
bool Reader<Data>::scan_data(metadata_dest_func dest)
{
    auto scanner = scan::Scanner::get_scanner(this->segment().format);
    compress::TempUnzip uncompressed(this->segment().abspath);
    return scanner->scan_segment(this->shared_from_this(), dest);
}

template<typename Data>
std::vector<uint8_t> Reader<Data>::read(const types::source::Blob& src)
{
    vector<uint8_t> buf = reader.read(src.offset, src.size);
    iotrace::trace_file(this->segment().abspath, src.offset, src.size, "read data");
    return buf;
}


/*
 * Checker
 */

template<typename Data>
Checker<Data>::Checker(const std::shared_ptr<const Data> data)
    : BaseChecker<Data>(data), gzabspath(sys::with_suffix(this->segment().abspath, ".gz")), gzidxabspath(sys::with_suffix(this->segment().abspath, ".gz.idx"))
{
}

template<typename Data>
bool Checker<Data>::exists_on_disk()
{
    return std::filesystem::exists(gzabspath);
}

template<typename Data>
bool Checker<Data>::is_empty()
{
    if (sys::size(gzabspath) > 1024) return false;
    return utils::compress::gunzip(gzabspath).empty();
}

template<typename Data>
size_t Checker<Data>::size()
{
    return sys::size(gzabspath) + sys::size(gzidxabspath, 0);
}

template<typename Data>
void Checker<Data>::move_data(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath)
{
    std::filesystem::rename(gzabspath, sys::with_suffix(new_abspath, ".gz"));
    sys::rename_ifexists(gzidxabspath, sys::with_suffix(new_abspath, ".gz.idx"));
}

template<typename Data>
bool Checker<Data>::rescan_data(std::function<void(const std::string&)> reporter, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    auto reader = this->data().reader(lock);
    return reader->scan_data(dest);
}

template<typename Data>
State Checker<Data>::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend<Data> checker(gzabspath, this->segment(), reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

template<typename Data>
size_t Checker<Data>::remove()
{
    size_t size = this->size();
    sys::unlink(gzabspath);
    std::filesystem::remove(gzidxabspath);
    return size;
}

template<typename Data>
core::Pending Checker<Data>::repack(const std::filesystem::path& rootdir, metadata::Collection& mds, const RepackConfig& cfg)
{
    auto tmpabspath = sys::with_suffix(gzabspath, ".repack");
    files::FinalizeTempfilesTransaction* finalize;
    core::Pending p = finalize = new files::FinalizeTempfilesTransaction;
    finalize->tmpfiles.push_back(tmpabspath);

    if (cfg.gz_group_size == 0)
    {
        finalize->on_commit = [&](const std::vector<std::filesystem::path>& tmpfiles) {
            std::filesystem::rename(tmpfiles[0], gzabspath);
            std::filesystem::remove(sys::with_suffix(gzabspath, ".idx"));
        };

        Creator creator(this->segment(), mds, tmpabspath);
        creator.validator = &scan::Validator::by_filename(this->segment().abspath);
        if (Data::padding == 1) creator.padding.push_back('\n');
        creator.create();
    } else {
        auto tmpidxabspath = sys::with_suffix(gzidxabspath, ".repack");
        finalize->tmpfiles.push_back(tmpidxabspath);
        finalize->on_commit = [&](const std::vector<std::filesystem::path>& tmpfiles) {
            std::filesystem::rename(tmpfiles[0], this->segment().abspath);
            if (!sys::rename_ifexists(tmpfiles[1], gzidxabspath))
                std::filesystem::remove(gzidxabspath);
        };

        Creator creator(this->segment(), mds, tmpabspath, tmpidxabspath, cfg.gz_group_size);
        creator.validator = &scan::Validator::by_filename(this->segment().abspath);
        if (Data::padding == 1) creator.padding.push_back('\n');
        creator.create();
    }

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

template<typename Data>
void Checker<Data>::test_truncate(size_t offset)
{
    if (offset > 0)
        throw std::runtime_error("gz test_truncate not implemented for offset > 0");

    utils::files::PreserveFileTimes pft(gzabspath);

    std::filesystem::remove(gzabspath);
    std::filesystem::remove(gzidxabspath);

    sys::File out(gzabspath, O_WRONLY | O_CREAT | O_TRUNC);
    compress::GzipWriter writer(out);
    writer.flush();
    out.close();
}

template<typename Data>
void Checker<Data>::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    throw std::runtime_error("test_make_hole not implemented");
}

template<typename Data>
void Checker<Data>::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    throw std::runtime_error("test_make_overlap not implemented");
}

template<typename Data>
void Checker<Data>::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    utils::files::PreserveFileTimes pt(this->segment().abspath);
    sys::File fd(this->segment().abspath, O_RDWR);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}

}

namespace gzconcat {

const char* Data::type() const { return "gzconcat"; }
bool Data::single_file() const { return true; }
std::shared_ptr<data::Reader> Data::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(static_pointer_cast<const Data>(shared_from_this()), lock);
}
std::shared_ptr<data::Writer> Data::writer(const data::WriterConfig& config, bool mock_data) const
{
    throw std::runtime_error(std::string(type()) + " writing is not yet implemented");
}
std::shared_ptr<data::Checker> Data::checker(bool mock_data) const
{
    return make_shared<Checker>(static_pointer_cast<const Data>(shared_from_this()));
}
std::shared_ptr<data::Checker> Data::create(const Segment& segment, metadata::Collection& mds, const RepackConfig& cfg)
{
    if (cfg.gz_group_size == 0)
    {
        gz::Creator creator(segment, mds, sys::with_suffix(segment.abspath, ".gz"));
        creator.create();
    } else {
        gz::Creator creator(segment, mds, sys::with_suffix(segment.abspath, ".gz"), sys::with_suffix(segment.abspath, ".gz.idx"), cfg.gz_group_size);
        creator.create();
    }

    auto data = std::make_shared<const Data>(segment.shared_from_this());
    return make_shared<Checker>(data);
}

}

namespace gzlines {

const char* Data::type() const { return "gzlines"; }
bool Data::single_file() const { return true; }
std::shared_ptr<data::Reader> Data::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(static_pointer_cast<const Data>(shared_from_this()), lock);
}
std::shared_ptr<data::Writer> Data::writer(const data::WriterConfig& config, bool mock_data) const
{
    throw std::runtime_error(std::string(type()) + " writing is not yet implemented");
}
std::shared_ptr<data::Checker> Data::checker(bool mock_data) const
{
    return make_shared<Checker>(static_pointer_cast<const Data>(shared_from_this()));
}
std::shared_ptr<data::Checker> Data::create(const Segment& segment, metadata::Collection& mds, const RepackConfig& cfg)
{
    if (cfg.gz_group_size == 0)
    {
        gz::Creator creator(segment, mds, sys::with_suffix(segment.abspath, ".gz"));
        creator.padding.push_back('\n');
        creator.create();
    } else {
        gz::Creator creator(segment, mds, sys::with_suffix(segment.abspath, ".gz"), sys::with_suffix(segment.abspath, ".gz.idx"), cfg.gz_group_size);
        creator.padding.push_back('\n');
        creator.create();
    }
    auto data = std::make_shared<const Data>(segment.shared_from_this());
    return make_shared<Checker>(data);
}

}

namespace gz {
template class Reader<gzlines::Data>;
template class Checker<gzlines::Data>;
template class Reader<gzconcat::Data>;
template class Checker<gzconcat::Data>;
}

}
#include "base.tcc"
