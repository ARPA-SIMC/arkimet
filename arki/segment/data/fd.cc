#include "fd.h"
#include "common.h"
#include "missing.h"
#include "arki/exceptions.h"
#include "arki/stream.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/validator.h"
#include "arki/scan.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include "arki/utils/accounting.h"
#include "arki/utils/rearrange.h"
#include "arki/iotrace.h"
#include <system_error>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <sstream>
#include <system_error>

using namespace std;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;
using arki::metadata::Collection;

namespace arki::segment::data {
namespace fd {

void File::fdtruncate_nothrow(off_t pos) noexcept
{
    if (::ftruncate(*this, pos) == -1)
        nag::warning("truncating %s to previous size %zd (rollback of append operation): %s", path().c_str(), pos, strerror(errno));
}

namespace {

template<typename File>
struct Creator : public AppendCreator
{
    File out;
    size_t written = 0;

    Creator(const Segment& segment, Collection& mds, const std::filesystem::path& tmpabspath)
        : AppendCreator(segment, mds), out(tmpabspath, O_WRONLY | O_CREAT | O_TRUNC, 0666)
    {
    }

    size_t append(const arki::metadata::Data& data) override
    {
        size_t wrpos = written;
        written += out.write_data(data);
        return wrpos;
    }

    void create()
    {
        if (!out.is_open())
            out.open(O_WRONLY | O_CREAT | O_TRUNC, 0666);
        AppendCreator::create();
        out.fdatasync();
        out.close();
    }
};


template<typename Data>
struct CheckBackend : public AppendCheckBackend
{
    core::File data;
    struct stat st;

    CheckBackend(const Segment& segment, std::function<void(const std::string&)> reporter, const Collection& mds)
        : AppendCheckBackend(reporter, segment, mds), data(segment.abspath())
    {
    }
    size_t actual_end(off_t offset, size_t size) const override
    {
        return offset + size + Data::padding;
    }
    void validate(Metadata& md, const types::source::Blob& source) override
    {
        validator->validate_file(data, source.offset, source.size);
    }
    size_t offset_end() const override { return st.st_size; }
    State check()
    {
        if (!data.open_ifexists(O_RDONLY))
        {
            reporter(data.path().native() + " not found on disk");
            return State(SEGMENT_DELETED);
        }
        data.fstat(st);
        return AppendCheckBackend::check();
    }
};

}

std::shared_ptr<segment::Data> Data::detect_data(std::shared_ptr<const Segment> segment)
{
    switch (segment->format())
    {
        case DataFormat::GRIB:
        case DataFormat::BUFR:
            return std::make_shared<segment::data::concat::Data>(segment);
        case DataFormat::VM2:
            return std::make_shared<segment::data::lines::Data>(segment);
        // These would normally fit in a directory, but we allow a
        // degenerate one-data segment to be able to read a single file as
        // a segment
        case DataFormat::ODIMH5:
        case DataFormat::NETCDF:
        case DataFormat::JPEG:
            return std::make_shared<segment::data::single::Data>(segment);
        default:
        {
            std::stringstream buf;
            buf << "cannot access data for " << segment->format() << " file " << segment->relpath() << ": format not supported";
            throw std::runtime_error(buf.str());
        }
    }
}


std::optional<time_t> Data::timestamp() const
{
    if (auto st = sys::stat(segment().abspath()))
        return std::optional<time_t>(st->st_mtime);
    return std::optional<time_t>();
}

bool Data::exists_on_disk() const
{
    std::unique_ptr<struct stat> st = sys::stat(segment().abspath());
    if (!st) return false;
    if (S_ISDIR(st->st_mode)) return false;
    return true;
}

bool Data::is_empty() const
{
    struct stat st;
    sys::stat(segment().abspath(), st);
    if (S_ISDIR(st.st_mode)) return false;
    return st.st_size == 0;
}

size_t Data::size() const
{
    struct stat st;
    sys::stat(segment().abspath(), st);
    return st.st_size;
}

utils::files::PreserveFileTimes Data::preserve_mtime()
{
    return utils::files::PreserveFileTimes(segment().abspath());
}

template<typename Data>
Reader<Data>::Reader(std::shared_ptr<const Data> data, std::shared_ptr<const core::ReadLock> lock)
    : BaseReader<Data>(data, lock), fd(data->segment().abspath(), O_RDONLY
#ifdef linux
                | O_CLOEXEC
#endif
            )
{
}

template<typename Data>
bool Reader<Data>::scan_data(metadata_dest_func dest)
{
    const auto& segment = this->segment();
    auto scanner = arki::scan::Scanner::get_scanner(segment.format());
    return scanner->scan_segment(static_pointer_cast<data::Reader>(this->shared_from_this()), dest);
}

template<typename Data>
std::vector<uint8_t> Reader<Data>::read(const types::source::Blob& src)
{
    vector<uint8_t> buf;
    buf.resize(src.size);

    if (posix_fadvise(fd, src.offset, src.size, POSIX_FADV_DONTNEED) != 0)
        nag::debug("fadvise on %s failed: %s", fd.path().c_str(), strerror(errno));
    ssize_t res = fd.pread(buf.data(), src.size, src.offset);
    if ((size_t)res != src.size)
    {
        stringstream msg;
        msg << "cannot read " << src.size << " bytes of " << src.format << " data from " << fd.path() << ":"
            << src.offset << ": only " << res << "/" << src.size << " bytes have been read";
        throw std::runtime_error(msg.str());
    }
    acct::plain_data_read_count.incr();
    iotrace::trace_file(fd, src.offset, src.size, "read data");

    return buf;
}

template<typename Data>
stream::SendResult Reader<Data>::stream(const types::source::Blob& src, StreamOutput& out)
{
    if (src.format == DataFormat::VM2)
        return data::Reader::stream(src, out);

    iotrace::trace_file(fd, src.offset, src.size, "streamed data");
    return out.send_file_segment(fd, src.offset, src.size);
}


template<typename Data, typename File>
Writer<Data, File>::Writer(const segment::WriterConfig& config, std::shared_ptr<const Data> data, int mode)
    : BaseWriter<Data>(config, data), fd(data->segment().abspath(), O_WRONLY | O_CREAT | mode, 0666)
{
    struct stat st;
    this->fd.fstat(st);
    initial_mtime = st.st_mtim;
    initial_size = this->fd.lseek(0, SEEK_END);
    current_pos = initial_size;
}

template<typename Data, typename File>
Writer<Data, File>::~Writer()
{
    if (!this->fired) rollback_nothrow();
}

template<typename Data, typename File>
size_t Writer<Data, File>::next_offset() const { return current_pos; }

template<typename Data, typename File>
const types::source::Blob& Writer<Data, File>::append(Metadata& md)
{
    this->fired = false;
    const arki::metadata::Data& data = md.get_data();
    pending.emplace_back(this->config, md, source::Blob::create_unlocked(this->segment().format(), this->segment().root(), this->segment().relpath(), current_pos, data.size()));
    current_pos += fd.write_data(data);
    return *pending.back().new_source;
}

template<typename Data, typename File>
void Writer<Data, File>::commit()
{
    if (this->fired) return;
    if (!this->segment().session().eatmydata)
        fd.fsync();
    for (auto& p: pending)
        p.set_source();
    pending.clear();
    initial_size = current_pos;
    this->fired = true;
}

template<typename Data, typename File>
void Writer<Data, File>::rollback()
{
    if (this->fired) return;
    fd.ftruncate(initial_size);
    fd.lseek(initial_size, SEEK_SET);
    const struct timespec times[2] = { { 0, UTIME_OMIT}, initial_mtime };
    fd.futimens(times);
    current_pos = initial_size;
    pending.clear();
    this->fired = true;
}

template<typename Data, typename File>
void Writer<Data, File>::rollback_nothrow() noexcept
{
    if (this->fired) return;
    fd.fdtruncate_nothrow(initial_size);
    ::lseek(fd, initial_size, SEEK_SET);
    const struct timespec times[2] = { { 0, UTIME_OMIT}, initial_mtime };
    ::futimens(fd, times);
    pending.clear();
    this->fired = true;
}


template<typename Data, typename File>
Checker<Data, File>::Checker(std::shared_ptr<const Data> data)
    : BaseChecker<Data>(data)
{
}

template<typename Data, typename File>
bool Checker<Data, File>::rescan_data(std::function<void(const std::string&)>, std::shared_ptr<const core::ReadLock> lock, metadata_dest_func dest)
{
    auto reader = this->data().reader(lock);
    return reader->scan_data(dest);
}

template<typename Data, typename File>
State Checker<Data, File>::check(std::function<void(const std::string&)> reporter, const Collection& mds, bool quick)
{
    CheckBackend<Data> checker(this->segment(), reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

template<typename Data, typename File>
void Checker<Data, File>::move_data(std::shared_ptr<const Segment> new_segment)
{
    std::filesystem::rename(this->segment().abspath(), new_segment->abspath());
}

template<typename Data, typename File>
size_t Checker<Data, File>::remove()
{
    size_t size = sys::size(this->segment().abspath());
    sys::unlink(this->segment().abspath().c_str());
    return size;
}

template<typename Data, typename File>
core::Pending Checker<Data, File>::repack(Collection& mds, const RepackConfig& cfg)
{
    auto tmpabspath = sys::with_suffix(this->segment().abspath(), ".repack");

    core::Pending p(new files::RenameTransaction(tmpabspath, this->segment().abspath()));

    Creator<File> creator(this->segment(), mds, tmpabspath);
    creator.validator = &arki::scan::Validator::by_filename(this->segment().abspath());
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}


template<typename Data, typename File>
void Checker<Data, File>::test_truncate(size_t offset)
{
    const auto& segment = this->segment();
    if (!std::filesystem::exists(segment.abspath()))
        sys::write_file(segment.abspath(), "");

    utils::files::PreserveFileTimes pft(segment.abspath());
    if (::truncate(segment.abspath().c_str(), offset) < 0)
    {
        stringstream ss;
        ss << "cannot truncate " << segment.abspath() << " at " << offset;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

template<typename Data, typename File>
void Checker<Data, File>::test_make_hole(Collection& mds, unsigned hole_size, unsigned data_idx)
{
    utils::files::PreserveFileTimes pt(this->segment().abspath());
    sys::File fd(this->segment().abspath(), O_RDWR);
    off_t end = fd.lseek(0, SEEK_END);
    if (data_idx >= mds.size())
    {
        fd.ftruncate(end + hole_size);
    } else {
        off_t start_ofs = mds[data_idx].sourceBlob().offset;
        std::vector<uint8_t> buf(end - start_ofs);
        fd.lseek(start_ofs);
        fd.read_all_or_throw(buf.data(), buf.size());
        fd.lseek(start_ofs + hole_size);
        fd.write_all_or_throw(buf.data(), buf.size());

        for (unsigned i = data_idx; i < mds.size(); ++i)
        {
            unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
            source->offset += hole_size;
            mds[i].set_source(std::move(source));
        }
    }
}

template<typename Data, typename File>
void Checker<Data, File>::test_make_overlap(Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    utils::files::PreserveFileTimes pt(this->segment().abspath());
    sys::File fd(this->segment().abspath(), O_RDWR);
    off_t start_ofs = mds[data_idx].sourceBlob().offset;
    off_t end = fd.lseek(0, SEEK_END);
    std::vector<uint8_t> buf(end - start_ofs);
    fd.lseek(start_ofs);
    fd.read_all_or_throw(buf.data(), buf.size());
    fd.lseek(start_ofs - overlap_size);
    fd.write_all_or_throw(buf.data(), buf.size());
    fd.ftruncate(end - overlap_size);

    for (unsigned i = data_idx; i < mds.size(); ++i)
    {
        unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
        source->offset -= overlap_size;
        mds[i].set_source(std::move(source));
    }
}

template<typename Data, typename File>
void Checker<Data, File>::test_corrupt(const Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    utils::files::PreserveFileTimes pt(this->segment().abspath());
    sys::File fd(this->segment().abspath(), O_RDWR);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}

template<typename Data, typename File>
void Checker<Data, File>::test_touch_contents(time_t timestamp)
{
    sys::touch_ifexists(this->segment().abspath(), timestamp);
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

}

namespace single {

const char* Data::type() const { return "single"; }
bool Data::single_file() const { return true; }
std::shared_ptr<data::Reader> Data::reader(std::shared_ptr<const core::ReadLock> lock) const
{
    try {
        return make_shared<Reader>(static_pointer_cast<const Data>(shared_from_this()), lock);
    } catch (std::system_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory)
            return make_shared<arki::segment::data::missing::Reader>(shared_from_this(), lock);
        else
            throw;
    }
}
std::shared_ptr<data::Writer> Data::writer(const segment::WriterConfig& config) const
{
    throw std::runtime_error("cannot store " + format_name(segment().format()) + " using fd::single writer");
}
std::shared_ptr<data::Checker> Data::checker() const
{
    throw std::runtime_error("cannot store " + format_name(segment().format()) + " using fd::single writer");
}

}

namespace concat {

size_t File::write_data(const arki::metadata::Data& data)
{
    // Prevent caching (ignore function result)
    //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

    // Append the data
    return data.write(*this);
}

void File::test_add_padding(size_t size)
{
    for (unsigned i = 0; i < size; ++i)
        write("", 1);
}

size_t HoleFile::write_data(const arki::metadata::Data& data)
{
    // Get the current file size
    off_t size = lseek(0, SEEK_END);

    // Enlarge its apparent size to include the size of buf
    ftruncate(size + data.size());

    return data.size();
}
void HoleFile::test_add_padding(size_t)
{
    throw std::runtime_error("HoleFile::test_add_padding not implemented");
}

const char* Data::type() const { return "concat"; }
bool Data::single_file() const { return true; }
std::shared_ptr<data::Reader> Data::reader(std::shared_ptr<const core::ReadLock> lock) const
{
    try {
        return make_shared<Reader>(static_pointer_cast<const Data>(shared_from_this()), lock);
    } catch (std::system_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory)
            return make_shared<arki::segment::data::missing::Reader>(shared_from_this(), lock);
        else
            throw;
    }
}
std::shared_ptr<data::Writer> Data::writer(const segment::WriterConfig& config) const
{
    if (session().mock_data)
        return make_shared<HoleWriter>(config, static_pointer_cast<const Data>(shared_from_this()));
    else
        return make_shared<Writer>(config, static_pointer_cast<const Data>(shared_from_this()));
}
std::shared_ptr<data::Checker> Data::checker() const
{
    if (session().mock_data)
        return make_shared<HoleChecker>(static_pointer_cast<const Data>(shared_from_this()));
    else
        return make_shared<Checker>(static_pointer_cast<const Data>(shared_from_this()));
}

std::shared_ptr<data::Checker> Data::create(const Segment& segment, Collection& mds, const RepackConfig& cfg)
{
    fd::Creator<File> creator(segment, mds, segment.abspath());
    creator.create();
    auto data = std::make_shared<const Data>(segment.shared_from_this());
    return make_shared<Checker>(data);
}

core::Pending Checker::repack(Collection& mds, const RepackConfig& cfg)
{
#if 1
    // Build the rearrange plan
    rearrange::Plan plan;
    size_t dst_offset = 0;
    for (auto& md: mds)
    {
        auto& blob = md->sourceBlob();
        plan.add(blob.offset, dst_offset, blob.size);
        blob.offset = dst_offset;
        dst_offset += blob.size;
    }

    auto tmpabspath = sys::with_suffix(this->segment().abspath(), ".repack");
    core::Pending p(new files::RenameTransaction(tmpabspath, this->segment().abspath()));

    {
        core::File in(this->segment().abspath(), O_RDONLY);
        core::File out(tmpabspath, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        plan.execute(in, out);
    }

    // Release locks on the source segment, which is about to go away
    for (auto& md: mds)
        md->sourceBlob().unlock();

    return p;
#else
    return fd::Checker<Data, File>::repack(mds, cfg);
#endif
}

core::Pending HoleChecker::repack(Collection& mds, const RepackConfig& cfg)
{
    filesystem::path tmpabspath = sys::with_suffix(segment().abspath(), ".repack");

    core::Pending p(new files::RenameTransaction(tmpabspath, segment().abspath()));

    fd::Creator<HoleFile> creator(segment(), mds, tmpabspath);
    // Skip validation, since all data reads as zeroes
    // creator.validator = &scan::Validator::by_filename(abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

}

namespace lines {

void File::test_add_padding(size_t size)
{
    for (unsigned i = 0; i < size; ++i)
        write("\n", 1);
}

const char* Data::type() const { return "lines"; }
bool Data::single_file() const { return true; }
std::shared_ptr<data::Reader> Data::reader(std::shared_ptr<const core::ReadLock> lock) const
{
    try {
        return make_shared<Reader>(static_pointer_cast<const Data>(shared_from_this()), lock);
    } catch (std::system_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory)
            return make_shared<arki::segment::data::missing::Reader>(shared_from_this(), lock);
        else
            throw;
    }
}
std::shared_ptr<data::Writer> Data::writer(const segment::WriterConfig& config) const
{
    return make_shared<Writer>(config, static_pointer_cast<const Data>(shared_from_this()));
}
std::shared_ptr<data::Checker> Data::checker() const
{
    return make_shared<Checker>(static_pointer_cast<const Data>(shared_from_this()));
}
std::shared_ptr<data::Checker> Data::create(const Segment& segment, Collection& mds, const RepackConfig& cfg)
{
    fd::Creator<File> creator(segment, mds, segment.abspath());
    creator.create();
    auto data = std::make_shared<const Data>(segment.shared_from_this());
    return make_shared<Checker>(data);
}

}

namespace fd {
template class fd::Reader<single::Data>;
template class fd::Reader<concat::Data>;
template class fd::Writer<concat::Data, concat::File>;
template class fd::Checker<concat::Data, concat::File>;
template class fd::Writer<concat::Data, concat::HoleFile>;
template class fd::Checker<concat::Data, concat::HoleFile>;
template class fd::Reader<lines::Data>;
template class fd::Writer<lines::Data, lines::File>;
template class fd::Checker<lines::Data, lines::File>;
}

}
#include "base.tcc"
