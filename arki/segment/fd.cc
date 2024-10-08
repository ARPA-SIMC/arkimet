#include "fd.h"
#include "common.h"
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
#include "arki/iotrace.h"
#include <system_error>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <sstream>

using namespace std;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace segment {
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

    Creator(const std::filesystem::path& root, const std::filesystem::path& relpath, metadata::Collection& mds, const std::filesystem::path& tmpabspath)
        : AppendCreator(root, relpath, mds), out(tmpabspath, O_WRONLY | O_CREAT | O_TRUNC, 0666)
    {
    }

    size_t append(const metadata::Data& data) override
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


template<typename Segment>
struct CheckBackend : public AppendCheckBackend
{
    core::File data;
    struct stat st;

    CheckBackend(const std::filesystem::path& abspath, const std::filesystem::path& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, relpath, mds), data(abspath)
    {
    }
    size_t actual_end(off_t offset, size_t size) const override
    {
        return offset + size + Segment::padding;
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
            return SEGMENT_DELETED;
        }
        data.fstat(st);
        return AppendCheckBackend::check();
    }
};

}


time_t Segment::timestamp() const
{
    struct stat st;
    sys::stat(abspath, st);
    return st.st_mtime;
}


template<typename Segment>
Reader<Segment>::Reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath, std::shared_ptr<core::Lock> lock)
    : BaseReader<Segment>(format, root, relpath, abspath, lock), fd(abspath, O_RDONLY
#ifdef linux
                | O_CLOEXEC
#endif
            )
{
}

template<typename Segment>
bool Reader<Segment>::scan_data(metadata_dest_func dest)
{
    const auto& segment = this->segment();
    auto scanner = scan::Scanner::get_scanner(segment.format);
    return scanner->scan_segment(static_pointer_cast<segment::Reader>(this->shared_from_this()), dest);
}

template<typename Segment>
std::vector<uint8_t> Reader<Segment>::read(const types::source::Blob& src)
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

template<typename Segment>
stream::SendResult Reader<Segment>::stream(const types::source::Blob& src, StreamOutput& out)
{
    if (src.format == "vm2")
        return arki::segment::Reader::stream(src, out);

    iotrace::trace_file(fd, src.offset, src.size, "streamed data");
    return out.send_file_segment(fd, src.offset, src.size);
}


template<typename Segment, typename File>
Writer<Segment, File>::Writer(const WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath, int mode)
    : BaseWriter<Segment>(config, format, root, relpath, abspath), fd(abspath, O_WRONLY | O_CREAT | mode, 0666)
{
    struct stat st;
    this->fd.fstat(st);
    initial_mtime = st.st_mtim;
    initial_size = this->fd.lseek(0, SEEK_END);
    current_pos = initial_size;
}

template<typename Segment, typename File>
Writer<Segment, File>::~Writer()
{
    if (!this->fired) rollback_nothrow();
}

template<typename Segment, typename File>
size_t Writer<Segment, File>::next_offset() const { return current_pos; }

template<typename Segment, typename File>
const types::source::Blob& Writer<Segment, File>::append(Metadata& md)
{
    const auto& segment = this->segment();
    this->fired = false;
    const metadata::Data& data = md.get_data();
    pending.emplace_back(this->config, md, source::Blob::create_unlocked(segment.format, segment.root, segment.relpath, current_pos, data.size()));
    current_pos += fd.write_data(data);
    return *pending.back().new_source;
}

template<typename Segment, typename File>
void Writer<Segment, File>::commit()
{
    if (this->fired) return;
    if (!this->config.eatmydata)
        fd.fsync();
    for (auto& p: pending)
        p.set_source();
    pending.clear();
    initial_size = current_pos;
    this->fired = true;
}

template<typename Segment, typename File>
void Writer<Segment, File>::rollback()
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

template<typename Segment, typename File>
void Writer<Segment, File>::rollback_nothrow() noexcept
{
    if (this->fired) return;
    fd.fdtruncate_nothrow(initial_size);
    ::lseek(fd, initial_size, SEEK_SET);
    const struct timespec times[2] = { { 0, UTIME_OMIT}, initial_mtime };
    ::futimens(fd, times);
    pending.clear();
    this->fired = true;
}


template<typename Segment, typename File>
Checker<Segment, File>::Checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
    : BaseChecker<Segment>(format, root, relpath, abspath)
{
}

template<typename Segment, typename File>
bool Checker<Segment, File>::exists_on_disk()
{
    std::unique_ptr<struct stat> st = sys::stat(this->segment().abspath);
    if (!st) return false;
    if (S_ISDIR(st->st_mode)) return false;
    return true;
}

template<typename Segment, typename File>
bool Checker<Segment, File>::is_empty()
{
    struct stat st;
    sys::stat(this->segment().abspath, st);
    if (S_ISDIR(st.st_mode)) return false;
    return st.st_size == 0;
}

template<typename Segment, typename File>
size_t Checker<Segment, File>::size()
{
    struct stat st;
    sys::stat(this->segment().abspath, st);
    return st.st_size;
}

template<typename Segment, typename File>
bool Checker<Segment, File>::rescan_data(std::function<void(const std::string&)> reporter, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    auto reader = this->segment().reader(lock);
    return reader->scan_data(dest);
}

template<typename Segment, typename File>
State Checker<Segment, File>::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend<Segment> checker(this->segment().abspath, this->segment().relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

template<typename Segment, typename File>
void Checker<Segment, File>::move_data(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath)
{
    std::filesystem::rename(this->segment().abspath, new_abspath);
}

template<typename Segment, typename File>
size_t Checker<Segment, File>::remove()
{
    size_t size = sys::size(this->segment().abspath);
    sys::unlink(this->segment().abspath.c_str());
    return size;
}

template<typename Segment, typename File>
core::Pending Checker<Segment, File>::repack(const std::filesystem::path& rootdir, metadata::Collection& mds, const RepackConfig& cfg)
{
    auto tmpabspath = sys::with_suffix(this->segment().abspath, ".repack");

    core::Pending p(new files::RenameTransaction(tmpabspath, this->segment().abspath));

    Creator<File> creator(rootdir, this->segment().relpath, mds, tmpabspath);
    creator.validator = &scan::Validator::by_filename(this->segment().abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

template<typename Segment, typename File>
void Checker<Segment, File>::test_truncate(size_t offset)
{
    const auto& segment = this->segment();
    if (!std::filesystem::exists(segment.abspath))
        sys::write_file(segment.abspath, "");

    utils::files::PreserveFileTimes pft(segment.abspath);
    if (::truncate(segment.abspath.c_str(), offset) < 0)
    {
        stringstream ss;
        ss << "cannot truncate " << segment.abspath << " at " << offset;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

template<typename Segment, typename File>
void Checker<Segment, File>::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    utils::files::PreserveFileTimes pt(this->segment().abspath);
    sys::File fd(this->segment().abspath, O_RDWR);
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

template<typename Segment, typename File>
void Checker<Segment, File>::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    utils::files::PreserveFileTimes pt(this->segment().abspath);
    sys::File fd(this->segment().abspath, O_RDWR);
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

template<typename Segment, typename File>
void Checker<Segment, File>::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    utils::files::PreserveFileTimes pt(this->segment().abspath);
    sys::File fd(this->segment().abspath, O_RDWR);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}


bool Segment::can_store(const std::string& format)
{
    return format == "grib" || format == "grib1" || format == "grib2"
        || format == "bufr" || format == "vm2";
}

}


namespace concat {

size_t File::write_data(const metadata::Data& data)
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

size_t HoleFile::write_data(const metadata::Data& data)
{
    // Get the current file size
    off_t size = lseek(0, SEEK_END);

    // Enlarge its apparent size to include the size of buf
    ftruncate(size + data.size());

    return data.size();
}
void HoleFile::test_add_padding(size_t size)
{
    throw std::runtime_error("HoleFile::test_add_padding not implemented");
}

const char* Segment::type() const { return "concat"; }
bool Segment::single_file() const { return true; }
std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}
std::shared_ptr<segment::Checker> Segment::checker() const
{
    return make_shared<Checker>(format, root, relpath, abspath);
}
std::shared_ptr<segment::Writer> Segment::make_writer(const WriterConfig& config, const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
{
    return make_shared<Writer>(config, format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath, metadata::Collection& mds, const RepackConfig& cfg)
{
    fd::Creator<File> creator(rootdir, relpath, mds, abspath);
    creator.create();
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
bool Segment::can_store(const std::string& format)
{
    return format == "grib" || format == "bufr";
}


const char* HoleSegment::type() const { return "hole_concat"; }
bool HoleSegment::single_file() const { return true; }
std::shared_ptr<segment::Reader> HoleSegment::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}
std::shared_ptr<segment::Checker> HoleSegment::checker() const
{
    return make_shared<HoleChecker>(format, root, relpath, abspath);
}
std::shared_ptr<segment::Writer> HoleSegment::make_writer(const WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
{
    return make_shared<HoleWriter>(config, format, root, relpath, abspath);
}
std::shared_ptr<segment::Checker> HoleSegment::make_checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
{
    return make_shared<HoleChecker>(format, root, relpath, abspath);
}

core::Pending HoleChecker::repack(const std::filesystem::path& rootdir, metadata::Collection& mds, const RepackConfig& cfg)
{
    filesystem::path tmpabspath = sys::with_suffix(segment().abspath, ".repack");

    core::Pending p(new files::RenameTransaction(tmpabspath, segment().abspath));

    fd::Creator<HoleFile> creator(rootdir, segment().relpath, mds, tmpabspath);
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

const char* Segment::type() const { return "lines"; }
bool Segment::single_file() const { return true; }
std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}
std::shared_ptr<segment::Checker> Segment::checker() const
{
    return make_shared<Checker>(format, root, relpath, abspath);
}
std::shared_ptr<segment::Writer> Segment::make_writer(const WriterConfig& config, const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
{
    return make_shared<Writer>(config, format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath, metadata::Collection& mds, const RepackConfig& cfg)
{
    fd::Creator<File> creator(rootdir, relpath, mds, abspath);
    creator.create();
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
bool Segment::can_store(const std::string& format)
{
    return format == "vm2";
}

}

namespace fd {
template class fd::Reader<concat::Segment>;
template class fd::Writer<concat::Segment, concat::File>;
template class fd::Checker<concat::Segment, concat::File>;
template class fd::Writer<concat::HoleSegment, concat::HoleFile>;
template class fd::Checker<concat::HoleSegment, concat::HoleFile>;
template class fd::Reader<lines::Segment>;
template class fd::Writer<lines::Segment, lines::File>;
template class fd::Checker<lines::Segment, lines::File>;
}

}
}
#include "base.tcc"
