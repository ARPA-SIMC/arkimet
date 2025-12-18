#include "tar.h"
#include "arki/data/validator.h"
#include "arki/exceptions.h"
#include "arki/iotrace.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/data.h"
#include "arki/nag.h"
#include "arki/stream.h"
#include "arki/types/source/blob.h"
#include "arki/utils/accounting.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/tar.h"
#include "common.h"
#include <cstring>
#include <fcntl.h>
#include <sstream>
#include <sys/uio.h>
#include <system_error>
#include <vector>

using namespace std;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;
using arki::metadata::Collection;

namespace arki::segment::data::tar {

namespace {

struct Creator : public AppendCreator
{
    File out;
    TarOutput tarout;
    size_t idx = 0;
    char fname[100];

    Creator(const Segment& segment, Collection& mds,
            const std::filesystem::path& dest_abspath)
        : AppendCreator(segment, mds), out(dest_abspath), tarout(out)
    {
    }

    size_t append(const arki::metadata::Data& data) override
    {
        // Append it to the new file
        snprintf(fname, 99, "%06zu.%s", idx,
                 format_name(segment.format()).c_str());
        ++idx;
        return tarout.append(fname, data.read());
    }

    void create()
    {
        out.open(O_WRONLY | O_CREAT | O_TRUNC, 0666);
        AppendCreator::create();
        tarout.end();
        out.fdatasync();
        out.close();
    }
};

struct CheckBackend : public AppendCheckBackend
{
    core::File data;
    struct stat st;

    CheckBackend(const std::filesystem::path& tarabspath,
                 const Segment& segment,
                 std::function<void(const std::string&)> reporter,
                 const Collection& mds)
        : AppendCheckBackend(reporter, segment, mds), data(tarabspath)
    {
    }

    void validate(Metadata& md, const types::source::Blob& source) override
    {
        validator->validate_file(data, source.offset, source.size);
    }

    size_t offset_end() const override { return st.st_size - 1024; }

    size_t actual_start(off_t offset, size_t size) const override
    {
        return offset - 512;
    }

    size_t actual_end(off_t offset, size_t size) const override
    {
        return offset + size + 512 - (size % 512);
    }

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

} // namespace

const char* Data::type() const { return "tar"; }
bool Data::single_file() const { return true; }
std::optional<time_t> Data::timestamp() const
{
    if (auto st = sys::stat(sys::with_suffix(segment().abspath(), ".tar")))
        return std::optional<time_t>(st->st_mtime);
    return std::optional<time_t>();
}
bool Data::exists_on_disk() const
{
    return std::filesystem::exists(
        sys::with_suffix(segment().abspath(), ".tar"));
}

bool Data::is_empty() const
{
    struct stat st;
    sys::stat(sys::with_suffix(segment().abspath(), ".tar"), st);
    if (S_ISDIR(st.st_mode))
        return false;
    return st.st_size <= 1024;
}

size_t Data::size() const
{
    return sys::size(sys::with_suffix(segment().abspath(), ".tar"));
}

utils::files::PreserveFileTimes Data::preserve_mtime()
{
    return utils::files::PreserveFileTimes(
        sys::with_suffix(segment().abspath(), ".tar"));
}

std::shared_ptr<data::Reader>
Data::reader(std::shared_ptr<const core::ReadLock> lock) const
{
    return std::make_shared<Reader>(
        static_pointer_cast<const Data>(shared_from_this()), lock);
}
std::shared_ptr<data::Writer>
Data::writer(const segment::WriterConfig& config) const
{
    throw std::runtime_error(std::string(type()) +
                             " writing is not yet implemented");
    // return std::make_shared<Writer>(config, static_pointer_cast<const
    // Data>(shared_from_this()));
}
std::shared_ptr<data::Checker> Data::checker() const
{
    return std::make_shared<Checker>(
        static_pointer_cast<const Data>(shared_from_this()));
}
bool Data::can_store(DataFormat format) { return true; }
std::shared_ptr<const Data>
Data::create(const Segment& segment, Collection& mds, const RepackConfig& cfg)
{
    Creator creator(segment, mds, sys::with_suffix(segment.abspath(), ".tar"));
    creator.create();
    return std::make_shared<const Data>(segment.shared_from_this());
}

Reader::Reader(std::shared_ptr<const Data> data,
               std::shared_ptr<const core::ReadLock> lock)
    : data::BaseReader<Data>(data, lock),
      fd(sys::with_suffix(this->segment().abspath(), ".tar"), O_RDONLY
#ifdef linux
                                                                  | O_CLOEXEC
#endif
      )
{
}

bool Reader::scan_data(metadata_dest_func dest)
{
    throw std::runtime_error(std::string(data().type()) +
                             " scanning is not yet implemented");
}

std::vector<uint8_t> Reader::read(const types::source::Blob& src)
{
    std::vector<uint8_t> buf;
    buf.resize(src.size);

    if (posix_fadvise(fd, src.offset, src.size, POSIX_FADV_DONTNEED) != 0)
        nag::debug("fadvise on %s failed: %s", fd.path().c_str(),
                   strerror(errno));
    ssize_t res = fd.pread(buf.data(), src.size, src.offset);
    if ((size_t)res != src.size)
        throw_runtime_error("cannot read ", src.size, " bytes of ", src.format,
                            " data from ", fd.path(), ":", src.offset,
                            ": only ", res, "/", src.size,
                            " bytes have been read");
    acct::plain_data_read_count.incr();
    iotrace::trace_file(fd, src.offset, src.size, "read data");

    return buf;
}

stream::SendResult Reader::stream(const types::source::Blob& src,
                                  StreamOutput& out)
{
    if (src.format == DataFormat::VM2)
        return data::Reader::stream(src, out);

    iotrace::trace_file(fd, src.offset, src.size, "streamed data");
    return out.send_file_segment(fd, src.offset, src.size);
}

Checker::Checker(std::shared_ptr<const Data> data)
    : data::BaseChecker<Data>(data),
      tarabspath(sys::with_suffix(segment().abspath(), ".tar"))
{
}

void Checker::move_data(std::shared_ptr<const Segment> new_segment)
{
    auto new_tarabspath = sys::with_suffix(new_segment->abspath(), ".tar");
    if (rename(tarabspath.c_str(), new_tarabspath.c_str()) < 0)
    {
        std::stringstream ss;
        ss << "cannot rename " << tarabspath << " to " << new_tarabspath;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

bool Checker::rescan_data(std::function<void(const std::string&)> reporter,
                          std::shared_ptr<const core::ReadLock> lock,
                          metadata_dest_func dest)
{
    auto reader = this->data().reader(lock);
    return reader->scan_data(dest);
}

State Checker::check(std::function<void(const std::string&)> reporter,
                     const Collection& mds, bool quick)
{
    CheckBackend checker(tarabspath, segment(), reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

void Checker::validate(Metadata& md, const arki::data::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob())
    {
        if (blob->filename != segment().relpath())
            throw std::runtime_error(
                "metadata to validate does not appear to be from this segment");

        sys::File fd(tarabspath, O_RDONLY);
        v.validate_file(fd, blob->offset, blob->size);
        return;
    }
    const auto& data = md.get_data();
    auto buf         = data.read();
    v.validate_buf(
        buf.data(),
        buf.size()); // TODO: add a validate_data that takes the metadata::Data
}

size_t Checker::remove()
{
    size_t size = sys::size(tarabspath);
    sys::unlink(tarabspath);
    return size;
}

core::Pending Checker::repack(Collection& mds, const RepackConfig& cfg)
{
    auto tmpabspath = sys::with_suffix(segment().abspath(), ".repack");

    core::Pending p(new files::RenameTransaction(tmpabspath, tarabspath));

    Creator creator(segment(), mds, tmpabspath);
    creator.validator =
        &arki::data::Validator::by_filename(segment().abspath());
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md : mds)
        md->sourceBlob().unlock();

    return p;
}

void Checker::test_truncate(size_t offset)
{
    if (offset % 512 != 0)
        throw std::runtime_error(
            "tar test_truncate only works at multiples of 512");

    utils::files::PreserveFileTimes pft(tarabspath);

    sys::File out(tarabspath, O_CREAT | O_TRUNC | O_WRONLY);
    out.ftruncate(offset);
    out.ftruncate(offset + 512);
    out.close();
}

void Checker::test_make_hole(Collection& mds, unsigned hole_size,
                             unsigned data_idx)
{
    throw std::runtime_error("test_make_hole not implemented");
}

void Checker::test_make_overlap(Collection& mds, unsigned overlap_size,
                                unsigned data_idx)
{
    throw std::runtime_error("test_make_overlap not implemented");
}

void Checker::test_corrupt(const Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    utils::files::PreserveFileTimes pt(segment().abspath());
    sys::File fd(segment().abspath(), O_RDWR);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}

void Checker::test_touch_contents(time_t timestamp)
{
    sys::touch_ifexists(tarabspath, timestamp);
}

} // namespace arki::segment::data::tar
#include "base.tcc"
