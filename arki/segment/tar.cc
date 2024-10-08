#include "tar.h"
#include "common.h"
#include "arki/exceptions.h"
#include "arki/stream.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/validator.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/tar.h"
#include "arki/nag.h"
#include "arki/utils/accounting.h"
#include "arki/iotrace.h"
#include <fcntl.h>
#include <vector>
#include <sys/uio.h>
#include <system_error>
#include <cstring>
#include <sstream>

using namespace std;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace segment {
namespace tar {

namespace {

struct Creator : public AppendCreator
{
    std::string format;
    File out;
    TarOutput tarout;
    size_t idx = 0;
    char fname[100];

    Creator(const std::filesystem::path& root, const std::filesystem::path& relpath, metadata::Collection& mds, const std::filesystem::path& dest_abspath)
        : AppendCreator(root, relpath, mds), out(dest_abspath), tarout(out)
    {
        if (!mds.empty())
            format = mds[0].source().format;
    }

    size_t append(const metadata::Data& data) override
    {
        // Append it to the new file
        snprintf(fname, 99, "%06zu.%s", idx, format.c_str());
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

    CheckBackend(const std::filesystem::path& tarabspath, const std::filesystem::path& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, relpath, mds), data(tarabspath)
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
            return SEGMENT_DELETED;
        }
        data.fstat(st);
        return AppendCheckBackend::check();
    }
};

}

const char* Segment::type() const { return "tar"; }
bool Segment::single_file() const { return true; }
time_t Segment::timestamp() const { return sys::timestamp(sys::with_suffix(abspath, ".tar")); }
std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}
std::shared_ptr<segment::Checker> Segment::checker() const
{
    return make_shared<Checker>(format, root, relpath, abspath);
}
bool Segment::can_store(const std::string& format)
{
    return true;
}
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath, metadata::Collection& mds, const RepackConfig& cfg)
{
    Creator creator(rootdir, relpath, mds, sys::with_suffix(abspath, ".tar"));
    creator.create();
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}



Reader::Reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath, std::shared_ptr<core::Lock> lock)
    : segment::BaseReader<Segment>(format, root, relpath, abspath, lock), fd(sys::with_suffix(abspath, ".tar"), O_RDONLY
#ifdef linux
                | O_CLOEXEC
#endif
            )
{
}

bool Reader::scan_data(metadata_dest_func dest)
{
    throw std::runtime_error(string(m_segment.type()) + " scanning is not yet implemented");
}

std::vector<uint8_t> Reader::read(const types::source::Blob& src)
{
    vector<uint8_t> buf;
    buf.resize(src.size);

    if (posix_fadvise(fd, src.offset, src.size, POSIX_FADV_DONTNEED) != 0)
        nag::debug("fadvise on %s failed: %s", fd.path().c_str(), strerror(errno));
    ssize_t res = fd.pread(buf.data(), src.size, src.offset);
    if ((size_t)res != src.size)
        throw_runtime_error(
            "cannot read ", src.size, " bytes of ", src.format, " data from ", fd.path(), ":",
            src.offset, ": only ", res, "/", src.size, " bytes have been read");
    acct::plain_data_read_count.incr();
    iotrace::trace_file(fd, src.offset, src.size, "read data");

    return buf;
}

stream::SendResult Reader::stream(const types::source::Blob& src, StreamOutput& out)
{
    if (src.format == "vm2")
        return arki::segment::Reader::stream(src, out);

    iotrace::trace_file(fd, src.offset, src.size, "streamed data");
    return out.send_file_segment(fd, src.offset, src.size);
}


Checker::Checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
    : segment::BaseChecker<Segment>(format, root, relpath, abspath), tarabspath(sys::with_suffix(abspath, ".tar"))
{
}

bool Checker::exists_on_disk()
{
    return std::filesystem::exists(tarabspath);
}

bool Checker::is_empty()
{
    struct stat st;
    sys::stat(tarabspath, st);
    if (S_ISDIR(st.st_mode)) return false;
    return st.st_size <= 1024;
}

size_t Checker::size()
{
    return sys::size(tarabspath);
}

void Checker::move_data(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath)
{
    auto new_tarabspath = sys::with_suffix(new_abspath, ".tar");
    if (rename(tarabspath.c_str(), new_tarabspath.c_str()) < 0)
    {
        stringstream ss;
        ss << "cannot rename " << tarabspath << " to " << new_tarabspath;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

bool Checker::rescan_data(std::function<void(const std::string&)> reporter, std::shared_ptr<core::Lock> lock, metadata_dest_func dest)
{
    auto reader = this->segment().reader(lock);
    return reader->scan_data(dest);
}

State Checker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend checker(tarabspath, segment().relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

void Checker::validate(Metadata& md, const scan::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob())
    {
        if (blob->filename != segment().relpath)
            throw std::runtime_error("metadata to validate does not appear to be from this segment");

        sys::File fd(tarabspath, O_RDONLY);
        v.validate_file(fd, blob->offset, blob->size);
        return;
    }
    const auto& data = md.get_data();
    auto buf = data.read();
    v.validate_buf(buf.data(), buf.size());  // TODO: add a validate_data that takes the metadata::Data
}

size_t Checker::remove()
{
    size_t size = sys::size(tarabspath);
    sys::unlink(tarabspath);
    return size;
}

core::Pending Checker::repack(const std::filesystem::path& rootdir, metadata::Collection& mds, const RepackConfig& cfg)
{
    auto tmpabspath = sys::with_suffix(segment().abspath, ".repack");

    core::Pending p(new files::RenameTransaction(tmpabspath, tarabspath));

    Creator creator(rootdir, segment().relpath, mds, tmpabspath);
    creator.validator = &scan::Validator::by_filename(segment().abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

void Checker::test_truncate(size_t offset)
{
    if (offset % 512 != 0)
        throw std::runtime_error("tar test_truncate only works at multiples of 512");

    utils::files::PreserveFileTimes pft(tarabspath);

    sys::File out(tarabspath, O_CREAT | O_TRUNC | O_WRONLY);
    out.ftruncate(offset);
    out.ftruncate(offset + 512);
    out.close();
}

void Checker::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    throw std::runtime_error("test_make_hole not implemented");
}

void Checker::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    throw std::runtime_error("test_make_overlap not implemented");
}

void Checker::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    utils::files::PreserveFileTimes pt(segment().abspath);
    sys::File fd(segment().abspath, O_RDWR);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}


}
}
}
#include "base.tcc"
