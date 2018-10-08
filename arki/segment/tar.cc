#include "tar.h"
#include "common.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/validator.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/tar.h"
#include "arki/nag.h"
#include "arki/utils/accounting.h"
#include "arki/iotrace.h"
#include <fcntl.h>
#include <vector>
#include <algorithm>
#include <sys/uio.h>
#include <sys/sendfile.h>
#include <system_error>


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

    Creator(const std::string& root, const std::string& relpath, metadata::Collection& mds, const std::string& dest_abspath)
        : AppendCreator(root, relpath, mds), out(dest_abspath), tarout(out)
    {
        if (!mds.empty())
            format = mds[0].source().format;
    }

    size_t append(const metadata::Data& data) override
    {
        // Append it to the new file
        snprintf(fname, 99, "%06zd.%s", idx, format.c_str());
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

    CheckBackend(const std::string& tarabspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
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
            reporter(data.name() + " not found on disk");
            return SEGMENT_DELETED;
        }
        data.fstat(st);
        return AppendCheckBackend::check();
    }
};

}

const char* Segment::type() const { return "tar"; }
bool Segment::single_file() const { return true; }
time_t Segment::timestamp() const { return sys::timestamp(abspath + ".tar"); }
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
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, const RepackConfig& cfg)
{
    Creator creator(rootdir, relpath, mds, abspath + ".tar");
    creator.create();
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}



Reader::Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
    : segment::BaseReader<Segment>(format, root, relpath, abspath, lock), fd(abspath + ".tar", O_RDONLY
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
        nag::debug("fadvise on %s failed: %m", fd.name().c_str());
    ssize_t res = fd.pread(buf.data(), src.size, src.offset);
    if ((size_t)res != src.size)
    {
        stringstream msg;
        msg << "cannot read " << src.size << " bytes of " << src.format << " data from " << fd.name() << ":"
            << src.offset << ": only " << res << "/" << src.size << " bytes have been read";
        throw std::runtime_error(msg.str());
    }
    acct::plain_data_read_count.incr();
    iotrace::trace_file(fd.name(), src.offset, src.size, "read data");

    return buf;
}

size_t Reader::stream(const types::source::Blob& src, core::NamedFileDescriptor& out)
{
    if (src.format == "vm2")
    {
        vector<uint8_t> buf = read(src);

        struct iovec todo[2] = {
            { (void*)buf.data(), buf.size() },
            { (void*)"\n", 1 },
        };
        ssize_t res = ::writev(out, todo, 2);
        if (res < 0 || (unsigned)res != buf.size() + 1)
            throw_system_error("cannot write " + to_string(buf.size() + 1) + " bytes to " + out.name());
        return buf.size() + 1;
    } else {
        // TODO: add a stream method to sys::FileDescriptor that does the
        // right thing depending on what's available in the system, and
        // potentially also handles retries. Retry can trivially be done
        // because offset is updated, and size can just be decreased by the
        // return value
        off_t offset = src.offset;
        ssize_t res = sendfile(out, fd, &offset, src.size);
        if (res < 0)
        {
            stringstream msg;
            msg << "cannot stream " << src.size << " bytes of " << src.format << " data from " << fd.name() << ":"
                << src.offset;
            throw_system_error(msg.str());
        } else if ((size_t)res != src.size) {
            // TODO: retry instead
            stringstream msg;
            msg << "cannot read " << src.size << " bytes of " << src.format << " data from " << fd.name() << ":"
                << src.offset << ": only " << res << "/" << src.size << " bytes have been read";
            throw std::runtime_error(msg.str());
        }

        acct::plain_data_read_count.incr();
        iotrace::trace_file(fd.name(), src.offset, src.size, "streamed data");
        return res;
    }
}


Checker::Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath)
    : segment::BaseChecker<Segment>(format, root, relpath, abspath), tarabspath(abspath + ".tar")
{
}

bool Checker::exists_on_disk()
{
    return sys::exists(tarabspath);
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

void Checker::move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    string new_tarabspath = new_abspath + ".tar";
    if (rename(tarabspath.c_str(), new_tarabspath.c_str()) < 0)
    {
        stringstream ss;
        ss << "cannot rename " << tarabspath << " to " << new_tarabspath;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
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

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, const RepackConfig& cfg)
{
    string tmpabspath = segment().abspath + ".repack";

    Pending p(new files::RenameTransaction(tmpabspath, tarabspath));

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
    if (offset > 0)
        throw std::runtime_error("tar test_truncate not implemented for offset > 0");

    utils::files::PreserveFileTimes pft(tarabspath);

    sys::File out(tarabspath, O_CREAT | O_TRUNC | O_WRONLY);
    out.ftruncate(0);
    out.ftruncate(1024);
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
    sys::File fd(segment().abspath, O_RDWR);
    sys::PreserveFileTimes pt(fd);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}


}
}
}
#include "base.tcc"
