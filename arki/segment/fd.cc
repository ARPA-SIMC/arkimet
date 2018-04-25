#include "fd.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/validator.h"
#include "arki/scan.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include "arki/utils/accounting.h"
#include "arki/iotrace.h"
#include <algorithm>
#include <system_error>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <sys/uio.h>
#include <sys/sendfile.h>

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
        nag::warning("truncating %s to previous size %zd (rollback of append operation): %m", name().c_str(), pos);
}

Creator::Creator(const std::string& root, const std::string& relpath, metadata::Collection& mds, std::unique_ptr<File> out)
    : AppendCreator(root, relpath, mds), out(std::move(out))
{
}

void Creator::create()
{
    if (!out->is_open())
        out->open(O_WRONLY | O_CREAT | O_TRUNC, 0666);
    AppendCreator::create();
    out->fdatasync();
    out->close();
}

size_t Creator::append(const std::vector<uint8_t>& data)
{
    size_t wrpos = written;
    written += out->write_data(data);
    return wrpos;
}


CheckBackend::CheckBackend(const std::string& abspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
    : AppendCheckBackend(reporter, relpath, mds), data(abspath)
{
}

size_t CheckBackend::offset_end() const { return st.st_size; }

void CheckBackend::validate(Metadata& md, const types::source::Blob& source)
{
    validator->validate_file(data, source.offset, source.size);
}

State CheckBackend::check()
{
    if (!data.open_ifexists(O_RDONLY))
    {
        reporter(data.name() + " not found on disk");
        return SEGMENT_DELETED;
    }
    data.fstat(st);
    return AppendCheckBackend::check();
}


Reader::Reader(const std::string& abspath, std::shared_ptr<core::Lock> lock)
    : segment::Reader(lock), fd(abspath, O_RDONLY
#ifdef linux
                | O_CLOEXEC
#endif
            )
{
}

time_t Reader::timestamp()
{
    struct stat st;
    sys::stat(segment().abspath, st);
    return st.st_mtime;
}


bool Reader::scan_data(metadata_dest_func dest)
{
    auto scanner = scan::Scanner::get_scanner(segment().format);
    return scanner->scan_file(segment().abspath, static_pointer_cast<segment::Reader>(shared_from_this()), dest);
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


Writer::Writer(const std::string& format, const std::string& root, const std::string& relpath, std::unique_ptr<File> fd)
    : segment::Writer(format, root, relpath, fd->name()), fd(fd.release())
{
    initial_size = this->fd->lseek(0, SEEK_END);
    current_pos = initial_size;
}

Writer::~Writer()
{
    if (!fired) rollback_nothrow();
    delete fd;
}

bool Writer::single_file() const { return true; }

size_t Writer::next_offset() const { return current_pos; }

const types::source::Blob& Writer::append(Metadata& md)
{
    fired = false;
    const std::vector<uint8_t>& buf = md.getData();
    pending.emplace_back(md, source::Blob::create_unlocked(md.source().format, root, relpath, current_pos, buf.size()));
    current_pos += fd->write_data(buf);
    return *pending.back().new_source;
}

void Writer::commit()
{
    if (fired) return;
    fd->fsync();
    for (auto& p: pending)
        p.set_source();
    pending.clear();
    initial_size = current_pos;
    fired = true;
}

void Writer::rollback()
{
    if (fired) return;
    fd->ftruncate(initial_size);
    fd->lseek(initial_size, SEEK_SET);
    current_pos = initial_size;
    pending.clear();
    fired = true;
}

void Writer::rollback_nothrow() noexcept
{
    if (fired) return;
    fd->fdtruncate_nothrow(initial_size);
    ::lseek(*fd, initial_size, SEEK_SET);
    pending.clear();
    fired = true;
}


bool Checker::single_file() const { return true; }

bool Checker::exists_on_disk()
{
    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (!st) return false;
    if (S_ISDIR(st->st_mode)) return false;
    return true;
}

time_t Checker::timestamp()
{
    struct stat st;
    sys::stat(abspath, st);
    return st.st_mtime;
}

size_t Checker::size()
{
    struct stat st;
    sys::stat(abspath, st);
    return st.st_size;
}

void Checker::move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    sys::rename(abspath, new_abspath);
}

size_t Checker::remove()
{
    size_t size = sys::size(abspath);
    sys::unlink(abspath.c_str());
    return size;
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    string tmpabspath = abspath + ".repack";

    Pending p(new files::RenameTransaction(tmpabspath, abspath));

    Creator creator(rootdir, relpath, mds, open_file(tmpabspath, O_WRONLY | O_CREAT | O_TRUNC, 0666));
    creator.validator = &scan::Validator::by_filename(abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

void Checker::test_truncate(size_t offset)
{
    if (!sys::exists(abspath))
        sys::write_file(abspath, "");

    utils::files::PreserveFileTimes pft(abspath);
    if (::truncate(abspath.c_str(), offset) < 0)
    {
        stringstream ss;
        ss << "cannot truncate " << abspath << " at " << offset;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

void Checker::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    sys::File fd(abspath, O_RDWR);
    sys::PreserveFileTimes pt(fd);
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

void Checker::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    sys::File fd(abspath, O_RDWR);
    sys::PreserveFileTimes pt(fd);
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

void Checker::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    sys::File fd(abspath, O_RDWR);
    sys::PreserveFileTimes pt(fd);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}


bool can_store(const std::string& format)
{
    return format == "grib" || format == "grib1" || format == "grib2"
        || format == "bufr" || format == "vm2";
}

}
}
}
