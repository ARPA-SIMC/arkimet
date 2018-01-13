#include "reader.h"
#include "reader/registry.h"
#include "arki/core/file.h"
#include "arki/utils.h"
#include "arki/utils/accounting.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/gzip.h"
#include "arki/types/source/blob.h"
#include "arki/nag.h"
#include "arki/iotrace.h"
#include "arki/exceptions.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/sendfile.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki::utils;
using namespace arki::core;

namespace arki {
namespace reader {

struct MissingFileReader : public Reader
{
protected:
    std::string abspath;

public:
    MissingFileReader(const std::string& abspath)
        : abspath(abspath) {}

    std::vector<uint8_t> read(const types::source::Blob& src) override
    {
        stringstream ss;
        ss << "cannot read " << src.size << " bytes of " << src.format << " data from " << abspath << ":"
           << src.offset << ": the file has disappeared";
        throw std::runtime_error(ss.str());
    }

    size_t stream(const types::source::Blob& src, NamedFileDescriptor& out) override
    {
        stringstream ss;
        ss << "cannot stream " << src.size << " bytes of " << src.format << " data from " << abspath << ":"
           << src.offset << ": the file has disappeared";
        throw std::runtime_error(ss.str());
    }
};

struct FileReader : public Reader
{
public:
    sys::File fd;
    const core::lock::Policy* lock_policy;
    Lock lock;

    FileReader(const std::string& fname, const core::lock::Policy* lock_policy)
        : fd(fname, O_RDONLY
#ifdef linux
                | O_CLOEXEC
#endif
            ), lock_policy(lock_policy)
    {
        // Lock one byte at the beginning of the file, to prevent a repack but
        // allow appends: there are no functions in arkimet that would rewrite
        // a part of a file: either append, or replace.
        lock.l_type = F_RDLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = 0;
        lock.l_len = 1;
        lock_policy->setlkw(fd, lock);
    }

    ~FileReader()
    {
        // TODO: consider a non-throwing setlk implementation to avoid throwing
        // in destructors
        lock.l_type = F_UNLCK;
        lock_policy->setlk(fd, lock);
    }


    std::vector<uint8_t> read(const types::source::Blob& src) override
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

    size_t stream(const types::source::Blob& src, NamedFileDescriptor& out) override
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
};

struct DirReader : public Reader
{
public:
    std::string fname;
    std::string format;
    sys::Path dirfd;
    sys::File repack_lock;
    const core::lock::Policy* lock_policy;
    Lock lock;

    DirReader(const std::string& fname, const core::lock::Policy* lock_policy)
        : dirfd(fname, O_DIRECTORY), repack_lock(dirfd.openat(".repack-lock", O_RDONLY | O_CREAT, 0777), str::joinpath(fname, ".repack-lock")),
          lock_policy(lock_policy)
    {
        size_t pos;
        if ((pos = fname.rfind('.')) != std::string::npos)
            format = fname.substr(pos + 1);

        // Lock the directory to prevent a repack but allow appends: there are
        // no functions in arkimet that would rewrite a part of a file: either
        // append, or replace.
        lock.l_type = F_RDLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = 0;
        lock.l_len = 0;
        lock_policy->setlkw(repack_lock, lock);
    }

    ~DirReader()
    {
        lock.l_type = F_UNLCK;
        lock_policy->setlk(repack_lock, lock);
    }

    sys::File open_src(const types::source::Blob& src)
    {
        char dataname[32];
        snprintf(dataname, 32, "%06zd.%s", (size_t)src.offset, format.c_str());
        sys::File file_fd(dirfd.openat(dataname, O_RDONLY | O_CLOEXEC), str::joinpath(dirfd.name(), dataname));

        if (posix_fadvise(file_fd, 0, src.size, POSIX_FADV_DONTNEED) != 0)
            nag::debug("fadvise on %s failed: %m", file_fd.name().c_str());

        return file_fd;
    }

    std::vector<uint8_t> read(const types::source::Blob& src) override
    {
        vector<uint8_t> buf;
        buf.resize(src.size);

        sys::File file_fd = open_src(src);

        ssize_t res = file_fd.pread(buf.data(), src.size, 0);
        if ((size_t)res != src.size)
        {
            stringstream msg;
            msg << "cannot read " << src.size << " bytes of " << src.format << " data from " << fname << ":"
                << src.offset << ": only " << res << "/" << src.size << " bytes have been read";
            throw std::runtime_error(msg.str());
        }

        acct::plain_data_read_count.incr();
        iotrace::trace_file(dirfd.name(), src.offset, src.size, "read data");

        return buf;
    }

    size_t stream(const types::source::Blob& src, NamedFileDescriptor& out) override
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
            sys::File file_fd = open_src(src);

            // TODO: add a stream method to sys::FileDescriptor that does the
            // right thing depending on what's available in the system, and
            // potentially also handles retries
            off_t offset = 0;
            ssize_t res = sendfile(out, file_fd, &offset, src.size);
            if (res < 0)
            {
                stringstream msg;
                msg << "cannot stream " << src.size << " bytes of " << src.format << " data from " << file_fd.name() << ":"
                    << src.offset;
                throw_system_error(msg.str());
            } else if ((size_t)res != src.size) {
                // TODO: retry instead
                stringstream msg;
                msg << "cannot read " << src.size << " bytes of " << src.format << " data from " << file_fd.name() << ":"
                    << src.offset << ": only " << res << "/" << src.size << " bytes have been read";
                throw std::runtime_error(msg.str());
            }

            acct::plain_data_read_count.incr();
            iotrace::trace_file(dirfd.name(), src.offset, src.size, "streamed data");
            return res;
        }
    }
};

struct ZlibFileReader : public Reader
{
public:
    std::string fname;
    gzip::File fd;
    uint64_t last_ofs = 0;

    ZlibFileReader(const std::string& fname, const core::lock::Policy* lock_policy)
        : fname(fname), fd(fname + ".gz", "rb")
    {
    }

    std::vector<uint8_t> read(const types::source::Blob& src) override
    {
        vector<uint8_t> buf;
        buf.resize(src.size);

        if (src.offset != last_ofs)
        {
            fd.seek(src.offset, SEEK_SET);

            if (src.offset >= last_ofs)
                acct::gzip_forward_seek_bytes.incr(src.offset - last_ofs);
            else
                acct::gzip_forward_seek_bytes.incr(src.offset);
        }

        fd.read_all_or_throw(buf.data(), src.size);
        last_ofs = src.offset + src.size;

        acct::gzip_data_read_count.incr();
        iotrace::trace_file(fname, src.offset, src.size, "read data");

        return buf;
    }

    size_t stream(const types::source::Blob& src, NamedFileDescriptor& out) override
    {
        vector<uint8_t> buf = read(src);
        if (src.format == "vm2")
        {
            struct iovec todo[2] = {
                { (void*)buf.data(), buf.size() },
                { (void*)"\n", 1 },
            };
            ssize_t res = ::writev(out, todo, 2);
            if (res < 0 || (unsigned)res != buf.size() + 1)
                throw_system_error("cannot write " + to_string(buf.size() + 1) + " bytes to " + out.name());
            return buf.size() + 1;
        } else {
            out.write_all_or_throw(buf);
            return buf.size();
        }
    }
};

struct IdxZlibFileReader : public Reader
{
public:
    std::string fname;
    compress::SeekIndex idx;
    size_t last_block = 0;
    sys::File fd;
    gzip::File gzfd;
    uint64_t last_ofs = 0;

    IdxZlibFileReader(const std::string& fname, const core::lock::Policy* lock_policy)
        : fname(fname), fd(fname + ".gz", O_RDONLY), gzfd(fd.name())
    {
        // Read index
        idx.read(fd.name() + ".idx");
    }

    void reposition(off_t ofs)
    {
        size_t block = idx.lookup(ofs);
        if (block != last_block || gzfd == NULL)
        {
            off_t res = fd.lseek(idx.ofs_comp[block], SEEK_SET);
            if ((size_t)res != idx.ofs_comp[block])
            {
                stringstream ss;
                ss << fd.name() << ": seeking to offset " << idx.ofs_comp[block] << " reached offset " << res << " instead";
                throw std::runtime_error(ss.str());
            }

            // (Re)open the compressed file
            int fd1 = fd.dup();
            gzfd.fdopen(fd1, "rb");
            last_block = block;
            acct::gzip_idx_reposition_count.incr();
        }

        // Seek inside the compressed chunk
        gzfd.seek(ofs - idx.ofs_unc[block], SEEK_SET);
        acct::gzip_forward_seek_bytes.incr(ofs - idx.ofs_unc[block]);
    }

    std::vector<uint8_t> read(const types::source::Blob& src) override
    {
        vector<uint8_t> buf;
        buf.resize(src.size);

        if (gzfd == NULL || src.offset != last_ofs)
        {
            if (gzfd != NULL && src.offset > last_ofs && src.offset < last_ofs + 4096)
            {
                // Just skip forward
                gzfd.seek(src.offset - last_ofs, SEEK_CUR);
                acct::gzip_forward_seek_bytes.incr(src.offset - last_ofs);
            } else {
                // We need to seek
                reposition(src.offset);
            }
        }

        gzfd.read_all_or_throw(buf.data(), src.size);
        last_ofs = src.offset + src.size;
        acct::gzip_data_read_count.incr();
        iotrace::trace_file(fname, src.offset, src.size, "read data");

        return buf;
    }

    size_t stream(const types::source::Blob& src, NamedFileDescriptor& out) override
    {
        vector<uint8_t> buf = read(src);
        if (src.format == "vm2")
        {
            struct iovec todo[2] = {
                { (void*)buf.data(), buf.size() },
                { (void*)"\n", 1 },
            };
            ssize_t res = ::writev(out, todo, 2);
            if (res < 0 || (unsigned)res != buf.size() + 1)
                throw_system_error("cannot write " + to_string(buf.size() + 1) + " bytes to " + out.name());
            return buf.size() + 1;
        } else {
            out.write_all_or_throw(buf);
            return buf.size();
        }
    }
};

}


namespace {

reader::Registry<reader::FileReader> registry_file;
reader::Registry<reader::DirReader> registry_dir;
reader::Registry<reader::ZlibFileReader> registry_zlib;
reader::Registry<reader::IdxZlibFileReader> registry_idxzlib;

}


void Reader::reset()
{
    registry_file.clear();
    registry_dir.clear();
    registry_zlib.clear();
    registry_idxzlib.clear();
}

std::shared_ptr<Reader> Reader::for_missing(const std::string& abspath)
{
    return make_shared<reader::MissingFileReader>(abspath);
}

std::shared_ptr<Reader> Reader::for_file(const std::string& abspath)
{
    return registry_file.reader(abspath);
}

std::shared_ptr<Reader> Reader::for_dir(const std::string& abspath)
{
    return registry_dir.reader(abspath);
}

std::shared_ptr<Reader> Reader::for_auto(const std::string& abspath)
{
    // Open the new file
    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            return for_dir(abspath);
        else
            return for_file(abspath);
    }
    else if (sys::exists(abspath + ".gz.idx"))
        return registry_idxzlib.reader(abspath);
    else if (sys::exists(abspath + ".gz"))
        return registry_zlib.reader(abspath);
    else
        return for_missing(abspath);
}

std::shared_ptr<Reader> Reader::create_new(const std::string& abspath, const core::lock::Policy* lock_policy)
{
    // Open the new file
    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            return std::make_shared<reader::DirReader>(abspath, lock_policy);
        else
            return std::make_shared<reader::FileReader>(abspath, lock_policy);
    }
    else if (sys::exists(abspath + ".gz.idx"))
        return std::make_shared<reader::IdxZlibFileReader>(abspath, lock_policy);
    else if (sys::exists(abspath + ".gz"))
        return std::make_shared<reader::ZlibFileReader>(abspath, lock_policy);
    else
        return make_shared<reader::MissingFileReader>(abspath);
}

unsigned Reader::test_count_cached()
{
    registry_file.cleanup();
    registry_dir.cleanup();
    registry_zlib.cleanup();
    registry_idxzlib.cleanup();
    unsigned size = 0;
    size += registry_file.test_inspect_cache().size();
    size += registry_dir.test_inspect_cache().size();
    size += registry_zlib.test_inspect_cache().size();
    size += registry_idxzlib.test_inspect_cache().size();
    return size;
}

}
