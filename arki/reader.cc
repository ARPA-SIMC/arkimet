#include "reader.h"
#include "arki/utils.h"
#include "arki/utils/accounting.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/gzip.h"
#include "arki/utils/lock.h"
#include "arki/types/source/blob.h"
#include "arki/nag.h"
#include "arki/iotrace.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace reader {

struct MissingFileReader : public Reader
{
protected:
    std::string abspath;

public:
    MissingFileReader(const std::string& abspath)
        : abspath(abspath) {}

    bool is(const std::string& fname) const override { return fname == abspath; }

    std::vector<uint8_t> read(const types::source::Blob& src) override
    {
        stringstream ss;
        ss << "cannot read " << src.size << " bytes of " << src.format << " data from " << abspath << ":"
           << src.offset << ": the file has disappeared";
        throw std::runtime_error(ss.str());
    }
};

struct FileReader : public Reader
{
public:
    sys::File fd;
    utils::Lock lock;

    FileReader(const std::string& fname)
        : fd(fname, O_RDONLY
#ifdef linux
                | O_CLOEXEC
#endif
            )
    {
        // Lock one byte at the beginning of the file, to prevent a repack but
        // allow appends: there are no functions in arkimet that would rewrite
        // a part of a file: either append, or replace.
        lock.l_type = F_RDLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = 0;
        lock.l_len = 1;
        lock.ofd_setlkw(fd);
    }

    ~FileReader()
    {
        // TODO: consider a non-throwing setlk implementation to avoid throwing
        // in destructors
        lock.l_type = F_UNLCK;
        lock.ofd_setlk(fd);
    }

    bool is(const std::string& fname) const override
    {
        return fd.name() == fname;
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
};

struct DirReader : public Reader
{
public:
    std::string fname;
    std::string format;
    sys::Path dirfd;

    DirReader(const std::string& fname)
        : dirfd(fname, O_DIRECTORY)
    {
        size_t pos;
        if ((pos = fname.rfind('.')) != std::string::npos)
            format = fname.substr(pos + 1);
    }

    bool is(const std::string& fname) const override
    {
        return dirfd.name() == fname;
    }

    std::vector<uint8_t> read(const types::source::Blob& src) override
    {
        vector<uint8_t> buf;
        buf.resize(src.size);

        char dataname[32];
        snprintf(dataname, 32, "%06zd.%s", (size_t)src.offset, format.c_str());
        sys::File file_fd(dirfd.openat(dataname, O_RDONLY | O_CLOEXEC), str::joinpath(dirfd.name(), dataname));

        if (posix_fadvise(file_fd, 0, src.size, POSIX_FADV_DONTNEED) != 0)
            nag::debug("fadvise on %s failed: %m", file_fd.name().c_str());

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
};

struct ZlibFileReader : public Reader
{
public:
    std::string fname;
    gzip::File fd;
    uint64_t last_ofs = 0;

    ZlibFileReader(const std::string& fname)
        : fname(fname), fd(fname + ".gz", "rb")
    {
    }

    bool is(const std::string& fname) const override
    {
        return this->fname == fname;
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

    IdxZlibFileReader(const std::string& fname)
        : fname(fname), fd(fname + ".gz", O_RDONLY), gzfd(fd.name())
    {
        // Read index
        idx.read(fd.name() + ".idx");
    }

    bool is(const std::string& fname) const override
    {
        return this->fname == fname;
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
};

std::shared_ptr<Reader> Registry::instantiate(const std::string& abspath)
{
    // Open the new file
    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            return make_shared<reader::DirReader>(abspath);
        else
            return make_shared<reader::FileReader>(abspath);
    }
    else if (sys::exists(abspath + ".gz.idx"))
        return make_shared<reader::IdxZlibFileReader>(abspath);
    else if (sys::exists(abspath + ".gz"))
        return make_shared<reader::ZlibFileReader>(abspath);
    else
        return make_shared<reader::MissingFileReader>(abspath);
}

std::shared_ptr<Reader> Registry::reader(const std::string& abspath)
{
    auto res = cache.find(abspath);
    if (res == cache.end())
    {
        auto reader = instantiate(abspath);
        cache.insert(make_pair(abspath, reader));
        return reader;
    }

    if (res->second.expired())
    {
        auto reader = instantiate(abspath);
        res->second = reader;
        return reader;
    }

    return res->second.lock();
}

void Registry::invalidate(const std::string& abspath)
{
    cache.erase(abspath);
}

void Registry::cleanup()
{
    auto i = cache.begin();
    while (i != cache.end())
    {
        if (i->second.expired())
        {
            auto next = i;
            ++next;
            cache.erase(i);
            i = next;
        } else
            ++i;
    }
}

Registry& Registry::get()
{
    static Registry singleton;
    return singleton;
}

}
}
