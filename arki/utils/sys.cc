#include "sys.h"
#include "string.h"
#include <cstddef>
#include <cstring>
#include <exception>
#include <sstream>
#include <system_error>
#include <cerrno>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/sendfile.h>
#include <utime.h>
#include <alloca.h>

namespace {

inline const char* to_cstring(const std::string& s)
{
    return s.c_str();
}

inline const char* to_cstring(const char* s)
{
    return s;
}

}

namespace arki {
namespace utils {
namespace sys {

std::unique_ptr<struct stat> stat(const std::string& pathname)
{
    std::unique_ptr<struct stat> res(new struct stat);
    if (::stat(pathname.c_str(), res.get()) == -1)
    {
        if (errno == ENOENT)
            return std::unique_ptr<struct stat>();
        else
            throw std::system_error(errno, std::system_category(), "cannot stat " + pathname);
    }
    return res;
}

void stat(const std::string& pathname, struct stat& st)
{
    if (::stat(pathname.c_str(), &st) == -1)
        throw std::system_error(errno, std::system_category(), "cannot stat " + pathname);
}

#define common_stat_body(testfunc) \
    struct stat st; \
    if (::stat(pathname.c_str(), &st) == -1) { \
        if (errno == ENOENT) \
            return false; \
        else \
            throw std::system_error(errno, std::system_category(), "cannot stat " + pathname); \
    } \
    return testfunc(st.st_mode)

bool isdir(const std::string& pathname)
{
    common_stat_body(S_ISDIR);
}

bool isblk(const std::string& pathname)
{
    common_stat_body(S_ISBLK);
}

bool ischr(const std::string& pathname)
{
    common_stat_body(S_ISCHR);
}

bool isfifo(const std::string& pathname)
{
    common_stat_body(S_ISFIFO);
}

bool islnk(const std::string& pathname)
{
    common_stat_body(S_ISLNK);
}

bool isreg(const std::string& pathname)
{
    common_stat_body(S_ISREG);
}

bool issock(const std::string& pathname)
{
    common_stat_body(S_ISSOCK);
}

#undef common_stat_body

time_t timestamp(const std::string& file)
{
    struct stat st;
    stat(file, st);
    return st.st_mtime;
}

time_t timestamp(const std::string& file, time_t def)
{
    auto st = sys::stat(file);
    return st.get() ? st->st_mtime : def;
}

size_t size(const std::string& file)
{
    struct stat st;
    stat(file, st);
    return (size_t)st.st_size;
}

size_t size(const std::string& file, size_t def)
{
    auto st = sys::stat(file);
    return st.get() ? (size_t)st->st_size : def;
}

ino_t inode(const std::string& file)
{
    struct stat st;
    stat(file, st);
    return st.st_ino;
}

ino_t inode(const std::string& file, ino_t def)
{
    auto st = sys::stat(file);
    return st.get() ? st->st_ino : def;
}


bool access(const std::string &s, int m)
{
    return ::access(s.c_str(), m) == 0;
}

bool exists(const std::string& file)
{
    return sys::access(file, F_OK);
}

std::string getcwd()
{
#if defined(__GLIBC__)
    char* cwd = ::get_current_dir_name();
    if (cwd == NULL)
        throw std::system_error(errno, std::system_category(), "cannot get the current working directory");
    const std::string str(cwd);
    ::free(cwd);
    return str;
#else
    size_t size = pathconf(".", _PC_PATH_MAX);
    char *buf = (char *)alloca( size );
    if (::getcwd(buf, size) == NULL)
        throw std::system_error(errno, std::system_category(), "cannot get the current working directory");
    return buf;
#endif
}

void chdir(const std::string& dir)
{
    if (::chdir(dir.c_str()) == -1)
        throw std::system_error(errno, std::system_category(), "cannot change the current working directory to " + dir);
}

void chroot(const std::string& dir)
{
    if (::chroot(dir.c_str()) == -1)
        throw std::system_error(errno, std::system_category(), "cannot chroot to " + dir);
}

mode_t umask(mode_t mask)
{
    return ::umask(mask);
}

std::string abspath(const std::string& pathname)
{
    if (pathname[0] == '/')
        return str::normpath(pathname);
    else
        return str::normpath(str::joinpath(sys::getcwd(), pathname));
}


/*
 * MMap
 */

MMap::MMap(void* addr, size_t length)
    : addr(addr), length(length)
{
}

MMap::MMap(MMap&& o)
    : addr(o.addr), length(o.length)
{
    o.addr = MAP_FAILED;
    o.length = 0;
}

MMap& MMap::operator=(MMap&& o)
{
    if (this == &o) return *this;

    munmap();
    addr = o.addr;
    length = o.length;
    o.addr = MAP_FAILED;
    o.length = 0;
    return *this;
}

MMap::~MMap()
{
    if (addr != MAP_FAILED) ::munmap(addr, length);
}

void MMap::munmap()
{
    if (::munmap(addr, length) == -1)
        throw std::system_error(errno, std::system_category(), "cannot unmap memory");
    addr = MAP_FAILED;
}


/*
 * FileDescriptor
 */

FileDescriptor::FileDescriptor() {}
FileDescriptor::FileDescriptor(FileDescriptor&& o)
    : fd(o.fd)
{
    o.fd = -1;
}
FileDescriptor::FileDescriptor(int fd) : fd(fd) {}
FileDescriptor::~FileDescriptor() {}

void FileDescriptor::throw_error(const char* desc)
{
    throw std::system_error(errno, std::system_category(), desc);
}

void FileDescriptor::throw_runtime_error(const char* desc)
{
    throw std::runtime_error(desc);
}

bool FileDescriptor::is_open() const { return fd != -1; }

void FileDescriptor::close()
{
    if (fd == -1) return;
    if (::close(fd) == -1)
        throw_error("cannot close");
    fd = -1;
}

void FileDescriptor::fstat(struct stat& st)
{
    if (::fstat(fd, &st) == -1)
        throw_error("cannot stat");
}

void FileDescriptor::fchmod(mode_t mode)
{
    if (::fchmod(fd, mode) == -1)
        throw_error("cannot fchmod");
}

int FileDescriptor::dup()
{
    int res = ::dup(fd);
    if (res == -1)
        throw_error("cannot dup");
    return res;
}

size_t FileDescriptor::read(void* buf, size_t count)
{
    ssize_t res = ::read(fd, buf, count);
    if (res == -1)
        throw_error("cannot read");
    return res;
}

bool FileDescriptor::read_all_or_retry(void* buf, size_t count)
{
    char* dest = (char*)buf;
    size_t remaining = count;
    while (remaining > 0)
    {
        size_t res = read(dest, remaining);
        if (res == 0)
        {
            if (remaining == count)
                return false;

            throw_runtime_error("partial read before EOF");
        }
        dest += res;
        remaining -= res;
    }
    return true;
}

void FileDescriptor::read_all_or_throw(void* buf, size_t count)
{
    size_t res = read(buf, count);
    if (res != count)
        throw_runtime_error("partial read");
}

size_t FileDescriptor::write(const void* buf, size_t count)
{
    ssize_t res = ::write(fd, buf, count);
    if (res == -1)
        throw_error("cannot write");
    return res;
}

size_t FileDescriptor::pread(void* buf, size_t count, off_t offset)
{
    ssize_t res = ::pread(fd, buf, count, offset);
    if (res == -1)
        throw_error("cannot pread");
    return res;
}

size_t FileDescriptor::pwrite(const void* buf, size_t count, off_t offset)
{
    ssize_t res = ::pwrite(fd, buf, count, offset);
    if (res == -1)
        throw_error("cannot pwrite");
    return res;
}

off_t FileDescriptor::lseek(off_t offset, int whence)
{
    off_t res = ::lseek(fd, offset, whence);
    if (res == (off_t)-1)
        throw_error("cannot seek");
    return res;
}

void FileDescriptor::write_all_or_retry(const void* buf, size_t count)
{
    size_t written = 0;
    while (written < count)
        written += write((unsigned char*)buf + written, count - written);
}

void FileDescriptor::write_all_or_throw(const void* buf, size_t count)
{
    size_t written = write((unsigned char*)buf, count);
    if (written < count)
        throw_runtime_error("partial write");
}

void FileDescriptor::ftruncate(off_t length)
{
    if (::ftruncate(fd, length) == -1)
        throw_error("cannot ftruncate");
}

MMap FileDescriptor::mmap(size_t length, int prot, int flags, off_t offset)
{
    void* res =::mmap(0, length, prot, flags, fd, offset);
    if (res == MAP_FAILED)
        throw_error("cannot mmap");
    return MMap(res, length);
}

bool FileDescriptor::ofd_setlk(struct flock& lk)
{
#ifdef F_OFD_SETLK
    if (fcntl(fd, F_OFD_SETLK, &lk) != -1)
#else
    if (fcntl(fd, F_SETLK, &lk) != -1)
#endif
        return true;
    if (errno != EAGAIN && errno != EACCES)
        throw_error("cannot acquire lock");
    return false;
}

bool FileDescriptor::ofd_setlkw(struct flock& lk, bool retry_on_signal)
{
    while (true)
    {
#ifdef F_OFD_SETLK
        if (fcntl(fd, F_OFD_SETLKW, &lk) != -1)
#else
        if (fcntl(fd, F_SETLKW, &lk) != -1)
#endif
            return true;
        if (errno != EINTR)
            throw_error("cannot acquire lock");
        if (!retry_on_signal)
            return false;
    }
}

bool FileDescriptor::ofd_getlk(struct flock& lk)
{
#ifdef F_OFD_SETLK
    if (fcntl(fd, F_OFD_GETLK, &lk) == -1)
#else
    if (fcntl(fd, F_GETLK, &lk) == -1)
#endif
        throw_error("cannot test lock");
    return lk.l_type == F_UNLCK;
}

int FileDescriptor::getfl()
{
    int res = fcntl(fd, F_GETFL, 0);
    if (res == -1)
        throw_error("cannot get file flags (fcntl F_GETFL)");
    return res;
}

void FileDescriptor::setfl(int flags)
{
    if (fcntl(fd, F_SETFL, flags) == -1)
        throw_error("cannot set file flags (fcntl F_SETFL)");
}


namespace {

struct TransferBuffer
{
    char* buf = nullptr;

    ~TransferBuffer()
    {
        delete[] buf;
    }

    void allocate()
    {
        if (buf)
            return;
        buf = new char[40960];
    }

    operator char*() { return buf; }
};

}

void FileDescriptor::sendfile(FileDescriptor& out_fd, off_t offset, size_t count)
{
    bool has_sendfile = true;
    TransferBuffer buffer;
    while (count > 0)
    {
        if (has_sendfile)
        {
            ssize_t res = ::sendfile(out_fd, fd, &offset, count);
            if (res < 0)
            {
                if (errno == EINVAL || errno == ENOSYS)
                {
                    has_sendfile = false;
                    buffer.allocate();
                }
                else
                {
                    std::stringstream msg;
                    msg << "cannot sendfile() " << count << " bytes from offset" << offset;
                    throw_error(msg.str().c_str());
                }
            } else {
                offset += res;
                count -= res;
            }
        } else {
            size_t res = pread(buffer, count, offset);
            out_fd.write_all_or_retry(buffer, res);
            offset += res;
            count -= res;
        }
    }
}

void FileDescriptor::futimens(const struct ::timespec ts[2])
{
    if (::futimens(fd, ts) == -1)
        throw_error("cannot change file timestamps");
}

void FileDescriptor::fsync()
{
    if (::fsync(fd) == -1)
        throw_error("fsync failed");
}

void FileDescriptor::fdatasync()
{
    if (::fdatasync(fd) == -1)
        throw_error("fdatasync failed");
}


/*
 * PreserveFileTimes
 */

PreserveFileTimes::PreserveFileTimes(FileDescriptor fd)
    : fd(fd)
{
    struct stat st;
    fd.fstat(st);
    ts[0] = st.st_atim;
    ts[1] = st.st_mtim;
}

PreserveFileTimes::~PreserveFileTimes()
{
    fd.futimens(ts);
}


/*
 * NamedFileDescriptor
 */

NamedFileDescriptor::NamedFileDescriptor(int fd, const std::string& pathname)
    : FileDescriptor(fd), pathname(pathname)
{
}

NamedFileDescriptor::NamedFileDescriptor(NamedFileDescriptor&& o)
    : FileDescriptor(std::move(o)), pathname(std::move(o.pathname))
{
}

NamedFileDescriptor& NamedFileDescriptor::operator=(NamedFileDescriptor&& o)
{
    if (this == &o) return *this;
    fd = o.fd;
    pathname = std::move(o.pathname);
    o.fd = -1;
    return *this;
}

void NamedFileDescriptor::throw_error(const char* desc)
{
    throw std::system_error(errno, std::system_category(), pathname + ": " + desc);
}

void NamedFileDescriptor::throw_runtime_error(const char* desc)
{
    throw std::runtime_error(pathname + ": " + desc);
}


/*
 * ManagedNamedFileDescriptor
 */

ManagedNamedFileDescriptor::~ManagedNamedFileDescriptor()
{
    if (fd != -1) ::close(fd);
}

ManagedNamedFileDescriptor& ManagedNamedFileDescriptor::operator=(ManagedNamedFileDescriptor&& o)
{
    if (&o == this) return *this;
    close();
    fd = o.fd;
    pathname = std::move(o.pathname);
    o.fd = -1;
    return *this;
}


/*
 * Path
 */

Path::Path(const char* pathname, int flags, mode_t mode)
    : ManagedNamedFileDescriptor(-1, pathname)
{
    open(flags, mode);
}

Path::Path(const std::string& pathname, int flags, mode_t mode)
    : ManagedNamedFileDescriptor(-1, pathname)
{
    open(flags, mode);
}

Path::Path(Path& parent, const char* pathname, int flags, mode_t mode)
    : ManagedNamedFileDescriptor(parent.openat(pathname, flags | O_PATH, mode),
            str::joinpath(parent.name(), pathname))
{
}

void Path::open(int flags, mode_t mode)
{
    close();
    fd = ::open(pathname.c_str(), flags | O_PATH, mode);
    if (fd == -1)
        throw_error("cannot open path");
}

DIR* Path::fdopendir()
{
    int fd1 = ::openat(fd, ".", O_DIRECTORY);
    if (fd1 == -1)
        throw_error("cannot open directory");

    DIR* res = ::fdopendir(fd1);
    if (!res)
        throw_error("cannot fdopendir");

    return res;
}

Path::iterator Path::begin()
{
    if (fd == -1)
       return iterator();
    else
        return iterator(*this);
}

Path::iterator Path::end()
{
    return iterator();
}

int Path::openat(const char* pathname, int flags, mode_t mode)
{
    int res = ::openat(fd, pathname, flags, mode);
    if (res == -1)
        throw_error("cannot openat");
    return res;
}

int Path::openat_ifexists(const char* pathname, int flags, mode_t mode)
{
    int res = ::openat(fd, pathname, flags, mode);
    if (res == -1)
    {
        if (errno == ENOENT)
            return -1;
        throw_error("cannot openat");
    }
    return res;
}

bool Path::faccessat(const char* pathname, int mode, int flags)
{
    return ::faccessat(fd, pathname, mode, flags) == 0;
}

void Path::fstatat(const char* pathname, struct stat& st)
{
    if (::fstatat(fd, pathname, &st, 0) == -1)
        throw_error("cannot fstatat");
}

bool Path::fstatat_ifexists(const char* pathname, struct stat& st)
{
    if (::fstatat(fd, pathname, &st, 0) == -1)
    {
        if (errno == ENOENT)
            return false;
        throw_error("cannot fstatat");
    }
    return true;
}

void Path::lstatat(const char* pathname, struct stat& st)
{
    if (::fstatat(fd, pathname, &st, AT_SYMLINK_NOFOLLOW) == -1)
        throw_error("cannot fstatat");
}

bool Path::lstatat_ifexists(const char* pathname, struct stat& st)
{
    if (::fstatat(fd, pathname, &st, AT_SYMLINK_NOFOLLOW) == -1)
    {
        if (errno == ENOENT)
            return false;
        throw_error("cannot fstatat");
    }
    return true;
}

void Path::unlinkat(const char* pathname)
{
    if (::unlinkat(fd, pathname, 0) == -1)
        throw_error("cannot unlinkat");
}

void Path::mkdirat(const char* pathname, mode_t mode)
{
    if (::mkdirat(fd, pathname, mode) == -1)
        throw_error("cannot mkdirat");
}

void Path::rmdirat(const char* pathname)
{
    if (::unlinkat(fd, pathname, AT_REMOVEDIR) == -1)
        throw_error("cannot unlinkat");
}

void Path::symlinkat(const char* target, const char* linkpath)
{
    if (::symlinkat(target, fd, linkpath) == -1)
        throw_error("cannot symlinkat");
}

std::string Path::readlinkat(const char* pathname)
{
    std::string res(256, 0);
    while (true)
    {
        // TODO: remove the cast to char* after C++14
        ssize_t sz = ::readlinkat(fd, pathname, (char*)res.data(), res.size());
        if (sz == -1)
            throw_error("cannot readlinkat");
        if (sz < (ssize_t)res.size())
        {
            res.resize(sz);
            return res;
        }
        res.resize(res.size() * 2);
    }
}


Path::iterator::iterator()
{
}

Path::iterator::iterator(Path& dir)
    : path(&dir)
{
    this->dir = dir.fdopendir();
    operator++();
}

Path::iterator::~iterator()
{
    if (dir) closedir(dir);
}

bool Path::iterator::operator==(const iterator& i) const
{
    if (!dir && !i.dir) return true;
    if (!dir || !i.dir) return false;
    return cur_entry->d_ino == i.cur_entry->d_ino;
}
bool Path::iterator::operator!=(const iterator& i) const
{
    if (!dir && !i.dir) return false;
    if (!dir || !i.dir) return true;
    return cur_entry->d_ino != i.cur_entry->d_ino;
}

void Path::iterator::operator++()
{
    errno = 0;
    cur_entry = readdir(dir);
    if (cur_entry == nullptr)
    {
        if (errno) path->throw_error("cannot readdir");

        // Turn into an end iterator
        free(cur_entry);
        cur_entry = nullptr;
        closedir(dir);
        dir = nullptr;
    }
}

bool Path::iterator::isdir() const
{
#if defined(_DIRENT_HAVE_D_TYPE) || defined(HAVE_STRUCT_DIRENT_D_TYPE)
    if (cur_entry->d_type == DT_DIR)
        return true;
    if (cur_entry->d_type != DT_UNKNOWN)
        return false;
#endif
    // No d_type, we'll need to stat
    struct stat st;
    path->fstatat(cur_entry->d_name, st);
    return S_ISDIR(st.st_mode);
}

bool Path::iterator::isblk() const
{
#if defined(_DIRENT_HAVE_D_TYPE) || defined(HAVE_STRUCT_DIRENT_D_TYPE)
    if (cur_entry->d_type == DT_BLK)
        return true;
    if (cur_entry->d_type != DT_UNKNOWN)
        return false;
#endif
    // No d_type, we'll need to stat
    struct stat st;
    path->fstatat(cur_entry->d_name, st);
    return S_ISBLK(st.st_mode);
}

bool Path::iterator::ischr() const
{
#if defined(_DIRENT_HAVE_D_TYPE) || defined(HAVE_STRUCT_DIRENT_D_TYPE)
    if (cur_entry->d_type == DT_CHR)
        return true;
    if (cur_entry->d_type != DT_UNKNOWN)
        return false;
#endif
    // No d_type, we'll need to stat
    struct stat st;
    path->fstatat(cur_entry->d_name, st);
    return S_ISCHR(st.st_mode);
}

bool Path::iterator::isfifo() const
{
#if defined(_DIRENT_HAVE_D_TYPE) || defined(HAVE_STRUCT_DIRENT_D_TYPE)
    if (cur_entry->d_type == DT_FIFO)
        return true;
    if (cur_entry->d_type != DT_UNKNOWN)
        return false;
#endif
    // No d_type, we'll need to stat
    struct stat st;
    path->fstatat(cur_entry->d_name, st);
    return S_ISFIFO(st.st_mode);
}

bool Path::iterator::islnk() const
{
#if defined(_DIRENT_HAVE_D_TYPE) || defined(HAVE_STRUCT_DIRENT_D_TYPE)
    if (cur_entry->d_type == DT_LNK)
        return true;
    if (cur_entry->d_type != DT_UNKNOWN)
        return false;
#endif
    struct stat st;
    path->fstatat(cur_entry->d_name, st);
    return S_ISLNK(st.st_mode);
}

bool Path::iterator::isreg() const
{
#if defined(_DIRENT_HAVE_D_TYPE) || defined(HAVE_STRUCT_DIRENT_D_TYPE)
    if (cur_entry->d_type == DT_REG)
        return true;
    if (cur_entry->d_type != DT_UNKNOWN)
        return false;
#endif
    struct stat st;
    path->fstatat(cur_entry->d_name, st);
    return S_ISREG(st.st_mode);
}

bool Path::iterator::issock() const
{
#if defined(_DIRENT_HAVE_D_TYPE) || defined(HAVE_STRUCT_DIRENT_D_TYPE)
    if (cur_entry->d_type == DT_SOCK)
        return true;
    if (cur_entry->d_type != DT_UNKNOWN)
        return false;
#endif
    struct stat st;
    path->fstatat(cur_entry->d_name, st);
    return S_ISSOCK(st.st_mode);
}

Path Path::iterator::open_path(int flags) const
{
    return Path(*path, cur_entry->d_name);
}


void Path::rmtree()
{
    for (auto i = begin(); i != end(); ++i)
    {
        if (strcmp(i->d_name, ".") == 0 || strcmp(i->d_name, "..") == 0) continue;
        if (i.isdir())
        {
            Path sub(*this, i->d_name);
            sub.rmtree();
        }
        else
            unlinkat(i->d_name);
    }
    // TODO: is there a way to do this using fd instead?
    rmdir(name());
}

std::string Path::mkdtemp(const std::string& prefix)
{
    char* fbuf = (char*)alloca(prefix.size() + 7);
    memcpy(fbuf, prefix.data(), prefix.size());
    memcpy(fbuf + prefix.size(), "XXXXXX", 7);
    return mkdtemp(fbuf);
}

std::string Path::mkdtemp(const char* prefix)
{
    size_t prefix_size = strlen(prefix);
    char* fbuf = (char*)alloca(prefix_size + 7);
    memcpy(fbuf, prefix, prefix_size);
    memcpy(fbuf + prefix_size, "XXXXXX", 7);
    return mkdtemp(fbuf);
}

std::string Path::mkdtemp(char* pathname_template)
{
    if (char* pathname = ::mkdtemp(pathname_template))
        return pathname;
    throw std::system_error(errno, std::system_category(), std::string("mkdtemp failed on ") + pathname_template);
}

/*
 * File
 */

File::File(const std::string& pathname)
    : ManagedNamedFileDescriptor(-1, pathname)
{
}

File::File(const std::string& pathname, int flags, mode_t mode)
    : ManagedNamedFileDescriptor(-1, pathname)
{
    open(flags, mode);
}

void File::open(int flags, mode_t mode)
{
    close();
    fd = ::open(pathname.c_str(), flags, mode);
    if (fd == -1)
        throw std::system_error(errno, std::system_category(), "cannot open file " + pathname);
}

bool File::open_ifexists(int flags, mode_t mode)
{
    close();
    fd = ::open(pathname.c_str(), flags, mode);
    if (fd != -1) return true;
    if (errno == ENOENT) return false;
    throw std::system_error(errno, std::system_category(), "cannot open file " + pathname);
}

File File::mkstemp(const std::string& prefix)
{
    char* fbuf = (char*)alloca(prefix.size() + 7);
    memcpy(fbuf, prefix.data(), prefix.size());
    memcpy(fbuf + prefix.size(), "XXXXXX", 7);
    return mkstemp(fbuf);
}

File File::mkstemp(const char* prefix)
{
    size_t prefix_size = strlen(prefix);
    char* fbuf = (char*)alloca(prefix_size + 7);
    memcpy(fbuf, prefix, prefix_size);
    memcpy(fbuf + prefix_size, "XXXXXX", 7);
    return mkstemp(fbuf);
}

File File::mkstemp(char* pathname_template)
{
    int fd = ::mkstemp(pathname_template);
    if (fd < 0)
        throw std::system_error(errno, std::system_category(), std::string("cannot create temporary file ") + pathname_template);
    return File(fd, pathname_template);
}


/*
 * Tempfile
 */

Tempfile::Tempfile() : sys::File(sys::File::mkstemp("")) {}
Tempfile::Tempfile(const std::string& prefix) : sys::File(sys::File::mkstemp(prefix)) {}
Tempfile::Tempfile(const char* prefix) : sys::File(sys::File::mkstemp(prefix)) {}

Tempfile::~Tempfile()
{
    if (m_unlink_on_exit)
        ::unlink(name().c_str());
}

void Tempfile::unlink_on_exit(bool val)
{
    m_unlink_on_exit = val;
}

void Tempfile::unlink()
{
    sys::unlink(name());
}


/*
 * Tempdir
 */

Tempdir::Tempdir() : sys::Path(sys::Path::mkdtemp("")) {}
Tempdir::Tempdir(const std::string& prefix) : sys::Path(sys::Path::mkdtemp(prefix)) {}
Tempdir::Tempdir(const char* prefix) : sys::Path(sys::Path::mkdtemp(prefix)) {}

Tempdir::~Tempdir()
{
    if (m_rmtree_on_exit)
        try {
            rmtree();
        } catch (...) {
        }
}

void Tempdir::rmtree_on_exit(bool val)
{
    m_rmtree_on_exit = val;
}


std::string read_file(const std::string& file)
{
    File in(file, O_RDONLY);

    // Get the file size
    struct stat st;
    in.fstat(st);

    if (st.st_size == 0)
        return std::string();

    // mmap the input file
    MMap src = in.mmap(st.st_size, PROT_READ, MAP_SHARED);

    return std::string((const char*)src, st.st_size);
}

void write_file(const std::string& file, const std::string& data, mode_t mode)
{
    write_file(file, data.data(), data.size(), mode);
}

void write_file(const std::string& file, const void* data, size_t size, mode_t mode)
{
    File out(file, O_WRONLY | O_CREAT | O_TRUNC, mode);
    out.write_all_or_retry(data, size);
    out.close();
}

void write_file_atomically(const std::string& file, const std::string& data, mode_t mode)
{
    write_file_atomically(file, data.data(), data.size(), mode);
}

void write_file_atomically(const std::string& file, const void* data, size_t size, mode_t mode)
{
    File out = File::mkstemp(file);

    // Read the umask
    mode_t mask = umask(0777);
    umask(mask);

    // Set the file permissions, honoring umask
    out.fchmod(mode & ~mask);

    out.write_all_or_retry(data, size);
    out.close();

    if (::rename(out.name().c_str(), file.c_str()) < 0)
        throw std::system_error(errno, std::system_category(), "cannot rename " + out.name() + " to " + file);
}

#if 0
void mkFilePath(const std::string& file)
{
    size_t pos = file.rfind('/');
    if (pos != std::string::npos)
        mkpath(file.substr(0, pos));
}
#endif

bool unlink_ifexists(const std::string& file)
{
    if (::unlink(file.c_str()) != 0)
    {
        if (errno != ENOENT)
            throw std::system_error(errno, std::system_category(), "cannot unlink " + file);
        else
            return false;
    }
    else
        return true;
}

void rename(const std::string& src_pathname, const std::string& dst_pathname)
{
    if (::rename(src_pathname.c_str(), dst_pathname.c_str()) != 0)
        throw std::system_error(errno, std::system_category(), "cannot rename " + src_pathname + " to " + dst_pathname);
}

bool rename_ifexists(const std::string& src, const std::string& dst)
{
    if (::rename(src.c_str(), dst.c_str()) != 0)
    {
        if (errno != ENOENT)
            throw std::system_error(errno, std::system_category(), "cannot rename " + src + " to " + dst);
        else
            return false;
    }
    else
        return true;
}

void touch(const std::string& pathname, time_t ts)
{
    struct utimbuf t = { ts, ts };
    if (::utime(pathname.c_str(), &t) != 0)
        throw std::system_error(errno, std::system_category(), "cannot set mtime/atime of " + pathname);
}


template<typename String>
static bool impl_mkdir_ifmissing(String pathname, mode_t mode)
{
    for (unsigned i = 0; i < 5; ++i)
    {
        // If it does not exist, make it
        if (::mkdir(to_cstring(pathname), mode) != -1)
            return true;

        // throw on all errors except EEXIST. Note that EEXIST "includes the case
        // where pathname is a symbolic link, dangling or not."
        if (errno != EEXIST && errno != EISDIR)
        {
            std::stringstream msg;
            msg << "cannot create directory " << pathname;
            throw std::system_error(errno, std::system_category(), msg.str());
        }

        // Ensure that, if dir exists, it is a directory
        std::unique_ptr<struct stat> st = sys::stat(pathname);
        if (st.get() == NULL)
        {
            // Either dir has just been deleted, or we hit a dangling
            // symlink.
            //
            // Retry creating a directory: the more we keep failing, the more
            // the likelyhood of a dangling symlink increases.
            //
            // We could lstat here, but it would add yet another case for a
            // race condition if the broken symlink gets deleted between the
            // stat and the lstat.
            continue;
        }
        else if (!S_ISDIR(st->st_mode))
        {
            // If it exists but it is not a directory, complain
            std::stringstream msg;
            msg << pathname << " exists but is not a directory";
            throw std::runtime_error(msg.str());
        }
        else
            // If it exists and it is a directory, we're fine
            return false;
    }
    std::stringstream msg;
    msg << pathname << " exists and looks like a dangling symlink";
    throw std::runtime_error(msg.str());
}

bool mkdir_ifmissing(const char* pathname, mode_t mode)
{
    return impl_mkdir_ifmissing(pathname, mode);
}

bool mkdir_ifmissing(const std::string& pathname, mode_t mode)
{
    return impl_mkdir_ifmissing(pathname, mode);
}

bool makedirs(const std::string& pathname, mode_t mode)
{
    if (pathname == "/" || pathname == ".") return false;
    std::string parent = str::dirname(pathname);

    // First ensure that the upper path exists
    makedirs(parent, mode);

    // Then create this dir
    return mkdir_ifmissing(pathname, mode);
}

std::string which(const std::string& name)
{
    // argv[0] has an explicit path: ensure it becomes absolute
    if (name.find('/') != std::string::npos)
        return sys::abspath(name);

    // argv[0] has no explicit path, look for it in $PATH
    const char* path = getenv("PATH");
    if (!path) return name;

    str::Split splitter(path, ":", true);
    for (const auto& i: splitter)
    {
        std::string candidate = str::joinpath(i, name);
        if (sys::access(candidate, X_OK))
            return sys::abspath(candidate);
    }

    return name;
}

void unlink(const std::string& pathname)
{
    if (::unlink(pathname.c_str()) < 0)
        throw std::system_error(errno, std::system_category(), "cannot unlink " + pathname);
}

void rmdir(const std::string& pathname)
{
    if (::rmdir(pathname.c_str()) < 0)
        throw std::system_error(errno, std::system_category(), "cannot rmdir " + pathname);
}

void rmtree(const std::string& pathname)
{
    Path path(pathname);
    path.rmtree();
}

bool rmtree_ifexists(const std::string& pathname)
{
    int fd = open(pathname.c_str(), O_PATH);
    if (fd == -1)
    {
        if (errno == ENOENT)
            return false;
        throw std::system_error(errno, std::system_category(), "cannot open path " + pathname);
    }
    Path path(fd, pathname);
    path.rmtree();
    return true;
}

void clock_gettime(::clockid_t clk_id, struct ::timespec& ts)
{
    int res = ::clock_gettime(clk_id, &ts);
    if (res == -1)
        throw std::system_error(errno, std::system_category(), "clock_gettime failed on clock " + std::to_string(clk_id));
}

unsigned long long timesec_elapsed(const struct ::timespec& begin, const struct ::timespec& until)
{
    if (begin.tv_sec > until.tv_sec)
        return 0;

    if (begin.tv_sec == until.tv_sec)
    {
        if (begin.tv_nsec > until.tv_nsec)
            return 0;
        return until.tv_nsec - begin.tv_nsec;
    }

    if (until.tv_nsec < begin.tv_nsec)
        return (until.tv_sec - begin.tv_sec - 1) * 1000000000 + (until.tv_nsec + 1000000000 - begin.tv_nsec);
    else
        return (until.tv_sec - begin.tv_sec) * 1000000000 + until.tv_nsec - begin.tv_nsec;
}

/*
 * Clock
 */

Clock::Clock(clockid_t clk_id)
    : clk_id(clk_id)
{
    clock_gettime(clk_id, ts);
}

unsigned long long Clock::elapsed()
{
    struct timespec cur_ts;
    clock_gettime(clk_id, cur_ts);
    return timesec_elapsed(ts, cur_ts);
}


/*
 * rlimit
 */

void getrlimit(int resource, struct ::rlimit& rlim)
{
    if (::getrlimit(resource, &rlim) == -1)
        throw std::system_error(errno, std::system_category(), "getrlimit failed");
}

void setrlimit(int resource, const struct ::rlimit& rlim)
{
    if (::setrlimit(resource, &rlim) == -1)
        throw std::system_error(errno, std::system_category(), "setrlimit failed");
}

OverrideRlimit::OverrideRlimit(int resource, rlim_t rlim)
    : resource(resource)
{
    getrlimit(resource, orig);
    set(rlim);
}

OverrideRlimit::~OverrideRlimit()
{
    setrlimit(resource, orig);
}

void OverrideRlimit::set(rlim_t rlim)
{
    struct rlimit newval(orig);
    newval.rlim_cur = rlim;
    setrlimit(resource, newval);
}

}
}
}
