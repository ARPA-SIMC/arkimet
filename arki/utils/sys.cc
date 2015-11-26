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
#include <fcntl.h>
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

MMap FileDescriptor::mmap(size_t length, int prot, int flags, off_t offset)
{
    void* res =::mmap(0, length, prot, flags, fd, offset);
    if (res == MAP_FAILED)
        throw_error("cannot mmap");
    return MMap(res, length);
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
 * Path
 */

Path::Path(const char* pathname, int flags)
    : NamedFileDescriptor(-1, pathname)
{
    fd = open(pathname, flags | O_PATH);
    if (fd == -1)
        throw_error("cannot open path");
}

Path::Path(const std::string& pathname, int flags)
    : NamedFileDescriptor(-1, pathname)
{
    fd = open(pathname.c_str(), flags | O_PATH);
    if (fd == -1)
        throw_error("cannot open path");
}

Path::Path(Path& parent, const char* pathname, int flags)
    : NamedFileDescriptor(parent.openat(pathname, flags | O_PATH),
            str::joinpath(parent.name(), pathname))
{
}

Path::~Path()
{
    if (fd != -1)
        ::close(fd);
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

void Path::fstatat(const char* pathname, struct stat& st)
{
    if (::fstatat(fd, pathname, &st, 0) == -1)
        throw_error("cannot fstatat");
}

void Path::lstatat(const char* pathname, struct stat& st)
{
    if (::fstatat(fd, pathname, &st, AT_SYMLINK_NOFOLLOW) == -1)
        throw_error("cannot fstatat");
}

void Path::unlinkat(const char* pathname)
{
    if (::unlinkat(fd, pathname, 0) == -1)
        throw_error("cannot unlinkat");
}

void Path::rmdirat(const char* pathname)
{
    if (::unlinkat(fd, pathname, AT_REMOVEDIR) == -1)
        throw_error("cannot unlinkat");
}

Path::iterator::iterator()
{
}

Path::iterator::iterator(Path& dir)
    : path(&dir)
{
    this->dir = dir.fdopendir();

    long name_max = fpathconf(dir.fd, _PC_NAME_MAX);
    if (name_max == -1) // Limit not defined, or error: take a guess
        name_max = 255;
    size_t len = offsetof(dirent, d_name) + name_max + 1;
    cur_entry = (struct dirent*)malloc(len);
    if (cur_entry == NULL)
        throw std::bad_alloc();

    operator++();
}

Path::iterator::~iterator()
{
    if (cur_entry) free(cur_entry);
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
    struct dirent* result;
    if (readdir_r(dir, cur_entry, &result) != 0)
        path->throw_error("cannot readdir_r");

    if (result == nullptr)
    {
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

/*
 * File
 */

File::File(const std::string& pathname)
    : NamedFileDescriptor(-1, pathname)
{
}

File::File(const std::string& pathname, int flags, mode_t mode)
    : NamedFileDescriptor(-1, pathname)
{
    open(flags, mode);
}

File::~File()
{
    if (fd != -1) ::close(fd);
}

void File::open(int flags, mode_t mode)
{
    fd = ::open(pathname.c_str(), flags, mode);
    if (fd == -1)
        throw std::system_error(errno, std::system_category(), "cannot open file " + pathname);
}

bool File::open_ifexists(int flags, mode_t mode)
{
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
    int fd = ::mkstemp(fbuf);
    if (fd < 0)
        throw std::system_error(errno, std::system_category(), std::string("cannot create temporary file ") + fbuf);
    return File(fd, fbuf);
}

std::string read_file(const std::string& file)
{
    File in(file, O_RDONLY);

    // Get the file size
    struct stat st;
    in.fstat(st);

    // mmap the input file
    MMap src = in.mmap(st.st_size, PROT_READ, MAP_SHARED);

    return std::string((const char*)src, st.st_size);
}

void write_file(const std::string& file, const std::string& data, mode_t mode)
{
    File out(file, O_WRONLY | O_CREAT, mode);
    out.write_all_or_retry(data.data(), data.size());
    out.close();
}

void write_file_atomically(const std::string& file, const std::string& data, mode_t mode)
{
    File out = File::mkstemp(file);

    // Read the umask
    mode_t mask = umask(0777);
    umask(mask);

    // Set the file permissions, honoring umask
    out.fchmod(mode & ~mask);

    out.write_all_or_retry(data.data(), data.size());
    out.close();

    if (rename(out.name().c_str(), file.c_str()) < 0)
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

template<typename String>
static void impl_mkdir_ifmissing(String pathname, mode_t mode)
{
    for (unsigned i = 0; i < 5; ++i)
    {
        // If it does not exist, make it
        if (::mkdir(to_cstring(pathname), mode) != -1)
            return;

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
            return;
    }
    std::stringstream msg;
    msg << pathname << " exists and looks like a dangling symlink";
    throw std::runtime_error(msg.str());
}

void mkdir_ifmissing(const char* pathname, mode_t mode)
{
    return impl_mkdir_ifmissing(pathname, mode);
}

void mkdir_ifmissing(const std::string& pathname, mode_t mode)
{
    return impl_mkdir_ifmissing(pathname, mode);
}

void makedirs(const std::string& pathname, mode_t mode)
{
    if (pathname == "/" || pathname == ".") return;
    std::string parent = str::dirname(pathname);

    // First ensure that the upper path exists
    makedirs(parent, mode);

    // Then create this dir
    mkdir_ifmissing(pathname, mode);
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

#if 0
std::string mkdtemp( std::string tmpl )
{
    char *_tmpl = reinterpret_cast< char * >( alloca( tmpl.size() + 1 ) );
    strcpy( _tmpl, tmpl.c_str() );
    return ::mkdtemp( _tmpl );
}
#endif
}
}
}
