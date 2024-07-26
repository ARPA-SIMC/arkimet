#include "files.h"
#include "sys.h"
#include "string.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <system_error>
#include <unistd.h>
#include <cstring>
#include <cerrno>

using namespace std;
using namespace std::string_literals;

namespace arki {
namespace utils {
namespace files {

bool filesystem_has_holes(const std::filesystem::path& dir)
{
    static const unsigned test_size = 512 * 10;

    // Create a file with holes
    sys::File fd = sys::File::mkstemp(dir);
    sys::unlink(fd.path());
    fd.ftruncate(test_size);

    struct stat st;
    fd.fstat(st);
    if (st.st_blocks > 0)
        return false;

    sys::File out("/dev/null", O_WRONLY);
    off_t offset = 0;
    ssize_t res = sendfile(out, fd, &offset, test_size);
    if (res == -1)
        throw_system_error("cannot sendfile(2) "s + std::to_string(test_size) + " bytes");
    if (offset != test_size)
        throw_system_error("sendfile read only "s + std::to_string(offset) + "/" + std::to_string(test_size));

    fd.fstat(st);
    return st.st_blocks == 0;
}

bool filesystem_has_ofd_locks(const std::filesystem::path& dir)
{
    sys::File fd1 = sys::File::mkstemp(dir);
    sys::File fd2(fd1.path());
    try {
        fd2.open(O_RDWR);
    } catch (...) {
        sys::unlink(fd1.path());
        throw;
    }
    sys::unlink(fd1.path());

    struct flock lock;
    memset(&lock, 0, sizeof(struct ::flock));
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 1;
    lock.l_pid = 0;

    bool res1 = fd1.ofd_setlk(lock);
    bool res2 = fd2.ofd_setlk(lock);

    return res1 && !res2;
}

void createFlagfile(const std::filesystem::path& pathname)
{
    sys::File fd(pathname, O_WRONLY | O_CREAT | O_NOCTTY, 0666);
    fd.close();
}

void createDontpackFlagfile(const std::filesystem::path& dir)
{
    createFlagfile(dir / FLAGFILE_DONTPACK);
}

void removeDontpackFlagfile(const std::filesystem::path& dir)
{
    std::filesystem::remove(dir / FLAGFILE_DONTPACK);
}

bool hasDontpackFlagfile(const std::filesystem::path& dir)
{
    return std::filesystem::exists(dir / FLAGFILE_DONTPACK);
}

void resolve_path(const std::filesystem::path& pathname, std::filesystem::path& basedir, std::filesystem::path& relpath)
{
    if (pathname.is_absolute())
        basedir.clear();
    else
        basedir = std::filesystem::current_path();
    relpath = pathname.lexically_normal();
}


PathWalk::PathWalk(const std::filesystem::path& root, Consumer consumer)
    : root(root), consumer(consumer)
{
}

void PathWalk::walk()
{
    sys::Path dir(root);
    struct stat st;
    dir.fstatat(".", st);
    seen.insert(st.st_ino);
    walk("", dir);
}

void PathWalk::walk(const std::filesystem::path& relpath, sys::Path& path)
{
    for (auto i = path.begin(); i != path.end(); ++i)
    {
        struct stat st;
        path.fstatat(i->d_name, st);

        // Prevent infinite loops
        if (seen.find(st.st_ino) != seen.end())
            continue;
        seen.insert(st.st_ino);

        // Pass the entry to the consumer, and skip it if the consumer does
        // not want it
        if (!consumer(relpath, i, st))
            continue;

        if (S_ISDIR(st.st_mode))
        {
            // Recurse
            auto subpath = relpath / i->d_name;
            sys::Path subdir(path, i->d_name);
            walk(subpath, subdir);
        }
    }
}


PreserveFileTimes::PreserveFileTimes(const std::filesystem::path& fname)
    : fname(fname)
{
    struct stat st;
    sys::stat(fname, st);
    times[0] = st.st_atim;
    times[1] = st.st_mtim;
}

PreserveFileTimes::~PreserveFileTimes() noexcept(false)
{
    if (utimensat(AT_FDCWD, fname.c_str(), times, 0) == -1)
        throw std::system_error(errno, std::system_category(), "cannot set file times");
}


RenameTransaction::RenameTransaction(const std::filesystem::path& tmpabspath, const std::filesystem::path& abspath)
    : tmpabspath(tmpabspath), abspath(abspath)
{
}

RenameTransaction::~RenameTransaction()
{
    if (!fired) rollback_nothrow();
}

void RenameTransaction::commit()
{
    if (fired) return;
    std::filesystem::rename(tmpabspath, abspath);
    fired = true;
}

void RenameTransaction::rollback()
{
    if (fired) return;
    sys::unlink(tmpabspath);
    fired = true;
}

void RenameTransaction::rollback_nothrow() noexcept
{
    if (fired) return;
    ::unlink(tmpabspath.c_str());
    fired = true;
}


FinalizeTempfilesTransaction::~FinalizeTempfilesTransaction()
{
    if (!fired) rollback_nothrow();
}

void FinalizeTempfilesTransaction::commit()
{
    if (fired) return;
    on_commit(tmpfiles);
    fired = true;
}

void FinalizeTempfilesTransaction::rollback()
{
    if (fired) return;
    for (const auto& f: tmpfiles)
        std::filesystem::remove(f);
    fired = true;
}

void FinalizeTempfilesTransaction::rollback_nothrow() noexcept
{
    if (fired) return;
    for (const auto& f: tmpfiles)
        ::unlink(f.c_str());
    fired = true;
}

}
}
}
