#include "config.h"
#include "arki/libconfig.h"
#include "arki/utils.h"
#include "files.h"
#include "sys.h"
#include "string.h"
#include <deque>
#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <system_error>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>

using namespace std;

namespace arki {
namespace utils {
namespace files {

bool filesystem_has_holes(const std::string& dir)
{
    static const unsigned test_size = 512 * 10;

    // Create a file with holes
    sys::File fd = sys::File::mkstemp(dir);
    sys::unlink(fd.name());
    fd.ftruncate(test_size);

    struct stat st;
    fd.fstat(st);
    if (st.st_blocks > 0)
        return false;

    sys::File out("/dev/null", O_WRONLY);
    off_t offset = 0;
    ssize_t res = sendfile(out, fd, &offset, test_size);
    if (res == -1)
        throw_system_error("cannot sendfile(2) " + std::to_string(test_size) + " bytes");
    if (offset != test_size)
        throw_system_error("sendfile read only " + std::to_string(offset) + "/" + std::to_string(test_size));

    fd.fstat(st);
    return st.st_blocks == 0;
}

bool filesystem_has_ofd_locks(const std::string& dir)
{
    sys::File fd1 = sys::File::mkstemp(dir);
    sys::File fd2(fd1.name());
    try {
        fd2.open(O_RDWR);
    } catch (...) {
        sys::unlink(fd1.name());
        throw;
    }
    sys::unlink(fd1.name());

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

void createDontpackFlagfile(const std::string& dir)
{
	utils::createFlagfile(str::joinpath(dir, FLAGFILE_DONTPACK));
}
void createNewDontpackFlagfile(const std::string& dir)
{
	utils::createNewFlagfile(str::joinpath(dir, FLAGFILE_DONTPACK));
}
void removeDontpackFlagfile(const std::string& dir)
{
    sys::unlink_ifexists(str::joinpath(dir, FLAGFILE_DONTPACK));
}
bool hasDontpackFlagfile(const std::string& dir)
{
    return sys::exists(str::joinpath(dir, FLAGFILE_DONTPACK));
}

void resolve_path(const std::string& pathname, std::string& basedir, std::string& relpath)
{
    if (pathname[0] == '/')
        basedir.clear();
    else
        basedir = sys::getcwd();
    relpath = str::normpath(pathname);
}


PathWalk::PathWalk(const std::string& root, Consumer consumer)
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

void PathWalk::walk(const std::string& relpath, sys::Path& path)
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
            string subpath = str::joinpath(relpath, i->d_name);
            sys::Path subdir(path, i->d_name);
            walk(subpath, subdir);
        }
    }
}


PreserveFileTimes::PreserveFileTimes(const std::string& fname)
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


RenameTransaction::RenameTransaction(const std::string& tmpabspath, const std::string& abspath)
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
    sys::rename(tmpabspath, abspath);
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
        sys::unlink_ifexists(f);
    fired = true;
}

void FinalizeTempfilesTransaction::rollback_nothrow() noexcept
{
    if (fired) return;
    for (const auto& f: tmpfiles)
        ::unlink(f.c_str());
    fired = true;
}


static std::string rcDirName(const std::string& nameInConfdir, const std::string& nameInEnv)
{
    std::string dirname;
    char* fromEnv = 0;

    // Otherwise the directory given in the environment variable nameInEnv is tried.
    if ((fromEnv = getenv(nameInEnv.c_str())))
        return fromEnv;

#ifdef CONF_DIR
    // Else, CONF_DIR is tried.
    return string(CONF_DIR) + "/" + nameInConfdir;
#else
    // Or if there is no config, we fail to read anything
    return string();
#endif
}

std::vector<std::string> rcFiles(const std::string& nameInConfdir, const std::string& nameInEnv)
{
    std::string dirname = rcDirName(nameInConfdir, nameInEnv);

    vector<string> files;
    sys::Path dir(dirname);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        string file = i->d_name;
        // Skip hidden files
        if (file[0] == '.') continue;
        // Skip backup files
        if (file[file.size() - 1] == '~') continue;
        // Skip non-files
        if (!i.isreg()) continue;
        files.push_back(str::joinpath(dirname, i->d_name));
    }

    // Sort the file names
    std::sort(files.begin(), files.end());

    return files;
}

std::string readRcDir(const std::string& nameInConfdir, const std::string& nameInEnv)
{
    vector<string> files = rcFiles(nameInConfdir, nameInEnv);

    // Read all the contents
    std::string res;
    for (const auto& file: files)
        res += sys::read_file(file);
    return res;
}

SourceCode readSourceFromRcDir(const std::string& nameInConfdir, const std::string& nameInEnv)
{
    vector<string> files = rcFiles(nameInConfdir, nameInEnv);
    SourceCode res;

    // Read all the contents
    for (const auto& file: files)
    {
        string tmp = sys::read_file(file);
        res.push_back(FileInfo(file, tmp.size()));
        res.code += tmp;
    }
    return res;
}

}
}
}
