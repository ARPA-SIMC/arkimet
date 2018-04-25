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

std::string read_file(const std::string &file)
{
    if (file == "-")
    {
        static const size_t bufsize = 4096;
        char buf[bufsize];
        std::string res;
        sys::NamedFileDescriptor in(0, "(stdin)");
        while (true)
        {
            size_t count = in.read(buf, bufsize);
            if (count == 0) break;
            res.append(buf, count);
        }
        return res;
    }
    else
        return sys::read_file(file);
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


Rename2Transaction::Rename2Transaction(const std::string& tmpabspath1, const std::string& abspath1, const std::string& tmpabspath2, const std::string& abspath2)
    : tmpabspath1(tmpabspath1), abspath1(abspath1), tmpabspath2(tmpabspath2), abspath2(abspath2)
{
}

Rename2Transaction::~Rename2Transaction()
{
    if (!fired) rollback_nothrow();
}

void Rename2Transaction::commit()
{
    if (fired) return;
    sys::rename(tmpabspath1, abspath1);
    sys::rename(tmpabspath2, abspath2);
    fired = true;
}

void Rename2Transaction::rollback()
{
    if (fired) return;
    sys::unlink(tmpabspath1);
    sys::unlink(tmpabspath2);
    fired = true;
}

void Rename2Transaction::rollback_nothrow() noexcept
{
    if (fired) return;
    ::unlink(tmpabspath1.c_str());
    ::unlink(tmpabspath2.c_str());
    fired = true;
}

}
}
}
