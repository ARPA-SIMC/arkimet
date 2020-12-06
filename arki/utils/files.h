#ifndef ARKI_UTILS_FILES_H
#define ARKI_UTILS_FILES_H

#include <arki/defs.h>
#include <arki/utils/sys.h>
#include <arki/exceptions.h>
#include <arki/core/transaction.h>
#include <string>
#include <sys/types.h>
#include <set>
#include <vector>

#define FLAGFILE_INDEX "index-out-of-sync"
#define FLAGFILE_DONTPACK "needs-check-do-not-pack"

namespace arki {
namespace utils {
namespace files {

/**
 * Check if a directory is on a filesystem that supports holes in files, by
 * writing a temporary file with holes on that directory and checking how much
 * disk has actually been allocated.
 */
bool filesystem_has_holes(const std::string& dir);

/**
 * Check if a directory is on a filesystem that supports Open File Descriptor
 * locks, locking a file twice and seeing if the second lock attempt fails.
 */
bool filesystem_has_ofd_locks(const std::string& dir);

// Flagfile handling

/// Create an empty file, succeeding if it already exists
void createFlagfile(const std::string& pathname);

/// Create an empty file, succeeding if it already exists
void createDontpackFlagfile(const std::string& dir);

/// Remove a file, succeeding if it does not exists
void removeDontpackFlagfile(const std::string& dir);

/// Check if a file exists
bool hasDontpackFlagfile(const std::string& dir);

/**
 * Normalise a pathname and resolve it in the file system.
 *
 * @param pathname
 *   The path name to resolve. It can be absolute or relative.
 * @retval basedir
 *   The base directory to use with str::joinpath to make the file name absolute.
 *   It is set to the empty string if \a pathname is an absolute path
 * @retval relpath
 *   The normalised version of \a pathname
 */
void resolve_path(const std::string& pathname, std::string& basedir, std::string& relpath);


/**
 * Recursively visit a directory and all its subdirectories, depth-first.
 *
 * It uses an inode cache to prevent recursion loops.
 */
struct PathWalk
{
    typedef std::function<bool(const std::string& relpath, sys::Path::iterator& entry, struct stat& st)> Consumer;
    std::string root;
    Consumer consumer;
    std::set<ino_t> seen;

    PathWalk(const std::string& root, Consumer consumer=nullptr);

    /// Start the visit
    void walk();

protected:
    void walk(const std::string& relpath, sys::Path& path);
};


struct PreserveFileTimes
{
    std::string fname;
    struct timespec times[2];

    PreserveFileTimes(const std::string& fname);
    ~PreserveFileTimes() noexcept(false);
};

struct RenameTransaction : public core::Transaction
{
    std::string tmpabspath;
    std::string abspath;
    bool fired = false;

    RenameTransaction(const std::string& tmpabspath, const std::string& abspath);
    virtual ~RenameTransaction();

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};

/**
 * On commit, call on_commit(tmpfiles).
 *
 * On rollback, unlink all tmpfiles
 */
struct FinalizeTempfilesTransaction : public core::Transaction
{
    std::vector<std::string> tmpfiles;
    std::function<void(const std::vector<std::string>& tmpfiles)> on_commit;
    bool fired = false;

    virtual ~FinalizeTempfilesTransaction();

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};

struct RAIIFILE
{
    FILE* fd = nullptr;
    RAIIFILE(const std::string& fname, const char* mode)
    {
        if (!(fd = fopen(fname.c_str(), mode)))
            throw_file_error(fname, "cannot open file");
    }
    RAIIFILE(sys::NamedFileDescriptor& ifd, const char* mode)
    {
        if (!(fd = fdopen(ifd.dup(), mode)))
            throw_file_error(ifd.name(), "cannot fdopen file");
    }
    RAIIFILE(const RAIIFILE&) = delete;
    RAIIFILE(RAIIFILE&& o)
        : fd(o.fd)
    {
        o.fd = nullptr;
    }
    RAIIFILE& operator=(const RAIIFILE&) = delete;
    RAIIFILE& operator=(RAIIFILE&&) = delete;
    ~RAIIFILE()
    {
        if (fd) fclose(fd);
    }
    operator FILE*() { return fd; }
};

}
}
}
#endif
