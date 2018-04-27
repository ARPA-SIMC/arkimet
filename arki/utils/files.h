#ifndef ARKI_UTILS_FILES_H
#define ARKI_UTILS_FILES_H

#include <arki/defs.h>
#include <arki/utils/sys.h>
#include <arki/transaction.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <memory>
#include <set>

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

// Flagfile handling

/// Create an empty file, succeeding if it already exists
void createDontpackFlagfile(const std::string& dir);

/// Create an empty file, failing if it already exists
void createNewDontpackFlagfile(const std::string& dir);

/// Remove a file, succeeding if it does not exists
void removeDontpackFlagfile(const std::string& dir);

/// Check if a file exists
bool hasDontpackFlagfile(const std::string& dir);

/**
 * Same as sys::read_file, but if \a file is "-" then reads all from
 * stdin
 */
std::string read_file(const std::string &file);

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

struct RenameTransaction : public Transaction
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

struct Rename2Transaction : public Transaction
{
    std::string tmpabspath1;
    std::string abspath1;
    std::string tmpabspath2;
    std::string abspath2;
    bool fired = false;

    Rename2Transaction(const std::string& tmpabspath1, const std::string& abspath1, const std::string& tmpabspath2, const std::string& abspath2);
    virtual ~Rename2Transaction();

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};

}
}
}
#endif
