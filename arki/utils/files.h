#ifndef ARKI_UTILS_FILES_H
#define ARKI_UTILS_FILES_H

/// utils/files - arkimet-specific file functions

#include <arki/defs.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <memory>

#define FLAGFILE_REBUILD ".needs-rebuild"
#define FLAGFILE_PACK ".needs-pack"
#define FLAGFILE_INDEX "index-out-of-sync"
#define FLAGFILE_DONTPACK "needs-check-do-not-pack"

namespace arki {
namespace utils {
namespace files {

// Flagfile handling

/// Create an empty file, succeeding if it already exists
void createRebuildFlagfile(const std::string& pathname);

/// Create an empty file, failing if it already exists
void createNewRebuildFlagfile(const std::string& pathname);

/// Remove a file, succeeding if it does not exists
void removeRebuildFlagfile(const std::string& pathname);

/// Check if a file exists
bool hasRebuildFlagfile(const std::string& pathname);


/// Create an empty file, succeeding if it already exists
void createPackFlagfile(const std::string& pathname);

/// Create an empty file, failing if it already exists
void createNewPackFlagfile(const std::string& pathname);

/// Remove a file, succeeding if it does not exists
void removePackFlagfile(const std::string& pathname);

/// Check if a file exists
bool hasPackFlagfile(const std::string& pathname);


/// Create an empty file, succeeding if it already exists
void createIndexFlagfile(const std::string& dir);

/// Create an empty file, failing if it already exists
void createNewIndexFlagfile(const std::string& dir);

/// Remove a file, succeeding if it does not exists
void removeIndexFlagfile(const std::string& dir);

/// Check if a file exists
bool hasIndexFlagfile(const std::string& dir);


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
 * @retval relname
 *   The normalised version of \a pathname
 */
void resolve_path(const std::string& pathname, std::string& basedir, std::string& relname);

/**
 * Normalise a file format string using the most widely used version
 *
 * This currently normalises:
 *  - grib1 and grib2 to grib
 *  - all of h5, hdf5, odim and odimh5 to odimh5
 */
std::string normaliseFormat(const std::string& format);

/**
 * Guess a file format from its extension
 */
std::string format_from_ext(const std::string& fname, const char* default_format=0);

struct PreserveFileTimes
{
    std::string fname;
    struct timespec times[2];

    PreserveFileTimes(const std::string& fname);
    ~PreserveFileTimes();
};

/**
 * Create a LineReader from a file descriptor.
 *
 * The file descriptor is not managed by the LineReader, and will ned to be
 * kept open by the caller for as long as the line reader is used, then closed
 * at the end.
 *
 * Note that a LineReader on a file descriptor needs to do read ahead to avoid
 * reading one character at a time, so if the caller stops calling getline(),
 * the file descriptor is likely to be positioned further ahead than the last
 * line read.
 */
std::unique_ptr<LineReader> linereader_from_fd(int fd, const std::string& pathname);

/**
 * Create a LineReader from a buffer on a string.
 *
 * No copy is made of the data: the buffer must remain valid for as long as the
 * line reader is used.
 */
std::unique_ptr<LineReader> linereader_from_chars(const char* buf, size_t size);

}
}
}
#endif
