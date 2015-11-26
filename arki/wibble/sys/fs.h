#ifndef WIBBLE_SYS_DIRECTORY_H
#define WIBBLE_SYS_DIRECTORY_H

#include <string>
#include <iosfwd>
#include <sys/types.h>  // mode_t
#include <sys/stat.h>   // struct stat
#include <unistd.h>     // access

struct dirent;

namespace wibble {
namespace sys {
namespace fs {

/**
 * stat() the given file filling in the given structure.
 * Raises exceptions in case of errors, including if the file does not exist.
 */
void stat(const std::string& pathname, struct stat& st);

/// access() a filename
bool access(const std::string& s, int m);

/// Same as access(s, F_OK);
bool exists(const std::string& s);

/**
 * Get the absolute path of a file
 */
std::string abspath(const std::string& pathname);

// Create a temporary directory based on a template.
std::string mkdtemp( std::string templ );

/// Read whole file into memory. Throws exceptions on failure.
std::string readFile(const std::string &file);

/**
 * Read the entire contents of a file into a string
 *
 * @param filename
 *   name or description of the stream we are reading. Used only for error
 *   messages.
 */
std::string readFile(std::istream& file, const std::string& filename);

/// Write \a data to \a file, replacing existing contents if it already exists
void writeFile(const std::string &file, const std::string &data);

/**
 * Write \a data to \a file, replacing existing contents if it already exists
 *
 * Data is written to a temporary file, then moved to its final destination, to
 * ensure an atomic operation.
 */
void writeFileAtomically(const std::string &file, const std::string &data);

/**
 * Compute the absolute path of an executable.
 *
 * If \a name is specified as a partial path, it ensures it is made absolute.
 * If \a name is not specified as a path, it looks for the executable in $PATH
 * and return its absolute pathname.
 */
std::string findExecutable(const std::string& name);

/**
 * Delete a file if it exists. If it does not exist, do nothing.
 *
 * @return true if the file was deleted, false if it did not exist
 */
bool deleteIfExists(const std::string& file);

/// Move \a src to \a dst, without raising exception if \a src does not exist
void renameIfExists(const std::string& src, const std::string& dst);

/// Delete the file
void unlink(const std::string& fname);

/// Remove the directory using rmdir(2)
void rmdir(const std::string& dirname);

/**
 * Returns true if the given pathname is a directory, else false.
 *
 * It also returns false if the pathname does not exist.
 */
bool isdir(const std::string& pathname);

/// Same as isdir but checks for block devices
bool isblk(const std::string& pathname);

/// Same as isdir but checks for character devices
bool ischr(const std::string& pathname);

/// Same as isdir but checks for FIFOs
bool isfifo(const std::string& pathname);

/// Same as isdir but checks for symbolic links
bool islnk(const std::string& pathname);

/// Same as isdir but checks for regular files
bool isreg(const std::string& pathname);

/// Same as isdir but checks for sockets
bool issock(const std::string& pathname);

/// File mtime
time_t timestamp(const std::string& file);

/// File size
size_t size(const std::string& file);

/// File inode number
ino_t inode(const std::string& file);

}
}
}

// vim:set ts=4 sw=4:
#endif
