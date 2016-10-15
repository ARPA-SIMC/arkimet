#ifndef ARKI_UTILS_SYS_H
#define ARKI_UTILS_SYS_H

/**
 * @author Enrico Zini <enrico@enricozini.org>
 * @brief Operating system functions
 *
 * Copyright (C) 2007--2015  Enrico Zini <enrico@debian.org>
 */

#include <string>
//#include <iosfwd>
#include <memory>
#include <iterator>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

namespace arki {
namespace utils {
namespace sys {

/**
 * stat() the given file and return the struct stat with the results.
 * If the file does not exist, return NULL.
 * Raises exceptions in case of errors.
 */
std::unique_ptr<struct stat> stat(const std::string& pathname);

/**
 * stat() the given file filling in the given structure.
 * Raises exceptions in case of errors, including if the file does not exist.
 */
void stat(const std::string& pathname, struct stat& st);

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

/// File mtime (or def if the file does not exist)
time_t timestamp(const std::string& file, time_t def);

/// File size
size_t size(const std::string& file);

/// File size (or def if the file does not exist)
size_t size(const std::string& file, size_t def);

/// File inode number
ino_t inode(const std::string& file);

/// File inode number (or 0 if the file does not exist)
ino_t inode(const std::string& file, ino_t def);

/// access() a filename
bool access(const std::string& s, int m);

/// Same as access(s, F_OK);
bool exists(const std::string& s);

/// Get the absolute path of the current working directory
std::string getcwd();

/// Get the absolute path of a file
std::string abspath(const std::string& pathname);

/**
 * Wraps a mmapped memory area, unmapping it on destruction.
 *
 * MMap objects can be used as normal pointers
 */
class MMap
{
    void* addr;
    size_t length;

public:
    MMap(const MMap&) = delete;
    MMap(MMap&&);
    MMap(void* addr, size_t length);
    ~MMap();

    MMap& operator=(const MMap&) = delete;
    MMap& operator=(MMap&&);

    size_t size() const { return length; }

    void munmap();

    template<typename T>
    operator const T*() const { return reinterpret_cast<const T*>(addr); }

    template<typename T>
    operator T*() const { return reinterpret_cast<T*>(addr); };
};

/**
 * Common operations on file descriptors.
 *
 * Except when documented otherwise, methods of this class are just thin
 * wrappers around the libc functions with the same name, that check error
 * results and throw exceptions if the functions failed.
 *
 * Implementing what to do on construction and destruction is left to the
 * subclassers: at the FileDescriptor level, the destructor does nothing and
 * leaves the file descriptor open.
 */
class FileDescriptor
{
protected:
    int fd = -1;

public:
    FileDescriptor();
    FileDescriptor(FileDescriptor&& o);
    FileDescriptor(int fd);
    virtual ~FileDescriptor();

    // We can copy at the FileDescriptor level because the destructor does not
    // close fd
    FileDescriptor(const FileDescriptor& o) = default;
    FileDescriptor& operator=(const FileDescriptor& o) = default;

    /**
     * Throw an exception based on errno and the given message.
     *
     * This can be overridden by subclasses that may have more information
     * about the file descriptor, so that they can generate more descriptive
     * messages.
     */
    [[noreturn]] virtual void throw_error(const char* desc);

    /**
     * Throw a runtime_error unrelated from errno.
     *
     * This can be overridden by subclasses that may have more information
     * about the file descriptor, so that they can generate more descriptive
     * messages.
     */
    [[noreturn]] virtual void throw_runtime_error(const char* desc);

    /// Check if the file descriptor is open (that is, if it is not -1)
    bool is_open() const;

    /**
     * Close the file descriptor, setting its value to -1.
     *
     * Does nothing if the file descriptor is already closed
     */
    void close();

    void fstat(struct stat& st);
    void fchmod(mode_t mode);

    int dup();

    size_t read(void* buf, size_t count);

    /**
     * Read all the data into buf, throwing runtime_error in case of a partial
     * read
     */
    void read_all_or_throw(void* buf, size_t count);

    size_t write(const void* buf, size_t count);

    template<typename Container>
    size_t write(const Container& c)
    {
        return write(c.data(), c.size() * sizeof(Container::value_type));
    }

    /// Write all the data in buf, retrying partial writes
    void write_all_or_retry(const void* buf, size_t count);

    template<typename Container>
    void write_all_or_retry(const Container& c)
    {
        write_all_or_retry(c.data(), c.size() * sizeof(typename Container::value_type));
    }

    /**
     * Write all the data in buf, throwing runtime_error in case of a partial
     * write
     */
    void write_all_or_throw(const void* buf, size_t count);

    template<typename Container>
    void write_all_or_throw(const Container& c)
    {
        write_all_or_throw(c.data(), c.size() * sizeof(typename Container::value_type));
    }

    off_t lseek(off_t offset, int whence=SEEK_SET);

    size_t pread(void* buf, size_t count, off_t offset);
    size_t pwrite(const void* buf, size_t count, off_t offset);

    template<typename Container>
    size_t pwrite(const Container& c, off_t offset)
    {
        return pwrite(c.data(), c.size() * sizeof(typename Container::value_type), offset);
    }

    void ftruncate(off_t length);

    MMap mmap(size_t length, int prot, int flags, off_t offset=0);

    operator int() const { return fd; }
};


/**
 * File descriptor with a name
 */
class NamedFileDescriptor : public FileDescriptor
{
protected:
    std::string pathname;

public:
    NamedFileDescriptor(int fd, const std::string& pathname);
    NamedFileDescriptor(NamedFileDescriptor&&);
    NamedFileDescriptor& operator=(NamedFileDescriptor&&);

    // We can copy at the NamedFileDescriptor level because the destructor does not
    // close fd
    NamedFileDescriptor(const NamedFileDescriptor& o) = default;
    NamedFileDescriptor& operator=(const NamedFileDescriptor& o) = default;

    [[noreturn]] virtual void throw_error(const char* desc);
    [[noreturn]] virtual void throw_runtime_error(const char* desc);

    /// Return the file pathname
    const std::string& name() const { return pathname; }
};


/**
 * File descriptor that gets automatically closed in the object destructor.
 */
struct ManagedNamedFileDescriptor : public NamedFileDescriptor
{
    using NamedFileDescriptor::NamedFileDescriptor;

    ManagedNamedFileDescriptor(ManagedNamedFileDescriptor&&) = default;
    ManagedNamedFileDescriptor(const ManagedNamedFileDescriptor&) = delete;

    /**
     * The destructor closes the file descriptor, but does not check errors on
     * ::close().
     *
     * In normal program flow, it is a good idea to explicitly call
     * ManagedNamedFileDescriptor::close() in places where it can throw safely.
     */
    ~ManagedNamedFileDescriptor();

    ManagedNamedFileDescriptor& operator=(const ManagedNamedFileDescriptor&) = delete;
    ManagedNamedFileDescriptor& operator=(ManagedNamedFileDescriptor&&);
};


/**
 * Wrap a path on the file system opened with O_PATH.
 */
struct Path : public ManagedNamedFileDescriptor
{
    /**
     * Iterator for directory entries
     */
    struct iterator : public std::iterator<std::input_iterator_tag, struct dirent>
    {
        Path* path = nullptr;
        DIR* dir = nullptr;
        struct dirent* cur_entry = nullptr;

        // End iterator
        iterator();
        // Start iteration on dir
        iterator(Path& dir);
        iterator(iterator&) = delete;
        iterator(iterator&& o)
            : dir(o.dir), cur_entry(o.cur_entry)
        {
            o.dir = nullptr;
            o.cur_entry = nullptr;
        }
        ~iterator();
        iterator& operator=(iterator&) = delete;
        iterator& operator=(iterator&&) = delete;

        bool operator==(const iterator& i) const;
        bool operator!=(const iterator& i) const;
        struct dirent& operator*() const { return *cur_entry; }
        struct dirent* operator->() const { return cur_entry; }
        void operator++();

        /// @return true if we refer to a directory, else false
        bool isdir() const;

        /// @return true if we refer to a block device, else false
        bool isblk() const;

        /// @return true if we refer to a character device, else false
        bool ischr() const;

        /// @return true if we refer to a named pipe (FIFO).
        bool isfifo() const;

        /// @return true if we refer to a symbolic link.
        bool islnk() const;

        /// @return true if we refer to a regular file.
        bool isreg() const;

        /// @return true if we refer to a Unix domain socket.
        bool issock() const;
    };

    using ManagedNamedFileDescriptor::ManagedNamedFileDescriptor;

    /**
     * Open the given pathname with flags | O_PATH.
     */
    Path(const char* pathname, int flags=0);
    /**
     * Open the given pathname with flags | O_PATH.
     */
    Path(const std::string& pathname, int flags=0);
    /**
     * Open the given pathname calling parent.openat, with flags | O_PATH
     */
    Path(Path& parent, const char* pathname, int flags=0);
    Path(const Path&) = delete;
    Path(Path&&) = default;
    Path& operator=(const Path&) = delete;
    Path& operator=(Path&&) = default;

    DIR* fdopendir();

    /// Begin iterator on all directory entries
    iterator begin();

    /// End iterator on all directory entries
    iterator end();

    int openat(const char* pathname, int flags, mode_t mode=0777);

    void fstatat(const char* pathname, struct stat& st);

    /// fstatat with the AT_SYMLINK_NOFOLLOW flag set
    void lstatat(const char* pathname, struct stat& st);

    void unlinkat(const char* pathname);

    /// unlinkat with the AT_REMOVEDIR flag set
    void rmdirat(const char* pathname);

    /**
     * Delete the directory pointed to by this Path, with all its contents.
     *
     * The path must point to a directory.
     */
    void rmtree();
};


/**
 * File in the file system
 */
class File : public ManagedNamedFileDescriptor
{
public:
    using ManagedNamedFileDescriptor::ManagedNamedFileDescriptor;

    File(File&&) = default;
    File(const File&) = delete;

    /**
     * Create an unopened File object for the given pathname
     */
    File(const std::string& pathname);

    /// Wrapper around open(2)
    File(const std::string& pathname, int flags, mode_t mode=0777);

    File& operator=(const File&) = delete;
    File& operator=(File&&) = default;

    /// Wrapper around open(2)
    void open(int flags, mode_t mode=0777);

    /**
     * Wrap open(2) and return false instead of throwing an exception if open
     * fails with ENOENT
     */
    bool open_ifexists(int flags, mode_t mode=0777);

    static File mkstemp(const std::string& prefix);
};

/// Read whole file into memory. Throws exceptions on failure.
std::string read_file(const std::string &file);

/**
 * Write \a data to \a file, replacing existing contents if it already exists.
 *
 * New files are created with the given permission mode, honoring umask.
 * Permissions of existing files do not change.
 */
void write_file(const std::string& file, const std::string& data, mode_t mode=0777);

/**
 * Write \a data to \a file, replacing existing contents if it already exists.
 *
 * New files are created with the given permission mode, honoring umask.
 * Permissions of existing files do not change.
 */
void write_file(const std::string& file, const void* data, size_t size, mode_t mode=0777);

/**
 * Write \a data to \a file, replacing existing contents if it already exists.
 *
 * Files are created with the given permission mode, honoring umask. If the
 * file already exists, its mode is ignored.
 *
 * Data is written to a temporary file, then moved to its final destination, to
 * ensure an atomic operation.
 */
void write_file_atomically(const std::string& file, const std::string& data, mode_t mode=0777);

/**
 * Write \a data to \a file, replacing existing contents if it already exists.
 *
 * Files are created with the given permission mode, honoring umask. If the
 * file already exists, its mode is ignored.
 *
 * Data is written to a temporary file, then moved to its final destination, to
 * ensure an atomic operation.
 */
void write_file_atomically(const std::string& file, const void* data, size_t size, mode_t mode=0777);

#if 0
// Create a temporary directory based on a template.
std::string mkdtemp(std::string templ);

/// Ensure that the path to the given file exists, creating it if it does not.
/// The file itself will not get created.
void mkFilePath(const std::string& file);
#endif

/**
 * Delete a file if it exists. If it does not exist, do nothing.
 *
 * @return true if the file was deleted, false if it did not exist
 */
bool unlink_ifexists(const std::string& file);

/**
 * Move \a src to \a dst, without raising exception if \a src does not exist
 *
 * @return true if the file was renamed, false if it did not exist
 */
bool rename_ifexists(const std::string& src, const std::string& dst);

/**
 * Create the given directory, if it does not already exists.
 *
 * It will throw an exception if the given pathname already exists but is not a
 * directory.
 *
 * @returns true if the directory was created, false if it already existed.
 */
bool mkdir_ifmissing(const char* pathname, mode_t mode=0777);

bool mkdir_ifmissing(const std::string& pathname, mode_t mode=0777);

/**
 * Create all the component of the given directory, including the directory
 * itself.
 *
 * @returns true if the directory was created, false if it already existed.
 */
bool makedirs(const std::string& pathname, mode_t=0777);

/**
 * Compute the absolute path of an executable.
 *
 * If \a name is specified as a partial path, it ensures it is made absolute.
 * If \a name is not specified as a path, it looks for the executable in $PATH
 * and return its absolute pathname.
 */
std::string which(const std::string& name);

/// Delete the file using unlink()
void unlink(const std::string& pathname);

/// Remove the directory using rmdir(2)
void rmdir(const std::string& pathname);

/// Delete the directory \a pathname and all its contents.
void rmtree(const std::string& pathname);

#if 0
/// Nicely wrap access to directories
class Directory
{
protected:
    /// Directory pathname
    std::string m_path;

public:
    class const_iterator
    {
        /// Directory we are iterating
        const Directory* dir;
        /// DIR* pointer
        void* dirp;
        /// dirent structure used for iterating entries
        struct dirent* direntbuf;

    public:
        // Create an end iterator
        const_iterator();
        // Create a begin iterator
        const_iterator(const Directory& dir);
        // Cleanup properly
        ~const_iterator();

        /// auto_ptr style copy semantics
        const_iterator(const const_iterator& i);
        const_iterator& operator=(const const_iterator& i);

        /// Move to the next directory entry
        const_iterator& operator++();

        /// @return the current file name
        std::string operator*() const;

        bool operator==(const const_iterator& iter) const;
        bool operator!=(const const_iterator& iter) const;
    };

    Directory(const std::string& path);
    ~Directory();

    /// Pathname of the directory
    const std::string& path() const { return m_path; }

    /// Check if the directory exists
    bool exists() const;

    /// Begin iterator
    const_iterator begin() const;

    /// End iterator
    const_iterator end() const;
};

#endif
}
}
}

#endif
