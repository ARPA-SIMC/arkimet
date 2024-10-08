#ifndef ARKI_CORE_FILE_H
#define ARKI_CORE_FILE_H

#include <arki/utils/sys.h>
#include <arki/core/fwd.h>
#include <string>
#include <vector>
#include <array>
#include <memory>
#include <cstddef>
#include <cstdint>

namespace arki {
namespace core {

/*
 * NamedFileDescriptor and File come from wobble and moving their
 * implementation outside of utils means making it hard to update it in the
 * future.
 *
 * This header exists to promote NamedFileDescriptor and File to non-utils arki
 * namespace, so they can be used by general arkimet APIs.
 */
using arki::utils::sys::NamedFileDescriptor;
using arki::utils::sys::File;

struct Stdin : public NamedFileDescriptor
{
    Stdin();
};

struct Stdout : public NamedFileDescriptor
{
    Stdout();
};

struct Stderr : public NamedFileDescriptor
{
    Stderr();
};


/**
 * Abstract input file implementation to use for output operation on things
 * that are not file descriptors
 */
struct AbstractInputFile
{
    virtual ~AbstractInputFile();

    [[deprecated("Use path() instead")]] virtual std::string name() const = 0;
    virtual std::filesystem::path path() const = 0;

    /**
     * Read up to \a size bytes into dest.
     *
     * Return the number of bytes read, or 0 for EOF
     */
    virtual size_t read(void* dest, size_t size) = 0;
};


/**
 * Generic abstract input interface, with buffering
 */
struct BufferedReader
{
protected:
    std::array<uint8_t, 65536> buffer;
    unsigned pos = 0;
    unsigned end = 0;

    /**
     * Assume the buffer is empty. Read into the buffer, returning the number
     * of bytes read.
     *
     * Return 0 in case end-of-file is reached.
     */
    virtual unsigned read() = 0;

    /**
     * Call read() to refill an empty input buffer.
     *
     * Return false if end-of-file is reached.
     */
    bool refill();

public:
    virtual ~BufferedReader();

    /**
     * Return the next character in the input buffer, or EOF if we are at the
     * end of file.
     *
     * Does not advance the current position
     */
    int peek();

    /**
     * Return the next character in the input buffer, or EOF if we are at the
     * end of file.
     *
     * It advances the current position
     */
    int get();

    /**
     * Create a BufferedReader from a file descriptor.
     *
     * The file descriptor is not managed by the BufferedReader, and will need
     * to be kept open by the caller for as long as the reader is used, then
     * closed at the end.
     *
     * Note that a BufferedReader reads data in chunks from its input, so if
     * the caller stops calling get(), the file descriptor is likely to be
     * positioned further ahead than the character read
     */
    static std::unique_ptr<BufferedReader> from_fd(NamedFileDescriptor& fd);

    /**
     * Create a BufferedReader from an abstract input file.
     *
     * The file descriptor is not managed by the BufferedReader, and will ned
     * to be kept open by the caller for as long as the reader is used, then
     * closed at the end.
     *
     * Note that a BufferedReader reads data in chunks from its input, so if
     * the caller stops calling get(), the file descriptor is likely to be
     * positioned further ahead than the character read
     */
    static std::unique_ptr<BufferedReader> from_abstract(AbstractInputFile& fd);

    /**
     * Create a BufferedReader from a buffer on a string.
     *
     * No copy is made of the data: the buffer must remain valid for as long as the
     * reader is used.
     */
    static std::unique_ptr<BufferedReader> from_chars(const char* buf, size_t size);

    /**
     * Create a BufferedReader from a buffer on a string.
     *
     * No copy is made of the data: the buffer must remain valid for as long as the
     * reader is used.
     */
    static std::unique_ptr<BufferedReader> from_string(const std::string& str);
};


/**
 * Abstract interface to things that return a line of text at a time
 */
class LineReader
{
protected:
    bool fd_eof = false;

public:
    virtual ~LineReader() {}

    /**
     * Similar to std::getline, raises an exception if anything went wrong.
     *
     * Returns false if EOF is reached, in which case line will be empty.
     */
    virtual bool getline(std::string& line) = 0;

    /**
     * Check if the last getline returned eof. This is always false before
     * getline is called for the first time.
     */
    bool eof() const { return fd_eof; }

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
    static std::unique_ptr<LineReader> from_fd(NamedFileDescriptor& fd);

    /**
     * Create a LineReader from an abstract input file.
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
    static std::unique_ptr<LineReader> from_abstract(AbstractInputFile& fd);

    /**
     * Create a LineReader from a buffer on a string.
     *
     * No copy is made of the data: the buffer must remain valid for as long as the
     * line reader is used.
     */
    static std::unique_ptr<LineReader> from_chars(const char* buf, size_t size);

    /**
     * Create a LineReader from a buffer on a string.
     *
     * No copy is made of the data: the buffer must remain valid for as long as the
     * reader is used.
     */
    static std::unique_ptr<LineReader> from_string(const std::string& str);
};


class Lock : public std::enable_shared_from_this<Lock>
{
protected:
    Lock() = default;

public:
    Lock(const Lock&) = delete;
    Lock(Lock&&) = delete;
    Lock& operator=(const Lock&) = delete;
    Lock&& operator=(Lock&&) = delete;
    virtual ~Lock();
};


/**
 * Wrap a struct flock, calling the corresponding FileDescriptor locking
 * operations on it.
 */
struct FLock : public ::flock
{
    FLock();

    bool ofd_setlk(NamedFileDescriptor& fd);
    bool ofd_setlkw(NamedFileDescriptor& fd, bool retry_on_signal=true);
    bool ofd_getlk(NamedFileDescriptor& fd);
};


namespace lock {

class Null : public Lock
{
public:
    using Lock::Lock;
};


/**
 * Abstract locking functions that allow changing locking behaviour at runtime
 */
struct Policy
{
    virtual ~Policy();
    virtual bool setlk(NamedFileDescriptor& fd, FLock&) const = 0;
    virtual bool setlkw(NamedFileDescriptor& fd, FLock&) const = 0;
    virtual bool getlk(NamedFileDescriptor& fd, FLock&) const = 0;
};


/**
 * Set the default behaviour for the test nowait feature: when set to true,
 * if the lock is busy ofd_setlkw will throw an exception instead of
 * waiting.
 */
void test_set_nowait_default(bool value);


/**
 * Change the behaviour of ofd_setlkw to wait if the lock is busy.
 *
 * This is used during tests to restore the standard behaviour
 */
struct TestWait
{
    bool orig;
    TestWait();
    ~TestWait();
};


/**
 * Change the behaviour of ofd_setlkw to throw an exception instead of
 * waiting if the lock is busy.
 *
 * This is used during tests to detect attempted accesses to locked files.
 */
struct TestNowait
{
    bool orig;
    TestNowait();
    ~TestNowait();
};

/**
 * Count how many times utils::Lock::ofd_* functions are called during the
 * lifetime of this object
 */
struct TestCount
{
    unsigned initial_ofd_setlk;
    unsigned initial_ofd_setlkw;
    unsigned initial_ofd_getlk;
    unsigned ofd_setlk = 0;
    unsigned ofd_setlkw = 0;
    unsigned ofd_getlk = 0;

    TestCount();

    /// Set ofd_* to the number of calls since instantiation
    void measure();
};

}


}
}
#endif
