#ifndef ARKI_CORE_FILE_H
#define ARKI_CORE_FILE_H

#include <arki/utils/sys.h>
#include <arki/core/fwd.h>
#include <string>

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
 * Abstract interface to things that return a line of text at a time
 */
struct LineReader
{
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
    virtual bool eof() const = 0;

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
     * Create a LineReader from a buffer on a string.
     *
     * No copy is made of the data: the buffer must remain valid for as long as the
     * line reader is used.
     */
    static std::unique_ptr<LineReader> from_chars(const char* buf, size_t size);
};


/**
 * Wrap a struct flock, calling the corresponding FileDescriptor locking
 * operations on it.
 */
struct Lock : public ::flock
{
    Lock();

    bool ofd_setlk(NamedFileDescriptor& fd);
    bool ofd_setlkw(NamedFileDescriptor& fd, bool retry_on_signal=true);
    bool ofd_getlk(NamedFileDescriptor& fd);
};


namespace lock {

/**
 * Abstract locking functions that allow changing locking behaviour at runtime
 */
struct Policy
{
    virtual ~Policy();
    virtual bool setlk(NamedFileDescriptor& fd, Lock&) const = 0;
    virtual bool setlkw(NamedFileDescriptor& fd, Lock&) const = 0;
    virtual bool getlk(NamedFileDescriptor& fd, Lock&) const = 0;
};

/// Lock Policy that does nothing
struct NullPolicy : public Policy
{
    bool setlk(NamedFileDescriptor& fd, Lock&) const override;
    bool setlkw(NamedFileDescriptor& fd, Lock&) const override;
    bool getlk(NamedFileDescriptor& fd, Lock&) const override;
};

/// Lock Policy that uses Open File Descriptor locks
struct OFDPolicy : public Policy
{
    bool setlk(NamedFileDescriptor& fd, Lock&) const override;
    bool setlkw(NamedFileDescriptor& fd, Lock&) const override;
    bool getlk(NamedFileDescriptor& fd, Lock&) const override;
};


/**
 * Set the default behaviour for the test nowait feature: when set to true,
 * if the lock is busy ofd_setlkw will throw an exception instead of
 * waiting.
 */
void test_set_nowait_default(bool value);


/**
 * Change the behaviour of ofd_setlkw to throw an exception instead of
 * waiting if the lock is busy.
 *
 * This is used during testsd to detect attempted accesses to locked files.
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
