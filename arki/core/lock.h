#ifndef ARKI_CORE_LOCK_H
#define ARKI_CORE_LOCK_H

#include <arki/core/file.h>
#include <memory>

namespace arki::core {

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

#endif


