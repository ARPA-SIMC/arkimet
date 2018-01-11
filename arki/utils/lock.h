#ifndef ARKI_UTILS_LOCK_H
#define ARKI_UTILS_LOCK_H

#include <fcntl.h>

namespace arki {
namespace utils {

namespace sys {
struct NamedFileDescriptor;
}

/**
 * Wrap a struct flock, calling the corresponding sys::FileDescriptor locking
 * operations on it.
 */
struct Lock : public ::flock
{
    Lock();

    bool ofd_setlk(sys::NamedFileDescriptor& fd);
    bool ofd_setlkw(sys::NamedFileDescriptor& fd, bool retry_on_signal=true);
    bool ofd_getlk(sys::NamedFileDescriptor& fd);

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
     * Set the default behaviour for the test nowait feature: when set to true,
     * if the lock is busy ofd_setlkw will throw an exception instead of
     * waiting.
     */
    static void test_set_nowait_default(bool value);
};


/**
 * Count how many times utils::Lock::ofd_* functions are called during the
 * lifetime of this object
 */
struct CountLocks
{
    unsigned initial_ofd_setlk;
    unsigned initial_ofd_setlkw;
    unsigned initial_ofd_getlk;
    unsigned ofd_setlk = 0;
    unsigned ofd_setlkw = 0;
    unsigned ofd_getlk = 0;

    CountLocks();

    /// Set ofd_* to the number of calls since instantiation
    void measure();
};

struct Locker
{
    virtual ~Locker();
    virtual bool setlk(sys::NamedFileDescriptor& fd, Lock&) = 0;
    virtual bool setlkw(sys::NamedFileDescriptor& fd, Lock&) = 0;
    virtual bool getlk(sys::NamedFileDescriptor& fd, Lock&) = 0;
};

struct NullLocker : public Locker
{
    bool setlk(sys::NamedFileDescriptor& fd, Lock&) override;
    bool setlkw(sys::NamedFileDescriptor& fd, Lock&) override;
    bool getlk(sys::NamedFileDescriptor& fd, Lock&) override;
};

struct OFDLocker : public Locker
{
    bool setlk(sys::NamedFileDescriptor& fd, Lock&) override;
    bool setlkw(sys::NamedFileDescriptor& fd, Lock&) override;
    bool getlk(sys::NamedFileDescriptor& fd, Lock&) override;
};

}
}
#endif
