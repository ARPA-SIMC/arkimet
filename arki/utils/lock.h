#ifndef ARKI_UTILS_LOCK_H
#define ARKI_UTILS_LOCK_H

#include <arki/utils/sys.h>
#include <fcntl.h>

namespace arki {
namespace utils {

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
        TestNowait();
        ~TestNowait();
    };
};

}
}
#endif
