#include "lock.h"
#include "arki/utils/sys.h"
#include <limits>
#include <sstream>
#include <cstring>

namespace arki {
namespace utils {

static bool test_nowait = false;

static unsigned count_ofd_setlk = 0;
static unsigned count_ofd_setlkw = 0;
static unsigned count_ofd_getlk = 0;

CountLocks::CountLocks()
    : initial_ofd_setlk(count_ofd_setlk),
      initial_ofd_setlkw(count_ofd_setlkw),
      initial_ofd_getlk(count_ofd_getlk)
{
}

static unsigned count_diff(unsigned initial, unsigned current) noexcept
{
    if (initial > current)
        // Counter wrapped around
        return std::numeric_limits<unsigned>::max() - initial + current;
    else
        return current - initial;
}

void CountLocks::measure()
{
    ofd_setlk = count_diff(initial_ofd_setlk, count_ofd_setlk);
    ofd_setlkw = count_diff(initial_ofd_setlkw, count_ofd_setlkw);
    ofd_getlk = count_diff(initial_ofd_getlk, count_ofd_getlk);
}


Lock::Lock()
{
    memset(this, 0, sizeof(struct ::flock));
}

bool Lock::ofd_setlk(sys::NamedFileDescriptor& fd)
{
    ++count_ofd_setlk;
    return fd.ofd_setlk(*this);
}

bool Lock::ofd_setlkw(sys::NamedFileDescriptor& fd, bool retry_on_signal)
{
    ++count_ofd_setlkw;
    if (test_nowait)
    {
        // Try to inquire about the lock, to maybe get information on what has
        // it held
        struct ::flock l = *this;
        if (!fd.ofd_getlk(l))
        {
            std::stringstream msg;
            msg << "a ";
            if (l.l_type == F_RDLCK)
                msg << "read ";
            else
                msg << "write ";
            msg << "lock is already held on " << fd.name() << " from ";
            switch (l.l_whence)
            {
                case SEEK_SET: msg << "set:"; break;
                case SEEK_CUR: msg << "cur:"; break;
                case SEEK_END: msg << "end:"; break;
            }
            msg << l.l_start << " len: " << l.l_len;
            throw std::runtime_error(msg.str());
        }

        // Then try and actually obtain it, raising an exception if it fails
        if (!fd.ofd_setlk(*this))
            throw std::runtime_error("file already locked");

        return true;
    }
    else
        return fd.ofd_setlkw(*this, retry_on_signal);
}

bool Lock::ofd_getlk(sys::NamedFileDescriptor& fd)
{
    ++count_ofd_getlk;
    return fd.ofd_getlk(*this);
}

Lock::TestNowait::TestNowait()
    : orig(test_nowait)
{
    test_nowait = true;
}

Lock::TestNowait::~TestNowait()
{
    test_nowait = orig;
}

void Lock::test_set_nowait_default(bool value)
{
    test_nowait = value;
}


Locker::~Locker() {}

bool NullLocker::setlk(sys::NamedFileDescriptor& fd, Lock&) { return true; }
bool NullLocker::setlkw(sys::NamedFileDescriptor& fd, Lock&) { return true; }
bool NullLocker::getlk(sys::NamedFileDescriptor& fd, Lock&) { return true; }

bool OFDLocker::setlk(sys::NamedFileDescriptor& fd, Lock& l) { return l.ofd_setlk(fd); }
bool OFDLocker::setlkw(sys::NamedFileDescriptor& fd, Lock& l) { return l.ofd_setlkw(fd); }
bool OFDLocker::getlk(sys::NamedFileDescriptor& fd, Lock& l) { return l.ofd_getlk(fd); }


}
}

