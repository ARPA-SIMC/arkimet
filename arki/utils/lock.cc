#include "lock.h"
#include <sstream>
#include <cstring>

namespace arki {
namespace utils {

static bool test_nowait = false;

Lock::Lock()
{
    memset(this, 0, sizeof(struct ::flock));
}

bool Lock::ofd_setlk(sys::NamedFileDescriptor& fd) { return fd.ofd_setlk(*this); }
bool Lock::ofd_setlkw(sys::NamedFileDescriptor& fd, bool retry_on_signal)
{
    if (test_nowait)
    {
        // Try to inquire about the lock, to maybe get information on what has
        // it held
        struct ::flock l = *this;
        if (!fd.ofd_getlk(l))
        {
            std::stringstream msg;
            if (l.l_type == F_RDLCK)
                msg << "read ";
            else
                msg << "write ";
            msg << "lock already held on " << fd.name() << " from ";
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
bool Lock::ofd_getlk(sys::NamedFileDescriptor& fd) { return fd.ofd_getlk(*this); }

Lock::TestNowait::TestNowait()
{
    test_nowait = true;
}

Lock::TestNowait::~TestNowait()
{
    test_nowait = false;
}

}
}

